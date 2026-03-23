# Redis互換RustサーバにおけるRDB Ingest/Export実装戦略

## Executive Summary

- Valkeyは「Redis OSS 7.2.4のフォーク」であり、Redis OSS 7.2およびそれ以前と互換だが、Redis Community Edition 7.4以降はオープンソースではなく「データファイルがValkeyと非互換」と明記されているため、まずは“OSS系（Redis OSS 〜7.2.x / Valkey 7.x/8.x）”にRDB互換性ターゲットを固定するのが現実的。citeturn25view0  
- Redis 7.2 / Valkey 7.2/8.0 系のRDBフォーマットは `RDB_VERSION 11` で、型コード・opcode群（AUX/RESIZEDB/EXPIRETIME_MS/SELECTDB/EOF等）が明確に定義されているため、まずは「RDB11の完全実装（読込＋書出）」を最小の“正しい核”として確立するのが最も保守的。citeturn5view0turn6view0turn6view1  
- Valkey 9ではRDBバージョン番号を80から開始し、12–79を“非互換な（非OSS）Redis”のために予約する設計に踏み切っているため、RDBバージョン/マジック文字列（REDIS/VALKEY）を“方針として明確に扱う”設計が必須。citeturn7view0  
- 既存のRustエコシステムは「RDBを“解析して別形式にダンプ”する」用途が中心で、両方向（ingest＋export）のコア用途にそのまま採用できる“標準的Rust実装”は現状ほぼ存在しない。例として `rdb` クレートはRDBを解析しJSON/RESPに再フォーマットすることを主目的にしている。citeturn11view2  
- `redis/librdb` はMITライセンスのC製RDBパーサで、SAX風のイベント駆動・ストリーミング設計を掲げ、CLIでRESP生成やlive serverへのロードまで備えるなど実用度は高いが、基本は“パース側”でありRDB生成器ではない。citeturn9view0turn9view1  
- したがって推奨の最終形は「in-treeの純Rustで“RDB11読み/書き”を完成させる」こと。既にDUMPペイロードのデコードを一部実装済みとの前提から、最難所（オブジェクトエンコーディング周辺）の再利用余地が大きい。citeturn13view2  
- レプリケーションのFULLRESYNCは“RDBスナップショット転送＋その間にバッファした書込みコマンドの送出”が基本挙動であり、export側は“スナップショット一貫性（バッファリング/オフセット整合）”の設計をRDB writerと一体で作る必要がある。citeturn13view0  
- ディスクレス（EOFマーカ方式）のRDB転送は「サイズ事前確定不要」でストリーミングexportに有利であり、互換性の観点でもRedis側はEOF方式の存在を前提にcapabilityで判別できる設計へ進化しているため、実装候補として優先度が高い。citeturn13view1turn17view2  
- 検証は `redis-cli --rdb` / `valkey-cli --rdb` を“実サーバ互換プローブ”として使うのが最短で強力。これらはレプリケーション初回同期でRDBが転送される性質を利用している。citeturn24view0turn24view1  

## Candidate Matrix

| Candidate | Maintenance status | License | Ingest support fit | Export support fit | Safety / security posture | Operational burden | Adopt now / pilot / avoid |
|---|---|---|---|---|---|---|---|
| **A. in-tree純Rust（RDB reader+writerを自前）** | 自チームが主語（外部依存最小） | 自プロジェクト準拠 | 既存のDUMPデコード資産を核にRDB11 ingestを完成させやすい（DUMPはRDB同形式＋RDBバージョン内包）。citeturn13view2turn5view0 | 既存エコシステムに“RDB生成”の決定版がないため、最終的に自前が不可避。citeturn11view2turn9view1 | Rustでfuzz/境界チェックをやりやすい（FFI無し）。設計次第でDoS耐性も作れる（後述） | 中（実装範囲は広いが運用は軽い） | **Adopt now（最終形）** |
| **B. `redis/librdb` をFFIでingestに採用＋exportはRust自前** | Redis GitHub orgで開発、2.0.0（2025-12-02）など更新が確認できる。citeturn8view1turn8view0 | MIT。citeturn9view0 | SAX風イベント駆動でストリーミングパースを主目的としている。citeturn9view1 | librdb自体は“パース/変換/ロード”が中心で、RDB writerではない。citeturn9view1 | C由来のメモリ安全性リスクは残る（実際にdouble-free修正がchangelogにある）。citeturn8view1 | 中〜高（ビルド/ABI/サニタイザ/脆弱性対応など） | **Pilot（タイムライン優先時の現実解）** |
| **C. Rust `rdb` クレート（rdb-rs）をingest基盤にしてformatterで適用＋export自前** | 2025-03-29にメンテ継承が宣言され、crate ownerも複数。citeturn11view1turn11view2 | MIT。citeturn11view2 | “読み取り→formatter呼出”モデルなので、formatterを実装してサーバ状態へ反映する形は可能。ただし本番replication ingest（DoS/部分適用/タイムアウト）向け要件は要追加設計。citeturn11view2 | 主目的はJSON/RESP等へのダンプであり、RDB writerではない。citeturn11view2 | Rustで安全性は高いが、コアパス採用の実績/硬さは要検証 | 低〜中（依存導入は軽い） | **Pilot（テスト/検証用の採用は有力、コア採用は慎重）** |
| **D. sidecar（`rdb-cli` 等）プロセス分離でingest変換（RESP/RESTORE）** | `rdb-cli` はlibrdb付属CLIとして存在し、RESP生成やlive serverへのロード例がある。citeturn9view1 | MIT（librdb）。citeturn9view0 | “サーバ本体のreplication FULLRESYNC”に組み込むにはI/O・プロセス管理・バックプレッシャ等が重い。オフライン移行ツール用途向き。citeturn9view1 | RDB writerではない。citeturn9view1 | 侵害時の被害半径を縮めやすい（sandboxしやすい） | 高（運用/障害点が増える） | **Pilot（移行ツール/差分検証に限定）** |
| **E. sidecar（Go等のRDBツールでreader/writer）** | 例：Go実装でRDB生成まで主張するツールが存在（≤RDB12等）。citeturn1search18turn2search17 | Apache-2.0等。citeturn2search17 | 本番replication ingestに組込むには結局IPC設計が必要 | writerがあるとしても“コアパス依存”としての信頼性・互換の追従は別途評価が必要 | 言語的には安全寄りでも、コアパスの供給網/メンテ体制/互換追従が最大リスク | 高 | **Avoid（コアではなくツール用途に限定）** |
| **F. Redis/Valkeyサーバ本体コードをライブラリ的に流用（FFI）** | RedisのRDB実装は存在するが、配布物にAGPLv3表記がある。citeturn0search21 | AGPLv3等（組込みはプロジェクト全体に重大影響）citeturn0search21 | 実装の“正”に近いが、結合度が極端に高い | 同左 | C/巨大依存＋ライセンスリスク | 非常に高い | **Avoid** |

## Recommended Default

**Primary choice: A. in-tree純RustでRDB11のreader+writerを実装（コアパスはFFI無し）**

- RDB writer（下流FULLRESYNCの“本物RDB snapshot”生成）は、現状の公開Rustエコシステムに“枯れた決定版”がなく、結局in-tree実装が避けられない。citeturn11view2turn9view1  
- 既に「DUMPペイロードのデコード（＝RDB形式の値エンコーディング）を一部実装済み」という状況は、RDB ingest実装で最も複雑な“値の復元”を再利用できる可能性が高い。さらにDUMPは“RDB同形式＋RDBバージョン内包＋チェックサム”という性質を持つため、設計を揃えると実装資産の再利用率が上がる。citeturn13view2  
- ターゲットをまずRDB11に固定すると、Redis 7.2/Valkey 7.2/8.0系の `RDB_VERSION 11` とopcodeがソースで明確で、互換ウィンドウを定義しやすい。citeturn5view0turn6view0turn6view1turn25view0  
- Valkey 9以降のRDBバージョンの飛び（80〜）は大きいが、逆に“互換境界が明示的”なので、初期はRDB11に集中し、将来は“機能（例：新型Hash等）を実装した時点でRDB80対応を足す”というロードマップにできる。citeturn7view0  

**Fallback choice: B. `redis/librdb` をFFIでingestに採用（exportはRust自前）**

- librdbはSAX風イベント駆動/ストリーミング前提で設計されており、“RDB ingestを短期で堅くする”という文脈では魅力がある。citeturn9view1  
- 一方でC由来の安全性・サプライチェーン・ビルド/ABIの運用負荷が増え、実際にメモリ安全性修正も履歴にあるため、長期のコアパスとしては慎重評価が必要。citeturn8view1  

## Minimum Type Coverage

**最初のProduction sliceで“必須”にすべきカバレッジ（RDB11前提）**

RDBファイル/ストリームの基本構造として、マジック文字列（通常は “REDIS”）、4桁ASCIIのRDBバージョン、DBセクション、EOF（0xFF）と末尾CRC64チェックサムという枠組みを正しく扱う必要がある。citeturn19view0  
加えて、Redis 7.2系の `rdb.h` で定義される主要opcode（AUX/RESIZEDB/EXPIRETIME_MS/EXPIRETIME/SELECTDB/EOF等）と、主要な型コード（STRING/LIST/SET/ZSET/HASH、さらにlistpack/quicklist等の派生型）を“少なくともparseできる”状態にする。citeturn5view0turn19view0  

最小sliceとして現実的に“必須（＝多くの実データで遭遇し、かつ互換性の核）”となるのは次。

- **strings**：RDBのRedis Stringは「長さプレフィクス」「整数エンコード」「LZF圧縮」の3形態があり、読込側はこの3形態を扱えないと現実のdumpで破綻しやすい。citeturn19view0turn5view0  
- **expiries**：EXPIRETIME（秒）とEXPIRETIME_MS（ミリ秒）。互換性と正確性のため必須。citeturn19view0turn5view0  
- **logical databases / SELECT**：SELECTDB（0xFE）でDB番号が切り替わる。複数DB互換のため必須。citeturn19view0turn5view0  
- **hashes / lists / sets / zsets**：RDBの基本データ型。ZSETは複数表現があり、少なくとも一般的な型コードのdecode/encodeが必要。citeturn5view0  
- **AUX fields / metadata**：AUX（0xFA）は任意のkey-value設定で、未知のAUXは無視すべきとされる（将来互換の鍵）。citeturn19view0turn19view1turn5view0  
- **checksum**：RDB v5以降は末尾CRC64。無効化時はゼロ埋めがあり得る。読込側は“検証ON/OFF”の方針を持つべき。citeturn19view0turn19view1  

**明示的にdefer可能（ただし“遭遇時の方針”は初期から必須）**

- **streams と stream metadata（consumer group, PEL等）**：RDB側で専用型（例：`RDB_TYPE_STREAM_LISTPACKS_3` 等）が存在し得る。初期は“未対応ならFULLRESYNCを失敗させる（既存データを壊さない）”のが安全。citeturn5view0  
- **function libraries**：RDB opcodeとしてFUNCTION2等が存在し得る。初期は“未対応なら明示エラー”でよい（遭遇率はワークロード依存）。citeturn5view0  
- **module payloads / MODULE_AUX**：モジュールネイティブ型はRDBに型識別子・エンコードバージョンを含む仕組みで、未ロードだと復元不能の前提。初期は“モジュール型を見たらデフォルトで失敗”が最も整合的。citeturn15view0turn5view0  
- **圧縮や小型表現の“書出最適化”**：export側は最初は非圧縮・最小限の表現で正しさを優先し、後から“互換性が崩れない範囲で”サイズ/速度最適化を入れる（読込側は初期から多形態対応）。citeturn19view0turn5view0  

## Ingest Design

### decoding model

RDB ingestは、基本的に「RDBストリームを逐次読み」「(db_id, key, type, value, expire_at, 付随メタ)」イベントへ落とすストリーミングデコーダとして設計するのが最も堅い。RDBの枠組み（マジック、“4桁ASCIIのRDBバージョン”、SELECTDB、任意のAUX、EOF＋CRC64）をまず“コンテナ層”として実装し、その内側で型ごとのvalue decodeを呼び出す構造が見通しが良い。citeturn19view0turn5view0  

値の復元は「canonical logical objects（論理型）」を内部表現に合わせて作るのが推奨だが、あなた方は既に一部の `DUMP` payload decode（＝RDBと同じ値エンコーディング）を持っているため、短期戦略としては次が最も効率的。

- **RDB key-valueのvalue部分を“既存のDUMP decodeロジックに近いAPI”へ寄せる**  
  DUMPの仕様として「値はRDBと同形式」「RDBバージョンが埋め込まれ、非互換なら拒否される」「64-bitチェックサムがある」ため、“バージョンを意識したdecode API”を共通化しやすい。citeturn13view2  

### staging/publish model

FULLRESYNC ingestで最大の事故は「途中までロードしてからエラーで止まり、部分データが公開される」こと。Redis/Valkeyも“フル同期時にDBを入れ替える/ロード中の扱いを切り替える”という概念を持ち、非同期ロード（swapdb）中を示す `async_loading` がINFOに現れる。citeturn17view0turn17view3  

したがって、Rustサーバ側も次の2段階コミットをデフォルトにする。

- **Stage**：新しい“ステージングDB”へ全キーをロード（db_idごとに構築）  
- **Validate**：EOF到達、CRC64検証（必要なら）、統計（キー数等）確定  
- **Publish**：ステージングDBをアトミックに切替（swap）し、旧DBを安全に破棄  

このモデルはメモリ二重化を招き得るため、運用向けに次のポリシーを用意するのが現実的。

- `swapdb` 相当：旧DBを保持したまま新DB構築（安全だがメモリピークが高い）  
- `on-empty-db` 相当：現DBが空のときのみswap方式、それ以外は“ブロッキングロード”へ（運用現場で選べる）  
- `disabled` 相当：常にブロッキングロード（最小メモリだが、失敗時のリカバリ設計が必須）  

少なくとも“swap方式はメモリを食うので慎重”という認識はRedis側でも示されている。citeturn17view2  

### error policy

- **フォーマット不正/CRC不一致/未知のRDB型/未知の必須opcode**：デフォルトはFULLRESYNCを失敗させ、publishしない（旧DBを保持）  
- **未知のAUX**：仕様上は無視（forward compatibilityの要）。citeturn19view0turn19view1  
- **module payload / function library / stream**：初期sliceでは“未対応なら失敗”をデフォルトにし、必要なら“明示フラグでbest-effort（当該キー破棄）”を提供する。ただしbest-effortはレプリケーション整合性を壊し得るため“移行ツール用途のみ推奨”と位置付ける。モジュール型がRDBに強い識別を持ち、未ロードなら復元不能という前提がある以上、無言スキップは危険。citeturn15view0turn5view0  

## Export Design

### encoding model

export（下流FULLRESYNC）は「RDB writer＋レプリケーションのスナップショット整合」をセットで設計する。Redisのレプリケーションでは、フル同期時に“RDB生成中に新規書込みコマンドをバッファし、RDB転送後にそのバッファを送る”という挙動が明記されているため、スナップショット作成時点の整合（offset境界）を必ず定義する必要がある。citeturn13view0  

writer自体は、初期は「RDB11を固定で出す」「表現は最小限（非圧縮・単純型コード中心）」で良い。RDBの基本構造（マジック“REDIS”、4桁ASCIIバージョン、DBセクション、EOF、CRC64）を守れば、ロード側は基本的に受理できる。citeturn19view0turn5view0  
また、Valkey 7.2/8.0 と Redis 7.2 はRDB_VERSION 11で一致しているため、まずここに合わせるのが互換最大化になる。citeturn5view0turn6view0turn6view1  

### streaming/materialization policy

FULLRESYNCでRDBを送る際、2方式がある。

- **長さ既知（bulk stringで`$<len>\r\n`）**：lenを先に出す必要があるため、“先に全RDBを生成してサイズ確定”が必要になりがち（巨大DBで辛い）  
- **EOFマーカ方式（diskless replicationのストリーミング）**：サイズ不明のまま転送できる。antirezの設計ノートによれば、`$EOF:<random>` を送ってからデータを流し、最後にそのランダムトークンを流して終端とする。citeturn13view1  

Rustサーバで“ストリーミングexport”を本気でやるなら、**EOFマーカ方式を第一候補**にする価値が高い。Redis側でもcapabilityで対応可否を判断できる流れに進んでいる。citeturn17view2  

ただし互換性・デバッグ容易性のために、初期実装は次の現実解が堅い。

- **デフォルト：EOFマーカ方式（ストリーミング）**  
- **フォールバック：長さ既知方式**（必要な相手にだけ）  
  - 生成先は一時ファイル or 一時メモリ（ただし巨大の場合はファイル推奨）  
  - どちらの方式でも、writerは同一のRDBエンコード関数を再利用  

### versioning policy

- 初期：**常にRDB11を吐く**（Redis OSS 7.2/Valkey 7.x/8.x互換の最大公倍数）。citeturn25view0turn5view0turn6view0turn6view1  
- 将来：Valkey 9のRDB80等に追従する場合、Valkeyが“非OSS RedisのRDBバージョン（12–79）との衝突回避”を意図している点を尊重し、**RDBバージョンの選択を“機能（表現可能性）”で決める**方針にする。citeturn7view0  
- “可能なら古いRDBで書き出したい”という要求はValkey側でも議論されており、将来的に“最小バージョンで出せるときは下げる”戦略も視野に入る。citeturn4search26  

## Compatibility Strategy

### Redis / Valkey coverage plan

- **Baseline互換ターゲット**：Redis OSS 2.x〜7.2.x と Valkey 7.2.x/8.0（=RDB11中心）  
  Valkey公式の移行ガイドは、Valkey 7.2.4がRedis 7.2.4のフォークで、Redis OSS 7.2およびそれ以前と互換であることを明記している。citeturn25view0  
- **非ターゲット（明示的に除外）**：Redis Community Edition 7.4以降のRDB（Valkeyと非互換と明記）。citeturn25view0  
  ここを“初期から頑張って追う”のは、RDB作者側（Redis CE）の非互換方針と、Valkey側の“foreign version range予約”の両方から見てリスクが高い。citeturn7view0turn25view0  

### future version handling

- **RDBバージョンの受付ポリシーを明文化**する：  
  - ingest：`<=11` を安定サポート（必要なら `80` などValkey側の新系列を追加）citeturn7view0turn5view0  
  - export：当面 `11` 固定  
- **未知AUXは無視**（forward compatibility）：AUXは任意設定で、未知キーは無視すべきとされる。citeturn19view0turn19view1  
- **未知オブジェクト型/未知opcodeはデフォルト拒否**：理解できないデータを“それっぽくロード”すると整合性が壊れるため（特にreplicationは致命的）。型コードとopcodeの定義は `rdb.h` によって明確に管理されている前提で扱う。citeturn5view0turn7view0  
- **マジック文字列（REDIS/VALKEY）を実装上の分岐点にする**：Valkeyはrdb.h内でREDIS/VALKEYマジックをチェックし、さらにバージョン番号の衝突回避レンジまで定義しているため、reader/writer設計の段階で“識別と分岐”を織り込む。citeturn7view0  

## Validation Blueprint

### corpus

- **RDB構造ベースのゴールデンコーパス**を“バージョン×機能”で作る：  
  - RDB基本構造（REDISマジック＋4桁バージョン、DB selector、EOF、CRC64）を満たすファイル。citeturn19view0  
  - EXPIRE（秒/ミリ秒）、AUX、RESIZEDBを含む組み合わせ。citeturn19view0turn19view1turn5view0  
  - Stringの3形態（長さ/整数/LZF）。citeturn19view0turn5view0  
  - 型コードの網羅（少なくともSTRING/LIST/SET/ZSET/HASH、可能ならlistpack/quicklist派生）。citeturn5view0  

### fuzzing

- **RDB reader専用のfuzzハーネス**：  
  - “入力は任意バイト列”を前提に、panic/abort/無限ループ/OOMを起こさないことを第一目標にする（境界チェック・最大長・最大要素数・最大ネスト等）。RDBは長さプレフィクスが多いので、上限設定がDoS耐性の鍵になる。citeturn19view0turn5view0  
- **structured fuzz**：RDBのopcode列（SELECTDB/EXPIRE/AUX/TYPE/KEY/VALUE/EOF）を生成するジェネレータ型fuzzを用意し、“合法だが意地悪なRDB”でデコーダの論理バグを叩く。citeturn19view0turn5view0  
- もしFFI（librdb）を採用するなら、C側はASAN/UBSAN等＋独立fuzzも検討（changelogにdouble-free修正があるため、特に重要）。citeturn8view1  

### differential tests

- **reference実装との差分検証**：  
  - Redis/Valkeyで生成したRDBを自サーバにingestし、同等のキー集合・TTL・型が再現されることを検証する。RDBのAUXに含まれる情報（redis-ver等）は未知キー無視の原則に従い、テストでは“無視して良いもの/一致すべきもの”を分ける。citeturn19view0turn19view1  
  - exportしたRDBをRedis/Valkeyでロードできるか（起動に成功し、想定キー数になるか）を検証する。Valkey移行ガイドでも `INFO KEYSPACE` によるキー数確認が例示されている。citeturn25view0  

### real-server interop

- **`redis-cli --rdb` / `valkey-cli --rdb` を互換プローブにする**：  
  これらは“レプリケーション初回同期でRDBが転送される”性質を利用し、リモートからRDBファイルを取得できる。自サーバが下流FULLRESYNCで送るRDBを、CLIでそのまま回収して解析/比較できる。citeturn24view0turn24view1  
- **`redis-cli --replica` / `valkey-cli --replica` によるコマンドストリーム確認**：  
  初回同期のRDBを捨てて以降のレプリケーションコマンドを表示できるため、“RDB→コマンド移行境界”や“バッファリングの正しさ”を見るのに使える。citeturn24view0turn24view1  

## Phased Delivery Plan

### Phase 1

**Upstream FULLRESYNC ingest（RDB11限定、必須型のみ）**

- レプリケーションにおける“スナップショット転送＋後続コマンド”の基本フローに合わせ、RDB受信→ステージングロード→publish→後続コマンド適用を実装する。citeturn13view0  
- 受信RDBのフレーミングを2方式で実装：  
  - `$<len>`（サイズ既知）  
  - `$EOF:<token>`（ディスクレス/EOF方式）—antirezが仕様を説明している。citeturn13view1  
- RDBコンテナ層：マジック/4桁バージョン/SELECTDB/EXPIRE/RESIZEDB/AUX/EOF/CRC64。citeturn19view0turn5view0  
- 型：string/list/set/hash/zset、TTL。LZF/整数エンコードのstring decodeを必須にする。citeturn19view0turn5view0  
- 未対応型（stream/function/module）はデフォルトでエラー停止（旧DBは保持）。module型はRDBに明確な識別子とエンコードバージョンがあり、未対応での継続は不正確になりやすい。citeturn15view0turn5view0  

### Phase 2

**Downstream FULLRESYNC export（RDB11 writer＋ストリーミング送出）**

- RDB11を生成して送るwriterを実装（まずは非圧縮・単純型コード中心）。RDB構造（EOF＋CRC64含む）を満たすことが最優先。citeturn19view0turn5view0  
- レプリケーション整合：RDB生成中の書込みバッファ→RDB転送後に送る、というRedis文書化された挙動に合わせて“自サーバ内のスナップショット境界”を定義する。citeturn13view0  
- 送出方式はEOFマーカをデフォルトにし、必要なら長さ既知方式にフォールバック（ストレージやメモリに一度書く）。EOF方式の発想とwire形式はantirezが説明している。citeturn13view1  
- 実サーバ互換テスト：`redis-cli --rdb` / `valkey-cli --rdb` で自サーバからRDBを吸い出し、Redis/ValkeyでロードできるかをCIで回す。citeturn24view0turn24view1turn25view0  

### Phase 3

**互換性・機能拡張（必要なものだけ追加）**

- stream/function/module等、遭遇率と製品要件に合わせて優先度付けして追加。RDB opcodeとしてFUNCTION2等が存在する点は事前に織り込む。citeturn5view0  
- Valkey 9以降のRDB80系列が必要なら、Valkeyの“foreign version range（12–79）”設計に従い、マジック/バージョン/型コードの追加を計画的に取り込む。citeturn7view0turn25view0  
- 圧縮/小型表現の書出最適化（LZF、int-encoding、listpack等）は“同じ意味を保つ範囲で”段階的に。RDB文字列の3形態やCRC64の存在は仕様として固定。citeturn19view0turn19view1turn5view0  

## Risk Register

- **RDBバージョン/実装のドリフト（特にRedis CE 7.4+系）**  
  - 影響：互換破綻、ユーザ期待との不一致  
  - 緩和：サポート範囲を明確化（OSS 〜7.2/Valkey互換をまず保証）。Valkey側はCE 7.4+非互換を明記している。citeturn25view0  
- **Valkey 9のRDB80系列への追従遅れ**  
  - 影響：新しいValkeyとの物理移行/replication互換が限定  
  - 緩和：RDB11をbaselineで固め、必要になった時点でRDB80を追加。Valkeyは衝突回避レンジとRDB_VERSION_MAPを明記している。citeturn7view0  
- **malformed/malicious RDBによるDoS（巨大長さ、圧縮爆弾、深いネスト）**  
  - 影響：OOM、CPUスピン、サービス停止  
  - 緩和：全lengthに上限、LZF展開の出力上限、要素数上限、タイムスライス/進捗監視、ストリーミング処理。RDBは長さプレフィクスが根本なので上限制御が効く。citeturn19view0turn5view0  
- **部分ロードが公開される（partial ingest publication）**  
  - 影響：レプリケーション整合の崩壊・データ破損  
  - 緩和：ステージング→検証→swap（2段階コミット）をデフォルトにする。swapdb的な考え方はRedis/Valkey側にも存在する。citeturn17view0turn17view3  
- **チェックサム/エンコードの実装バグ（CRC64 / LZF / double等）**  
  - 影響：ロード不能、サイレントデータ破損  
  - 緩和：CRC64はRDB v5+で末尾8byteという仕様に従い（無効化時ゼロも考慮）、fuzz＋相互ロード検証を徹底。citeturn19view0turn19view1  
- **FFI採用時のメモリ安全性リスク（librdb等）**  
  - 影響：クラッシュ/脆弱性  
  - 緩和：コアパスはRust化を最終形にするか、採用するならサニタイザ/独立fuzz/プロセス分離を検討。librdbでもdouble-free修正履歴がある。citeturn8view1  
- **スナップショット整合（export中の書込み）**  
  - 影響：レプリカが一貫しない状態になる  
  - 緩和：RDB生成中のコマンドをバッファし、RDB転送後に送るというRedisの基本手順に合わせ、内部実装で“境界オフセット”を定義。citeturn13view0  

## References

- Redis 7.2 `rdb.h`（**source code / primary**：RDB_VERSION=11、型コード、opcode定義）citeturn5view0  
  ```text
  https://raw.githubusercontent.com/redis/redis/7.2/src/rdb.h
  ```

- Valkey 7.2 `rdb.h`（**source code / primary**：RDB_VERSION=11）citeturn6view0  
  ```text
  https://raw.githubusercontent.com/valkey-io/valkey/7.2/src/rdb.h
  ```

- Valkey 8.0 `rdb.h`（**source code / primary**：RDB_VERSION=11）citeturn6view1  
  ```text
  https://raw.githubusercontent.com/valkey-io/valkey/8.0/src/rdb.h
  ```

- Valkey unstable `rdb.h`（**source code / primary**：RDB_VERSION=80、12–79予約、REDIS/VALKEYマジック方針）citeturn7view0  
  ```text
  https://raw.githubusercontent.com/valkey-io/valkey/unstable/src/rdb.h
  ```

- RDB File Format（**spec-like documentation / secondaryだが広く参照**：マジック、4桁ASCII版、AUXの未知キー無視、CRC64等）citeturn19view0turn19view1  
  ```text
  https://rdb.fnordig.de/file_format.html
  https://rdb.fnordig.de/version_history.html
  ```

- Redis Replication docs（**official documentation / primary**：フル同期のRDB生成中バッファ→転送後送信、ディスクレス説明）citeturn13view0  
  ```text
  https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
  ```

- Diskless replication design notes by antirez（**primary-ish（設計者ブログ）**：EOFマーカ方式のwire形式）citeturn13view1  
  ```text
  https://antirez.com/news/81
  ```

- Redis INFO docs（**official documentation / primary**：`async_loading` と `repl-diskless-load=swapdb` の関係）citeturn17view0  
  ```text
  https://redis.io/docs/latest/commands/info/
  ```

- Redis Modules API reference（**official documentation / primary**：diskless async loadに関するモジュールオプション）citeturn17view1  
  ```text
  https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/
  ```

- Valkey migration guide（**official documentation / primary**：Valkey 7.2.4はRedis 7.2.4フォーク、Redis OSS互換、Redis CE 7.4+非互換）citeturn25view0  
  ```text
  https://valkey.io/topics/migration/
  ```

- Valkey `DUMP` command docs（**official documentation / primary**：DUMPに64-bit checksum、RDB同形式、RDB version内包）citeturn13view2  
  ```text
  https://valkey.io/commands/dump/
  ```

- `redis/librdb`（**source code / primary**：目的、SAX風、RESP生成等／MIT license／changelog）citeturn9view1turn9view0turn8view1  
  ```text
  https://github.com/redis/librdb
  https://raw.githubusercontent.com/redis/librdb/main/LICENSE
  https://github.com/redis/librdb/blob/main/CHANGELOG.md
  ```

- Rust crate `rdb` docs（**crate metadata & docs / primary-ish**：RDB解析→JSON/RESP等へダンプの目的、owner情報）citeturn11view2turn11view1  
  ```text
  https://docs.rs/rdb/latest/rdb/
  https://fnordig.de/2025/03/29/state-of-maintenance-rdb-rs/
  ```

- Redis CLI docs（**official documentation / primary**：`redis-cli --rdb` がreplication初回同期を利用してRDBを取得、`--replica`など）citeturn24view0  
  ```text
  https://redis.io/docs/latest/develop/tools/cli/
  ```

- Valkey CLI docs（**official documentation / primary**：`valkey-cli --rdb` / `--functions-rdb` / `--replica` 等）citeturn24view1  
  ```text
  https://valkey.io/topics/cli/
  ```

- Redis `rdb.c` ライセンス表記（**source code / primary**：AGPLv3等の表記があり、コード流用はライセンスリスクが高い）citeturn0search21  
  ```text
  https://download.redis.io/redis-stable/src/rdb.c
  ```

- Go系RDBツール例（**source code / secondary**：writer主張やサイドカー候補の例示）citeturn1search18turn2search17  
  ```text
  https://github.com/HDT3213/rdb
  ```