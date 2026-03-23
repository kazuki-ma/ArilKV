# Rust製Redis互換サーバ向け Full-Sync レプリケーション設計レポート

## 概要と比較

### 1. Executive Summary

- 単一の論理Redis互換インスタンスとして外部公開する以上、**フル同期（スナップショット）とその後のコマンドストリームの境界は、単一の「グローバル cutover（論理時点）」で定義する設計が最も防御的**である。理由は、Redis/Valkeyのレプリケーションが「単一のレプリケーションストリーム上の**バイト単位オフセット**」を基準に、PSYNCで欠落区間を取り戻す前提だからである。citeturn7search3turn2view0turn14view0  
- Redis/Valkey系の実績あるフル同期は、**(a) 時点整合なスナップショット（RDB）**を送り、**(b) その作成・転送中に発生した更新を別途バッファして**、RDB転送後に**欠落なく送る**（= gapなし）という二層構造で成立している。Redisはこの間、レプリカ向け出力バッファ/バックログを使う。citeturn7search3turn10view0turn34view1  
- 近年のValkeyは、フル同期時の「更新差分の滞留（COB: client output buffers）」問題を軽減するため、**RDB転送用の別コネクション（rdb-channel）と、更新ストリーム用のmain-channelを分離**し、**RDB転送中も更新を並行ストリーミング**する（dual-channel replication）。citeturn32view0turn33view0turn32view1turn32view2  
- あなたのサーバが**actor/owner-thread + 内部シャード**であり、Redisのfork/CoWプロセスモデルをそのまま踏襲しないなら、フル同期の要は「forkの代替となる**時点整合スナップショット**」である。DragonflyDBが採るような、**シャード内epoch（世代）+ 走査スレッド + 書き込みフックによる“論理カット固定”**は、actorベースでも実装可能な実証済みパターンに近い。citeturn23view0turn23view1  
- 推奨アーキテクチャは、**単一グローバルcutover offset**を固定し、その時点の全シャードを**“バリア（marker）”で揃えた上で、オンライン（非停止）スナップショット**をRDBとして生成すること。RDB生成中の更新は、**(1) 共有バックログ（リング/ブロックリスト）**に必ず積み、必要に応じて**(2) フル同期セッション専用バッファ**で保護する。citeturn10view0turn12view0turn14view0turn34view0  
- 下り（あなたのサーバ→Redis/Valkeyレプリカ）では、互換性の観点からまず「**単一チャネル（従来互換）**」で正しさを達成し、その後、Valkey互換を狙って**dual-channelをオプション（capability交渉）**として実装するのが段階的に安全である。Valkeyはmain-channel/rdb-channelのクライアント種別までINFOで明確化している。citeturn32view1turn33view0turn32view2  
- 上り（Redis/Valkeyプライマリ→あなたのサーバ）では、RDBロード中に更新ストリームが到着し得るため、**(A) RDBをストリームロードしつつ更新をバッファ**し、ロード完了後に**オフセット順に適用**する（または、読み取りを止めて上流に自然バックプレッシャをかける）が王道。Redis/Valkey側にも出力バッファ上限があり、読み手が遅いと切断され得るため、受信側もバッファ上限/退避が必要になる。citeturn34view1turn26view0turn7search3  
- per-shard独立cutoverは、**単一インスタンス公開**のままでは基本的に危険（multi-keyや“同一論理順序”の破綻）で、**安全になるのは「シャード＝外部に独立公開され、独立のレプリケーションログ/オフセット空間を持つ」トポロジ（= cluster/パーティション境界が外部契約）**に限られる。Redis Cluster/Redis Enterpriseはまさにその前提でシャードを扱う。citeturn24search7turn24search3turn24search1  
- 失敗モードは「境界ずれ」「欠落」「二重適用」「バッファ枯渇」「遅いレプリカによるメモリ膨張」が中心で、Redis/Valkeyもclient-output-buffer-limit等で“遅いレプリカを切る”設計思想を持つ。Valkey dual-channelはCOBをレプリカ側へ寄せて一次側負荷を下げる方向。citeturn34view1turn32view0turn33view0turn21view5  
- 検証は、単体のRDB互換だけでは不十分で、**境界不変条件（invariants）に対するプロパティテスト + Redis/Valkey実機相互運用 + 障害注入（切断/遅延/バッファ枯渇）**が必要。レプリケーションを「ログ＋スナップショット」のSMR（State Machine Replication）として扱い、スナップショット収集/インストールのインタフェースを明確化すべき。citeturn35search2turn35search1turn35search0  

### 2. System Comparison Matrix

| System / mechanism | snapshot strategy | cutover strategy | backlog strategy | sharding story | strengths | weaknesses | source classification |
|---|---|---|---|---|---|---|---|
| Redis OSS replication / PSYNC / diskless | fork/CoWで“BGSAVE時点”の整合スナップショット（RDB）。disklessはEOF markerでストリーム転送も可能。citeturn7search3turn34view2turn30view2 | FULLRESYNC時にオフセットを提示し、その時点以降の差分をレプリカへ送れるよう状態をセット（WAIT_BGSAVE_END等）。オフセットはレプリケーションストリーム上のバイト位置。citeturn10view0turn14view0turn2view0 | repl-backlog（固定サイズ）+ replica client output buffer。遅いレプリカはclient-output-buffer-limitで切断。citeturn34view0turn34view1turn14view1 | 単一インスタンスは単一ストリーム。clusterはシャードごとに非同期複製。citeturn24search7turn7search3 | 実績最大。PSYNC/backlogで再接続に強い。設定も運用知見が多い。citeturn7search3turn34view0 | fork/CoW前提はactor型・マルチスレッド設計と相性が悪い場合がある（OS依存/停止時間/メモリ）。単一チャネルではフル同期中の差分が滞留しやすい。citeturn34view2turn34view1 | official product doc: Redis replication docs, redis.conf citeturn7search3turn34view1 / official source code: replication.c citeturn10view0turn14view0turn30view2 |
| Valkey replication + dual-channel | 基本はRDB。ただしdual-channel時はRDBをrdb-channelへ分離し、子プロセスが直接RDB送信し得る。citeturn33view0turn32view2turn32view0 | main-channelで増分（レプリケーションストリーム）を並行送信し、レプリカ側でローカルバッファに貯めてRDBロード後に適用。FULL同期の成功率と遅延を改善。citeturn33view0turn32view0turn32view1 | COB圧を一次側からレプリカ側へ移し、一次側メモリ/CPU負荷を低減。設定で有効化可能。citeturn32view0turn32view2turn32view3 | 基本はRedis互換。INFOでmain-channel/rdb-channelの種別を可観測化。citeturn32view1 | フル同期中の「差分滞留」問題を構造的に緩和。TLS制約下での子プロセス直送も狙う。citeturn33view0turn32view0 | 互換性は相手実装のcapability依存。二重コネクション/状態機械が増え実装複雑度が上がる。citeturn33view0turn32view2 | official product doc: Valkey blog/INFO/valkey.conf citeturn32view0turn32view1turn32view2 / official source code: PR #60 citeturn33view0 |
| Redis “RDB channel for full sync” work（参考） | rdb-channel概念（フル同期用の別チャネル）を実装に取り込みつつある（unstable）。disklessLoadingRioをabort可能にする等、二重チャネル前提の安全策も見える。citeturn28view3turn30view1turn30view3 | main-channelとrdb-channelの関連付け（main-ch-client-id）や、rdb-onlyクライアントの扱いなど、プロトコル拡張要素が増えている。citeturn30view1turn30view3 | rdb-onlyやrdb-channel等、役割別に切断や状態遷移を制御。citeturn30view1turn30view3 | cluster/slot migration文脈とも絡む。citeturn10view0turn28view3 | dual-channel設計が“Redis系”でも必要とされている兆候。互換設計の方向性を読む材料。citeturn28view3turn30view3 | unstable/将来仕様であり、互換契約として固定されていない可能性。citeturn28view3 | official source code: redis/redis replication.c (raw) citeturn30view1turn30view3 |
| Redis Enterprise / Redis Software（active-passive “Replica Of”含む） | データベースは複数シャードで構成し、各シャードにレプリカシャードを持つ。citeturn24search1turn24search3 | “Replica Of”などで初期ロード後に書き込み同期。gradual sync（並列度制御）やシャード単位の動きが見える。citeturn24search15turn24search4 | 既定で**シャードあたり**のレプリケーションバックログを持ち、サイズの自動設定指針も記載。citeturn24search4turn24search15 | シャード=独立プロセス/単位として設計され、プロキシとデータパスを分離。citeturn24search3turn24search1 | “外部公開単位=シャード”が前提のため、per-shard backlogや並列同期が自然に成立。運用面の制御点（gradual/size）が豊富。citeturn24search15turn24search4 | OSS互換“単一インスタンス”とは前提が異なる（シャーディングが基本）。内部仕様は閉じている部分も多い。citeturn24search3turn24search1 | official product doc: Redis Software docs citeturn24search1turn24search4turn24search15turn24search3 |
| DragonflyDB（Redis互換エンジン例） | fork無しのポイントインタイム・スナップショット設計を文書化。シャードepochをカットとして固定し、走査+書き込みフックで整合性を保つ。citeturn23view0 | “仮想カット（virtual cut）を全シャードに適用”という発想で、並行書き込み下でのスナップショット分離を説明。citeturn23view0 | レプリケーション用スナップショットでは差分ログ保持がメモリ圧になる点を問題視し、緩和策（差分の扱い）まで議論。citeturn23view0 | shared-nothingのマルチシャード前提。外部APIはRedis互換だが内部は大きく異なると明言。citeturn23view1turn23view0 | actor/owner-thread系に近い“シャード単位”でのスナップショット思考が得られる。CoW/fork依存を減らす設計の参考になる。citeturn23view0 | 設計議論には“RDBフォーマット拡張”など互換外の提案も含まれ得る（そのままRedis互換には使えない部分がある）。citeturn23view0 | vendor engineering content: Dragonfly docs citeturn23view0turn23view1 |
| RedisShake（移行/同期ツールのカットオーバーパターン） | “ソースへレプリカとして接続し、RDB（フル）+ AOFストリーム（増分）”を受ける。フルはコマンドへ展開して適用、増分は継続同期。citeturn26view0 | フル同期と増分同期を分離し、フル中に到着した増分を取り扱う（ディスク一時保存等）。citeturn26view0 | 受信データを一時的にディスクへ置く設計が明示されており、バッファ肥大への現実的対処になる。citeturn26view0 | cluster/sentinel等のトポロジ別注意点を整理。citeturn26view1 | “フル+増分”の運用・実装上の落とし穴（バッファ/再開不可など）が明文化されており、検証/運用設計の参考になる。citeturn26view1turn26view0 | 長期同期には不向き（checkpoint不可等）。レプリケーションそのものの実装ではなく、ツールとしての制約がある。citeturn26view1 | vendor engineering content: RedisShake docs（OSSだが製品/ツール文脈） citeturn26view0turn26view1 |
| Mechanism: Replicated log + snapshot install（SMR/論文パターン） | “ログが無限成長するため、スナップショット取得とインストールが必要”というSMRの典型インタフェース。citeturn35search2turn35search1 | cutoverは“スナップショットが表すログ位置（index/offset）”で定義し、以降のログを適用して追いつく。citeturn35search1turn35search2 | backlog=ログ。保持窓（保持期間/サイズ）を設計し、範囲外ならフル同期へフォールバック。 | シャーディングは“独立SMR”か“並列SMR”で扱いが分かれる（設計次第）。citeturn35search2 | 不変条件を形式化しやすい。境界の正しさ（snapshotはログprefixの圧縮）が明確。citeturn35search2turn35search1 | Redis互換の“コマンドストリーム=ログ”へ落とす実装はできるが、RDB/PSYNCなど既存プロトコル制約に合わせる工夫が要る。 | research / paper: Raft, SMR論文 citeturn35search1turn35search2 |

## 推奨アーキテクチャ

### 3. Recommended Architecture

#### one primary recommendation

**「単一グローバルcutover offset + シャード同期バリア + オンライン（非停止）ポイントインタイムRDB生成 + 共有バックログ + フル同期セッション保護」**を基本線とする。

この設計は、Redis/ValkeyがPSYNCで扱う“レプリケーションオフセット=バイト位置”という契約に沿って、スナップショットと増分ストリームを「同一の線形履歴（単一ストリーム）」として定義できる点で最も防御的である。citeturn2view0turn14view0turn34view0

**中核アイデア（要点）**

- **cutoverを`O`（グローバルレプリケーションオフセット）で定義**し、RDBは「ログprefix（≤O）を反映した状態」を表現する。増分ストリームは「O以降」を必ず送る。citeturn10view0turn14view0turn7search3  
- 内部シャードは、**“スナップショット開始バリア”を全シャードに配送して同一論理点に揃える**。これは分散スナップショットにおけるmarker発想と一致し、全体整合のカットを作る典型である。citeturn35search0turn23view0  
- スナップショット生成方法は、fork依存を避けるために、Dragonflyのような**epoch固定 + 走査 + 書き込みフック（before-image送出）**で“開始時点の整合スナップショット”を得る（conservative snapshottingに相当）。citeturn23view0turn34view2  
- 増分は、**共有バックログ（リング/ブロックリスト）**に常に蓄積し、PSYNCにはバックログ範囲で応答、範囲外ならFULLRESYNCへフォールバックする（Redis/redis.confが想定する運用モデル）。citeturn34view0turn12view0turn14view1  
- フル同期中の“差分保持”は、(a) 共有バックログだけに頼ると溢れ得るため、**フル同期セッションごとに「保護参照」**（例: バックログの先頭ブロックrefcount固定）または**セッション専用バッファ（上限付き）**を併用し、溢れたら切断してやり直す（Redis/Valkeyも遅いレプリカは切断する方向）。citeturn34view1turn20view5turn32view3  
- 相手がValkeyでdual-channel対応なら、将来的に**RDB転送（rdb-channel）と増分（main-channel）を分離**してフル同期成功率と一次側負荷を改善可能（capability交渉で選択）。citeturn33view0turn32view1turn32view2  

#### one fallback recommendation

**「グローバル書き込み停止（短時間）でのスナップショット生成 + 共有バックログのみ」**を、Phase 1の保守的フォールバックとして用意する。

- 実装が最短で、cutover不具合（欠落/重複）を出しにくい。  
- ただしデータサイズが大きいと停止時間が許容できず、長期的にはオンラインスナップショットへ移行が必要（Redisがfork/CoWで回避してきた問題に相当）。citeturn34view2turn23view0  

#### why

- **プロトコル契約（PSYNC/オフセット/バックログ）と整合する**：オフセットが“バイト単位の単一ストリーム”である以上、cutoverも単一でないと“スナップショット=ログprefix”が定義できない。citeturn2view0turn14view0turn34view0  
- actorベースに適合：forkに頼らず、シャードごとに責務分離しつつ全体整合カットを作れる（Dragonflyのepoch固定設計が近い参照例）。citeturn23view0turn23view1  
- 実運用の失敗モード（遅いレプリカ/バッファ肥大）に対して、既存Redis系が持つ“切断・再同期”モデルに沿って防御できる。citeturn34view1turn20view5turn32view3  

## 正しさの条件とシャード分析

### 4. Cutover Invariants

フル同期境界の正しさを、実装・テスト・障害解析で使える形に落とすため、最低限以下を**不変条件（invariants）**として固定する（“満たせない場合は切断してFULLRESYNCへフォールバック”の判断基準にもする）。

- **単一ストリーム不変条件**：公開インスタンスに対して、レプリケーションストリームは単一であり、グローバルオフセットは単調増加する。Redis系ではレプリケーションストリームのバイト数に応じてオフセットが進む前提が明示されている。citeturn2view0turn14view0  
- **prefix-snapshot不変条件**：RDBスナップショットが表す状態は、あるcutover offset `O`までのストリーム（prefix）を適用した結果と同値である。RDBに入った更新は`≤O`、増分に入る更新は`>O`。citeturn7search3turn10view0turn33view0  
- **exactly-once効果（境界）**：任意の更新効果は「RDBに含まれる」か「増分ストリームに含まれる」のどちらか一方で、**両方に跨って二重に適用されない**。  
- **gap-free**：レプリカ側がRDBをインストールし、`O+1`以降の増分を順序通り適用すれば、プライマリ状態へ収束する（欠落がない）。Redis/ValkeyのFULLRESYNC→増分という設計自体がこの意図である。citeturn7search3turn33view0  
- **順序保存**：増分ストリーム内のコマンド順序は、プライマリでの実行順序と一致する（他コマンドとの相対順序が保存される）。Redisは“同一レプリケーション履歴をプロキシすることで同一replid/offsetを保つ”旨をコードコメントで示す。citeturn14view2  
- **マルチDB整合**：`SELECT`などDBコンテキスト変更は、増分ストリーム上で正しく再現される（新規レプリカ向けにSELECTを再送する等）。Redis実装はフル同期セットアップ時にSELECTを強制再発行する意図をコメントしている。citeturn10view0turn14view2  
- **遅いレプリカ防御**：レプリカがフル同期/増分適用に追いつけず、バッファ上限に到達したら“切断してやり直す”が安全側。Redisはclient-output-buffer-limitで同系統の制御を提供する。citeturn34view1turn32view3  

### 5. Shard/Epoch Analysis

#### per-shard epochs are safe or unsafe?

- **単一の論理Redisインスタンスとして公開する限り、per-shard独立cutover epochは原則Unsafe**。  
  - 理由は、PSYNCの要求・応答が“単一のreplidと単一オフセット空間”を前提にしており、レプリケーションストリームが単一線形である以上、スナップショットもそのprefixとして定義される必要があるためである。citeturn2view0turn14view0turn34view0  
  - もしシャードごとに異なるcutoverを許すと、「キーA（シャード1）はRDBに入ったが、キーB（シャード2）は増分側」など、**同一コマンドの効果が境界で分割**される危険が生じる。これはmulti-keyコマンドやトランザクション境界の意味論を壊しやすい。  

- **ただし、各シャードが「外部に独立公開される複製単位（=独立のプライマリ/レプリカ、独立のログ/オフセット）」であるなら、per-shard epochはSafeになり得る**。  
  - Redis Clusterは、シャード（ノード）間で非同期複製し、データはスロット分割されるという前提を仕様で述べる。citeturn24search7  
  - Redis Enterpriseは、データセットをシャードに分割し、**各シャードにレプリカシャード**を持つ“シャード単位複製”を説明している。citeturn24search1turn24search3  

#### Can shards snapshot at different wall-clock times if pinned to one logical cutover?

- **可能（推奨）**。ただし条件は「スナップショットが“論理カット（epoch/marker）”に対して整合である」こと。  
  - Dragonflyの説明は、スナップショット開始時にシャードのepoch（カット）を取り、その後は走査と書き込みフックで“カット以前の値”をシリアライズすることでポイントインタイム整合を維持する、という形でこれを説明している。citeturn23view0  
  - 分散スナップショットのmarker発想（Chandy-Lamport）は「全体で意味あるカットを作る」ための理論的基盤で、actorシャード間の“バリアメッセージ”設計に対応する。citeturn35search0  

## 設計詳細

### 6. Downstream Export Design

#### data structures

- **GlobalReplicationLog（共有バックログ/共有送信バッファ）**  
  - 実体：固定長リング、またはRedisが採用する「ブロックリスト + 参照カウント」方式。Redis実装では、レプリケーションバッファへ追記する際に`master_repl_offset`をバイト数分進め、バックログ長も更新している。citeturn14view0turn12view0  
  - 各レプリカは「自分が参照している開始ブロック/位置」を保持し、トリムは“参照がなくなった範囲だけ”行う（refcountの考え方が必要）。Redisのバックログ生成時には“次に生成されるバイトが先頭”というオフセット定義も見える。citeturn12view0turn12view1  

- **FullSyncSession（フル同期セッション状態）**  
  - `cutover_offset O`：このセッションのRDBが表す論理時点。  
  - `snapshot_handle`：RDBストリーム生成器（複数シャードのイテレータを束ねる）。  
  - `suffix_stream_guard`：O以降の増分が失われないよう、バックログの保持を“このセッションが追いつくまで”保証する仕組み（例: バックログ先頭ブロックのrefcount固定、またはセッション専用バッファ）。Redis側でも遅いレプリカは出力バッファ制限で切断する思想があり、上限設計が必要。citeturn34view1turn32view3  

- **ShardSnapshotView（シャード時点整合ビュー）**  
  - `cut_epoch`：シャード内の論理カット。  
  - `scan_cursor`：キー空間走査カーソル。  
  - `on_write_hook`：cut以前の値が未シリアライズのまま変更される場合、before-imageをスナップショット側へ送る。Dragonflyのconservative snapshottingがこのパターンを具体化している。citeturn23view0  

#### sequencing

**従来互換（single channel）**

1) レプリカ接続（`PSYNC ? -1`や`SYNC`）を受け、フル同期へ。  
2) `cutover_offset O`を確定（＝“この後に送るRDBが表す論理時点”）。RedisではFULLRESYNCのセットアップでオフセットをレプリカ構造体へ覚え、差分を蓄積し始める状態へ遷移する意図が明示される。citeturn10view0  
3) RDB生成を開始（シャードへバリア）。RDB生成中もプライマリは通常通り更新を受ける。更新は**必ずGlobalReplicationLogへ追記**される（単調オフセット）。citeturn14view0turn7search3  
4) RDB転送完了後、`O+1`以降のログ（バックログ/セッション保護領域）をレプリカへ流し、追いついたら通常ストリームへ合流。  

**Valkey互換（dual-channelオプション）**

- 相手がcapabilityを提示した場合、ValkeyのPRが示すように「まずmain-channelで`+DUALCHANNELSYNC`等を返し、レプリカがrdb-channelを張り、プライマリは“スナップショット終端offset”を提示、main-channelはそのoffsetからPSYNCする」モデルに寄せられる。citeturn33view0turn32view0turn32view1  
- Valkey側は`type=main-channel / rdb-channel`という可観測性まで整備しているため、同互換を狙うならあなたのINFO/REPLICA LISTにも相当概念を入れるのが運用上有利。citeturn32view1  

#### reconnect behavior

- **PSYNC成功条件**：要求offsetがバックログ範囲内であること。redis.confは、バックログを大きくすれば断時間に耐え「フル同期を避けられる」旨を説明している。citeturn34view0  
- **バックログ範囲外**：FULLRESYNCへ。Valkeyスライドやログ例でも“lack of backlog→full sync”が示される。citeturn20view7turn34view0  
- **フル同期中の切断**：セッション保護領域を維持できない（メモリ上限・保持窓超過）なら、切断して再度FULLRESYNC。Redis/Valkeyはclient output buffer limitで遅いレプリカを切断するモデルを持つ。citeturn34view1turn21view0turn20view5  

### 7. Upstream Ingest Design

#### sequencing

上り（外部Redis/Valkey→あなたのサーバ）では、受信側が“RDBをインストールするまで更新を適用できない”期間が生じる点が本質である。

1) `REPLICAOF`相当で接続し、`PSYNC`/`SYNC`へ進む。  
2) `+FULLRESYNC <replid> <offset>`とRDBペイロードを受け取る（互換）。“フル同期=スナップショット転送 + その後更新ストリーム継続”という流れはRedis/Valkeyの基本。citeturn7search3turn10view0turn33view0  
3) **RDBをストリーミングパース**し、各キーを担当シャードへ配送して“ロード用ステージ領域”へ構築する。  
4) RDB受信完了後に到着する増分コマンドは、順序通り適用する必要があるため、ロード完了まで**バッファ**する。RedisShakeは“フル（RDB）と増分（AOFストリーム）を両方受け、一時的にディスクへ置き、フルをコマンドへ分解して適用、その後増分を継続同期”という運用パターンを文書化している。citeturn26view0  
5) 全シャードのステージ構築が完了したら、**バリアで一括publish（ポインタスワップ）**し、その後にバッファした増分を`offset`順に適用して追いつく。  

#### staging/publish model

- **推奨：double-buffer（ステージ→アトミックpublish）**  
  - フル同期開始時点で旧データセットを破棄するか保持するかは運用方針だが、Redis系は“新しいプライマリへ付け替えると旧データセットを捨てる”という挙動を採る例がある（DragonflyのREPLICAOF説明も同旨）。citeturn22search20  
  - actorモデルでは「各シャードが自分のステージ領域を持つ」形にし、publishバリアで可視化を切り替えると、読み取り側の一貫性が保ちやすい。  

#### buffering model（上り）

- **最小実装（Phase 1向け）**：メモリ内リングに“受信した増分フレーム（RESP生データ長付き）”を貯め、ロード完了後に再パースして適用。  
- **プロダクション防御（推奨）**：メモリ上限を超える場合に備え、RedisShakeのように**一時ディスクスプール**（順序保持）を用意し、上限時は切断→再同期も許容する。citeturn26view0turn34view1  
- **バックプレッシャ**：読み取りを止めて上流を詰まらせる手もあるが、上流側にはclient-output-buffer-limitがあり、遅いレプリカとして切断され得る。したがって“止める”単独では防御的でない。citeturn34view1  

## 計画とリスク

### 8. Phased Delivery Plan

#### Phase 1: minimum correct production slice

- **RDB ingest（上り）**：Redis/ValkeyからのFULLRESYNCで受けたRDBを、最低限の型（string/hash/list/set/zset/stream…優先度順）＋expire＋DB選択を含めてロードできる。  
- **RDB export（下り）**：あなたの内部状態から、同じ型セットでRDBを生成できる。cutoverは「グローバルバリア + （最初は短時間書き込み停止でも可）」で正しさ優先。citeturn7search3turn34view2turn35search2  
- **共有バックログ**：PSYNCの要求offsetがバックログ範囲内ならCONTINUE、範囲外ならFULLRESYNCへ。redis.confが説明するrepl-backlog-size/ttlと同等の運用パラメータを持つ。citeturn34view0turn12view0turn14view0  
- **遅いレプリカの切断**：replica向け出力/セッションバッファ上限を実装し、超過時は切断→再同期へ（Redisの設計思想に合わせる）。citeturn34view1turn32view0  

#### Phase 2: performance hardening

- **オンライン（非停止）ポイントインタイムスナップショット**：Dragonfly型のepoch固定 + 走査 + 書き込みフックで、停止時間をほぼゼロへ近づける。citeturn23view0  
- **フル同期セッション保護の最適化**：共有バックログのrefcount/保護範囲を整備し、“フル同期中にバックログが上書きされない”保証を上限付きで実装。citeturn12view0turn14view1  
- **RDB生成の並列化**：シャードごとにスナップショットイテレータを回し、RDBストリームを統合（DB切替や型順序はRDBフォーマットに合わせる）。  

#### Phase 3: advanced optimizations

- **dual-channel export（Valkey互換のcapability交渉）**：Valkey PRが記述するmain-channel/rdb-channel手順を実装し、COB滞留・一次側CPUを減らす。設定フラグやINFO可観測性もValkeyに合わせる。citeturn33view0turn32view2turn32view1  
- **受信側の大規模バッファ耐性**：増分バッファをディスクへ退避し、極端に遅いRDBロードでも再同期を減らす。RedisShakeの“RDB+増分ストリーム受信→一時保存→適用”を参照。citeturn26view0  
- **形式モデル（TLA+/Alloy等）とログ/スナップショットの境界証明**：SMRの“snapshot install”不変条件（ログprefix圧縮）をモデル化し、境界バグを系統的に潰す。citeturn35search2turn35search1  

### 9. Risk Register

- **snapshot built too early / too late（境界ずれ）**  
  - 影響：欠落または二重適用。  
  - 緩和：`cutover_offset O`を唯一の真実として、RDB生成・増分送信・レプリカ適用の全経路でOを埋め込み、監査ログに残す（FULLRESYNC応答オフセットの設計意図はRedis/Valkey実装にある）。citeturn10view0turn33view0  

- **writes missed during snapshot generation（スナップショット作成中の欠落）**  
  - 影響：レプリカが永遠に追いつかない。  
  - 緩和：全書き込みは必ずグローバルログへ記録し、スナップショット中もログ追記を止めない（Redisはレプリケーションバッファ追記で`master_repl_offset`を進める）。citeturn14view0  

- **writes duplicated across snapshot and suffix stream（二重適用）**  
  - 影響：SET系は上書きで目立たないが、INCR/LPUSHなどは致命的。  
  - 緩和：シャードスナップショットは“cut以前の値だけ”を確実にemitし、cut後の変更はログ側にのみ残す（Dragonflyのconservative hook発想）。citeturn23view0  

- **cross-shard command split（multi-keyが境界で割れる）**  
  - 影響：一貫性崩壊。  
  - 緩和：単一インスタンスでは必ずグローバルcutover。バリアは“グローバル順序”に挿入し、multi-key処理のコミットとスナップショット境界は同じ順序化機構で制御する（分散スナップショットのmarker発想）。citeturn35search0turn2view0  

- **replica disconnect during backlog drain（増分ドレイン中の切断）**  
  - 影響：再同期増・負荷。  
  - 緩和：PSYNCに対応するbacklog窓を適切に確保（repl-backlog-size）。ただし遅いレプリカは切断して健全性を守る（client-output-buffer-limit）。citeturn34view0turn34view1  

- **memory blow-up from per-session buffering（フル同期セッション専用バッファ肥大）**  
  - 影響：OOM。  
  - 緩和：上限・切断・再同期。Valkey dual-channelのように“フル同期中も増分を送る”最適化を将来導入（ただし互換性/実装複雑度と交換）。citeturn32view0turn33view0turn32view3  

- **interaction with cluster mode（将来のcluster化）**  
  - 影響：単一ストリーム前提が崩れる。  
  - 緩和：cluster時は“シャード=外部複製単位”へ切り替える（Redis Cluster/Redis Enterprise型）。単一インスタンス時はグローバルcutoverを維持。citeturn24search7turn24search1turn24search3  

## 検証と参考文献

### 10. Validation Plan

#### correctness

- **不変条件テスト（Cutover Invariants）**：  
  - 任意ワークロード下で `snapshot_state(O) + apply(log>O)` がプライマリ状態と一致すること（exactly-once/gap-free）。  
  - multi-key/トランザクション境界を挟んだときに、境界割れが起きないこと。  
- **プロパティテスト**：ランダムコマンド列 + ランダム切断/再接続（PSYNC範囲内/外）で、最終的に一致または安全側にFULLRESYNCへフォールバックすること。redis.confが想定する“backlog窓超過→フル同期”を検証条件にする。citeturn34view0turn7search3  

#### interop

- **相互運用マトリクス**（最低限）  
  - Redis(6.x/7.x/最新)→あなた（レプリカ）：FULLRESYNC+RDB ingestの型互換。  
  - あなた（プライマリ）→Redis/Valkey（レプリカ）：FULLRESYNC+RDB export、PSYNC。  
  - Valkey(8.x/9.x)→あなた、あなた→Valkey：まず単一チャネル、次にcapabilityが整ったらdual-channelを追加。ValkeyはINFOでmain-channel/rdb-channelを区別しているため、検証観点に使える。citeturn32view1turn33view0  

#### performance

- **フル同期中のp99遅延**（書き込み/読み込み）と、同期時間、バックログ/COBメモリを測定。Valkeyはdual-channelで“同期時間短縮やメモリ削減”をベンチ観点として挙げているため、同じメトリクスが比較可能。citeturn32view0turn32view3  
- **遅いレプリカ耐性**：帯域制限/遅延注入し、切断や再同期が“想定通り”起きるか（client-output-buffer-limit互換）。citeturn34view1turn20view5  

#### formal methods

- **分散スナップショット（marker）**：バリア設計が“意味ある全体カット”を形成することを、Chandy-Lamportの枠組み（考え方）で点検する。citeturn35search0  
- **ログ＋スナップショット（SMR）**：スナップショット収集/インストールを明示したインタフェース（snapshot/ install）としてモデル化し、境界条件（prefix-snapshot）を検証する。citeturn35search2turn35search1  

### 11. References

（分類は指定の4種類に限定。URLは要件に従いインラインコードで記載。）

- official product doc — `https://redis.io/docs/latest/operate/oss_and_stack/management/replication/` citeturn7search3  
- official product doc — `https://redis.io/docs/latest/commands/psync/` citeturn3search7  
- official source code — `https://github.com/redis/redis/blob/unstable/src/replication.c`（FULLRESYNCセットアップ/SELECT再送/等） citeturn10view0turn14view2  
- official source code — `https://raw.githubusercontent.com/redis/redis/refs/heads/unstable/src/replication.c`（REPLCONF rdb-only / EOF marker / rdb-channel handshake 等） citeturn30view1turn30view2turn30view3  
- official product doc — `https://raw.githubusercontent.com/redis/redis/6.2/redis.conf`（repl-backlog-size/ttl、client-output-buffer-limit、diskless load 等） citeturn34view0turn34view1turn34view2  
- official product doc — `https://valkey.io/topics/replication/` citeturn31search2  
- official product doc — `https://valkey.io/blog/valkey-8-0-0-rc1/`（dual-channel replicationの狙い/効果） citeturn32view0  
- official product doc — `https://valkey.io/commands/info/`（main-channel/rdb-channelの可観測性） citeturn32view1  
- official product doc — `https://raw.githubusercontent.com/valkey-io/valkey/8.0/valkey.conf`（`dual-channel-replication-enabled`） citeturn32view2  
- official source code — `https://github.com/valkey-io/valkey/pull/60`（dual-channel high level interface design） citeturn33view0  
- official product doc — Redis Software / Replica Of backlog（per-shard backlog sizing） `https://redis.io/docs/latest/operate/rs/databases/import-export/replica-of/` citeturn24search4  
- official product doc — Redis Software / gradual sync & backlog settings（BDB object） `https://redis.io/docs/latest/operate/rs/references/rest-api/objects/bdb/` citeturn24search15  
- official product doc — Redis Enterprise cluster architecture（proxy/shard分離） `https://redis.io/technology/redis-enterprise-cluster-architecture/` citeturn24search3  
- vendor engineering content — Dragonfly Point-in-Time Snapshotting Design `https://www.dragonflydb.io/docs/managing-dragonfly/snapshotting` citeturn23view0  
- vendor engineering content — Dragonfly Replication `https://www.dragonflydb.io/docs/managing-dragonfly/replication` citeturn23view1  
- vendor engineering content — RedisShake Sync Reader（RDB+増分ストリーム、ディスク一時保存） `https://tair-opensource.github.io/RedisShake/en/reader/sync_reader.html` citeturn26view0  
- research / paper — Chandy & Lamport “Distributed Snapshots” `https://lamport.azurewebsites.net/pubs/chandy.pdf` citeturn35search0  
- research / paper — Ongaro & Ousterhout “Raft” `https://raft.github.io/raft.pdf` citeturn35search1  
- research / paper — “On the Efficiency of Durable State Machine Replication” `https://www.usenix.org/system/files/conference/atc13/atc13-bessani.pdf` citeturn35search2