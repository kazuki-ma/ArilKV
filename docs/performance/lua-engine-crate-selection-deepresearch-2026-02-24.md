# Rust製Redis互換サーバ向け Luaスクリプティング実装における crate／ランタイム選定と統合アーキテクチャ

## Executive Summary

- 互換性と移行容易性を最優先にするなら、Luaランタイムは **Lua 5.1** を前提にするのが最もリスクが低い（Redisは埋め込み Lua 5.1 で EVAL を実行する）。citeturn11view0turn12view0turn29view0  
- Rust側の現実的な第一候補は **mlua**（Lua 5.1/5.2/5.3/5.4/5.5、LuaJIT、Luau を feature で選択でき、vendored で静的ビルドも可能）。citeturn15view0turn24search7  
- **rlua は非推奨かつアーカイブ済み**で、新規採用は避けるべき（移行用の薄いラッパに位置付けられ、リポジトリも read-only）。citeturn17view0turn16view0  
- セキュリティ面では「**Lua sandbox を正しく作っても、パッケージング／モジュールロード経路が残ると脱出**」が現実に起きている（Debian系RedisのLua sandbox escape: CVE-2022-0543）。citeturn10search1turn10search14turn10search23  
- Redis互換として最低限必要なのは、**KEYS/ARGV と redis.call／redis.pcall**、キャッシュ（SHA1/ NOSCRIPT）、エラー表現（error_reply/status_reply）、Read-only制約（EVAL_RO 等）をまず揃えること。citeturn29view0turn11view1turn11view2turn12view0  
- 実運用に耐えるために、初期から **(a) スクリプト無効化のキルスイッチ、(b) 実行時間監視、(c) メモリ上限、(d) スクリプトキャッシュ上限（動的生成対策）** を入れるべき。citeturn12view0turn11view0turn31view2turn31view4  
- Redisはスクリプトを **原子実行（実行中はサーバ動作をブロック）** と定義し、長時間実行は危険だが、閾値超過でも自動停止しない（原子性破壊のため）。citeturn12view0  
- レプリケーション整合性は「**スクリプト本文を複製する（verbatim）**」よりも「**スクリプトが発行した write コマンド群を複製する（effects replication）**」が運用上の基盤になりつつあり、Redis 7.0 以降は effects replication のみがサポートされる。citeturn30view0  
- 「Pure Rust Lua VM」は魅力があるが、現時点でRedis互換の中核に置くのは慎重に（例：piccolo は “very experimental” で pre-1.0 破壊的変更が前提）。citeturn19view2  
- フェーズ導入では、**Phase 1 は read-only を既定（EVAL_RO / EVALSHA_RO）にし、write を明示的に opt-in** する形が安全・ロールバック容易。citeturn12view0turn30view0  

## Candidate Matrix

| Candidate | Maintenance status | License | Runtime dependency model | Redis-compat feasibility | Security/sandbox controls | Expected performance envelope | Adopt now / pilot / avoid |
|---|---|---|---|---|---|---|---|
| **mlua + Lua 5.1 (vendored)** | mlua は 2026-01-27 に更新（0.11.6）。Lua 5.1 feature を選択可能。citeturn6search17turn24search7turn15view0 | MITciteturn24search7 | C実装Lua（Lua 5.1）を埋め込み。vendored で lua-src 由来の静的ビルド。citeturn15view0 | Redis は Lua 5.1 を埋め込み実行。互換の土台として最適。citeturn12view0turn11view0turn29view0 | mlua は安全モードで unsafe 標準ライブラリ／Cモジュールを抑止できる設計。citeturn7view0turn31view2＋Redis互換sandbox（require無効等）を上乗せ。citeturn29view0turn28view0 | PUC-Lua相当の素直なVM。JITは無し。CPU/メモリ制御は hook/メモリ上限で担保可能。citeturn31view4turn31view1 | **Adopt now** |
| **mlua + LuaJIT (vendored)** | mluaでLuaJIT feature選択可、静的ビルドも可。citeturn15view0 | MITciteturn24search7 | LuaJIT（Lua 5.1互換系）を埋め込み。vendored で luajit-src。citeturn15view0 | Lua 5.1互換という点で高いが、LuaJIT固有差分・運用差分を吸収する必要。citeturn15view0turn11view0 | C/JITで攻撃面は増え得るため、Redis互換sandbox＋モジュールロード遮断は必須。CVE-2022-0543 が教訓。citeturn10search1turn10search14turn29view0 | JITを有効化できる設計がある（mlua側にも enable_jit が存在）。citeturn7view0 | **Pilot**（性能要件が強い場合） |
| **rlua (現行は移行ラッパ)** | GitHub上でアーカイブ済み・read-only。mluaへの移行用で新規推奨されない。citeturn16view0turn17view0 | MIT（docs.rs表示）citeturn9view0 | 実体は mlua の再エクスポート中心。citeturn17view0turn9view0 | 可能だが、長期保守観点で採用根拠が薄い。citeturn17view0turn16view0 | 旧API互換の利点はあるが、保守停止により脆弱性対応や将来互換が弱い。citeturn16view0 | 不確実（保守停止に起因）。 | **Avoid** |
| **piccolo (pure Rust Lua VM)** | 活発な議論はあるが「実験的」「pre-1.0破壊的変更が頻繁」と明記。citeturn19view2 | CC0-1.0 / MIT（両方含まれる）。citeturn18view0 | Pure Rust VM（C依存なし）。citeturn18view0turn19view2 | “PUC-Luaのいずれかに実用互換” を目標とするが、Redis互換を即保証する位置づけではない。citeturn19view1turn19view2 | 目的として sandbox/DoS耐性を掲げ、fuel・メモリ追跡の記述もある。citeturn19view2 | 目標は「遅すぎない」だが、互換・成熟度が主要リスク。citeturn19view1turn19view2 | **Avoid**（研究/PoCなら Pilot） |
| **mlua + Luau** | mluaがLuauをサポートし、Luau向けsandbox APIも存在。citeturn15view0turn8view2 | mlua: MIT、Luau: MIT（LICENSE）。citeturn24search7turn2search2 | Luau VM（mluaがfeatureで組込）。citeturn15view0 | Redis Lua 5.1 セマンティクスとは別系統のため、互換性は低い。citeturn11view0turn15view0 | Luau向けにライブラリread-only化等のsandboxスイッチが提供される。citeturn8view2 | Luau-JIT等の選択肢あり。citeturn15view0 | **Avoid**（Redis互換が必須なら） |
| **Wasmtime (WASM)で非Luaスクリプト** | Bytecode Alliance主導で成熟度は高いが、Redis Lua互換は別物。citeturn20search6turn21search0 | Apache-2.0 WITH LLVM-exception（公式）。citeturn21search3 | WASMランタイム。メモリ制限（ResourceLimiter）とCPU制御（fuel/epoch）の公式APIがある。citeturn20search6turn21search0turn21search5 | Redis Lua API互換はゼロから設計が必要。citeturn29view0turn11view0 | fuelは「無限実行を決定的に防ぐ」用途に使える（ただしコード計測/計装が必要）。citeturn21search0turn21search1 | fuel計装が必要で、性能トレードオフが明示される。citeturn21search0turn21search5 | **Avoid**（互換必須なら） |
| **Rhai（非Luaの純Rustスクリプト）** | ドキュメント・ライセンス情報が整備され、埋め込み用途を明確化。citeturn20search9turn20search11 | Apache-2.0 または MIT（公式）。citeturn20search11 | Pure Rustエンジン。citeturn20search9 | Redis Lua互換はない（クライアント資産を捨てる前提）。citeturn11view0turn29view0turn20search9 | “安全で簡単にスクリプトを追加”という目的だが、Redis互換要件とは別。citeturn20search9 | Redis互換の性能比較軸では評価不能。 | **Avoid**（互換必須なら） |

## Recommended Default

**Primary choice: mlua + Lua 5.1 (vendored) をベースに「Redis互換 sandbox + Redis Lua API」を実装する**

選定理由は3点に集約される。  
第一に、Redis側の仕様が Lua 5.1 を前提としており、EVAL は埋め込み Lua 5.1 インタプリタで実行されるため、互換性コストが最小になる。citeturn11view0turn12view0turn29view0  
第二に、mlua は Lua 5.1 / LuaJIT / Luau を feature で切り替え、vendored で静的ビルドできるため、OS/ディストリ差分（ABI・パッケージング）を抑えやすい。citeturn15view0  
第三に、mlua 自体が「安全モードで unsafe な標準ライブラリや C モジュールロードを抑止する」方針を明記しており、Redis互換の sandbox（require無効等）と整合する。citeturn7view0turn31view2turn29view0  

**Fallback choice: mlua + LuaJIT（必要時のみ）**

性能上の圧力が強い場合、Lua 5.1互換系として LuaJIT へスイッチできる余地を確保する。ただし、運用上の攻撃面（JIT／FFI／動的ロード経路）を徹底的に潰す必要があり、CVE-2022-0543 のような「sandbox 脱出」が再現し得るため、デフォルトには置かない。citeturn15view0turn10search1turn10search14turn29view0  

## Integration Blueprint

### VM lifecycle model

**推奨: owner-thread（シャード／ワーカ）ごとに Lua VM を1つ持つ（Per-owner-thread VM）**

Redisはスクリプト実行を原子的（実行中はサーバ活動をブロック）と定義しており、シングルスレッド実装と整合している。citeturn12view0  
Rust製Redis互換サーバが owner-thread モデル（キー空間がシャードに属する）であるなら、**そのシャードのイベントループ＝単一スレッド**でLua VMを駆動するのが最も単純で、スレッド安全性とデッドロック面の説明が容易になる（Lua state を跨ぐ共有を避けられる）。このモデルは、Redisが「スクリプトがアクセスするキーは必ず入力キー引数として渡し、プログラム生成したキー名を触るな」と強く要求する点とも相性がよい。citeturn11view0turn29view0turn14view0  

VM生成時は、**unsafeなライブラリやCモジュールロード経路を持たない状態**で初期化する。mlua は `Lua::new()` が「安全な標準ライブラリのサブセットをロードし、unsafeな標準ライブラリやCモジュールロードを許さない」旨を明記し、`unsafe_new` が全標準ライブラリをロードしCモジュールを許す。citeturn7view0turn31view2  
Redisのsandboxは `require` を無効化してモジュールロードを防いでいるため、この方針と一致する。citeturn29view0  

### Script cache model

**推奨: シャード内キャッシュ（per-VM）＋グローバルなメタ情報（オプション）**

Redisの Eval script は SHA1 ダイジェストで識別され、EVAL 実行でキャッシュに積まれる。SCRIPT LOAD は「実行せずにコンパイルしてキャッシュへロード」し、EVALSHA でSHA1指定実行する。citeturn11view1turn11view2  
また Redis の script cache は **常に volatile（永続化されず、再起動や failover、SCRIPT FLUSH 等で失われ得る）** と明記されているため、互換を狙うなら「永続キャッシュにしない」のが安全である。citeturn11view1  

実装としては以下の2層が現実的。

- **Compile cache**: `sha1 -> compiled_chunk_or_function_handle`（Lua registry 参照を保持）。  
- **Policy layer**: キャッシュ上限・LRU・メトリクス。Redis 7.4 では動的生成スクリプトによるメモリ枯渇対策として、EVAL/EVAL_RO でロードされたスクリプトを LRU で削除する挙動が入っている（evicted_scripts メトリクス）。citeturn11view0  

### Command execution flow

#### EVAL / EVAL_RO

- 入力: `script, numkeys, keys..., args...`。キー名は `KEYS`、通常引数は `ARGV` に入る。citeturn11view0turn29view0  
- 互換性の要: **スクリプトがアクセスするキー名は全て入力キー引数で明示し、生成キー名で触らない**（クラスタ／シャード整合性のため）。citeturn11view0turn29view0  
- 実行前: SHA1算出 → キャッシュになければ compile & store（EVAL相当）。citeturn11view1  
- 実行環境:  
  - `redis` singleton をLuaへ注入し、`redis.call` / `redis.pcall` をホスト関数で実装する（Redis Lua APIの中心）。citeturn29view0turn30view0  
  - `redis.call` は Redis コマンドを実行して結果を返し、ランタイム例外はユーザへエラーとして返る。citeturn29view0  
  - `redis.pcall` は例外を投げず、エラー時は `redis.error_reply` 型（Lua table の `err` フィールド）を返す。citeturn30view0  
- EVAL_RO（read-only）: `redis.call/pcall` 内で **write コマンドの発行を拒否**し、Redis同様にエラー化する。Redisは read-only script の概念と、read-only variant（EVAL_ROなど）を規定している。citeturn12view0turn29view0  
  - 注意点として Redis は `PUBLISH` / `SPUBLISH` / `PFCOUNT` もスクリプト内では write 扱いになる場合がある。citeturn12view0turn29view0  

#### EVALSHA / EVALSHA_RO

- Redis同様、`sha1` がキャッシュにない場合は `NOSCRIPT` エラーを返し、クライアントは SCRIPT LOAD → 再実行を行うのが基本フロー。citeturn11view1turn11view2  
- パイプラインでは `NOSCRIPT` が扱いづらいので、クライアントは EVAL にフォールバックするのが推奨とされる（互換テストではここが差分になりやすい）。citeturn11view1  

#### FCALL / FCALL_RO

- 関数は FUNCTION LOAD でロードされ、FCALL は `function name, numkeys, keys..., args...`。キーと引数は Lua callback の **第1引数（keysテーブル）／第2引数（argsテーブル）** で渡される（KEYS/ARGVではない）。citeturn14view0turn14view4turn29view0  
- `redis.register_function` は FUNCTION LOAD のコンテキスト内で関数を登録するAPIとして規定されている。citeturn30view0  

### Error/timeout handling

**エラー表現（最低限）**

- `redis.error_reply(x)` は Lua table `{ err = <string> }` を返し、Redisクライアントには error reply になる。citeturn30view0  
- `redis.status_reply(x)` は Lua table `{ ok = <string> }` を返し、Redisクライアントには status reply になる。citeturn30view0  
- エラー文字列は Redis慣習として先頭語をエラーコードにするのが望ましい（例: `ERR ...`）。citeturn30view0  

**実行時間（timeout）**

Redisは「最大実行時間はデフォルト5秒」「busy-reply-threshold でミリ秒精度設定可」「閾値超過でも自動停止しない（原子性破壊のため）」を明記する。citeturn12view0  
したがって互換寄りの実装では、少なくとも以下を分けるのが安全。

- **観測閾値（busy-reply-threshold相当）**: 超過時にログ／メトリクス。Redis同様、原則は強制停止しない。citeturn12view0  
- **強制停止（kill）**: read-only スクリプトに限り SCRIPT KILL / FUNCTION KILL で停止可能にする（write を1回でも行った場合は停止不能で、Redisは SHUTDOWN NOSAVE しか残らない、という思想）。citeturn12view0turn11view1  

**実装メモ（mlua前提）**

- mlua 自体は unsafe コード作成用に `ffi (mlua-sys)` を再エクスポートしているため、必要ならC API hook 等を低レベルで差し込める。citeturn31view0turn31view1  
- また mlua は memory limit 機構を提供し、Lua 5.1/JIT/Luau に対しても memory limit をサポートした旨が changelog にある。citeturn31view4turn31view2  

## Phased Delivery Plan

### Phase 0: disabled + plumbing

目的は「後戻り容易な配管」を先に作り、いきなり本番互換の深みに入らないこと。

- **キルスイッチ**: 現状の `ERR scripting is disabled in this server` を維持しつつ、設定で完全無効化を既定にする（起動時／動的切替の両方）。  
- **スクリプト識別とキャッシュ構造**: `sha1` 計算、キャッシュ（マップ＋統計）のみ実装してもよい。RedisもSHA1でキャッシュし、SCRIPT LOAD / EVALSHA がそれを前提とする。citeturn11view1turn11view2  
- **監査ログ／メトリクス**: 実行時間、キャッシュサイズ、eviction数など。Redis 7.4 は EVAL 由来スクリプトを LRU で削除し evicted_scripts を提供するため、互換観点でもメトリクスは重要。citeturn11view0  
- **セキュリティ境界の方針決定**: 「require無効」「外部モジュールロード無し」を最初に固定する。Redisは sandbox で require を無効化し、利用可能ライブラリを限定する。citeturn29view0turn28view0  

### Phase 1: EVAL minimal

ここでのゴールは「read-only で安全に動く」こと。write を混ぜると、kill/timeout時に原子性問題が一気に高くなるため、まず read-only を既定にするのが現実的。

- **まず EVAL_RO / EVALSHA_RO を “GA相当” 路線**に置く（EVAL/EVALSHAは opt-in の段階的開放）。Redisには read-only scripts の性質が定義され、EVAL_RO などが導入されている。citeturn12view0turn11view0  
- **KEYS/ARGV の注入と key-arg規約強制**:  
  - `KEYS` と `ARGV` を Lua グローバルとして提供。citeturn29view0turn11view0  
  - 「スクリプトが触るキーを入力キーで列挙し、生成キー名で触らない」制約を実装側でも検証（可能なら実行時に redis.call のキー引数が KEYS 内に含まれるかチェック）。citeturn11view0turn29view0  
- **redis.call / redis.pcall / error_reply / status_reply / sha1hex / log** を最低限提供する（Redis Lua APIに明記）。citeturn30view0  
- **sandbox**: imported modules を禁止（require無効）にし、利用可能ライブラリを Redisの範囲に寄せる（string/table/math、osは `os.clock()` のみ等）。citeturn29view0turn28view0turn12view0  

### Phase 2: EVALSHA + cache semantics

- **SCRIPT LOAD / SCRIPT EXISTS / SCRIPT FLUSH** を互換レベルに引き上げる。SCRIPT LOAD は「実行せず compile&load」を要求し、EVALSHA は「キャッシュにないと NOSCRIPT」。citeturn11view1turn11view2  
- **キャッシュの volatile 性**を明確化し、再起動・failover・flush で消える前提にする（Redis互換）。citeturn11view1  
- **キャッシュ肥大対策**: Redis 7.4 相当の “EVALでロードされたスクリプトの LRU 削除” をオプション実装する（互換要求が強い場合）。citeturn11view0  

### Phase 3: function API (FCALL*) parity

- **FUNCTION LOAD** でライブラリと関数をロードし、`redis.register_function` を解釈して登録する（Redisが規定）。citeturn30view0turn14view0  
- **FCALL / FCALL_RO** を実装し、keys/args を callback の第1/第2引数として渡す（KEYS/ARGVではない）。citeturn14view0turn14view4turn29view0  
- **フラグ（no-writes等）** と read-only コマンドの相互作用を実装する。Redisは no-writes フラグの意味と、FCALL_RO / EVAL_RO などの関係を詳細に記述している。citeturn30view0turn12view0  

### Phase 4: hardening/perf

- **replication/AOF の一貫性戦略を固定**:  
  - Redisは以前は “verbatim script replication” を使い、3.2.0で effects replication を導入し、7.0以降は effects replication のみと明記している。citeturn30view0  
  - Redis互換サーバとしては、長期的には **script effects replication（スクリプトが発行した write コマンド群を複製）** を基本にする方が、非決定性（TIME/乱数/実行環境差）に対して堅い。citeturn30view0turn14view4  
- **リソース制御の強化**: memory limit と命令数／時間監視を本番規定にする（mluaのmemory limitサポート、Redisの最大実行時間規定）。citeturn31view4turn12view0  
- **sandboxの侵害経路を監査**: CVE-2022-0543 のように “package/loadlib 経路が残るとRCE” になり得るため、動的リンク／モジュールロード経路の有無をビルド成果物で検証する。citeturn10search1turn10search14turn10search23  
- **互換性ギャップ可視化**: Redis Lua APIの “Runtime libraries”（cjson/cmsgpack/struct/bit 等）や “global変数禁止” 等、差分が出やすい部分の互換テストを増やす。citeturn28view0turn29view0  

## Risk Register

- **Lua sandbox escape（RCE）**  
  **リスク**: Lua環境から `package.loadlib` 等でホスト機能が呼べると致命的。Debian系Redisで実際に sandbox escape が発生している。citeturn10search1turn10search14turn10search23  
  **緩和策**: vendored 静的リンク、require無効、外部モジュールロード禁止、利用可能ライブラリの限定（Redisと同様）、ビルド成果物の検査（`package`/`io`/`os.execute` の露出確認）。citeturn15view0turn29view0turn28view0  

- **長時間実行／無限ループによるレイテンシ崩壊**  
  **リスク**: Redisは実行時間閾値（デフォルト5秒）を持つが、原子性のため自動停止しない。シャードが詰まれば同一シャード上の全リクエストが停止する。citeturn12view0  
  **緩和策**: 監視閾値（busy-reply-threshold相当）の導入、read-only に限る kill、運用デフォルトを read-only に寄せる、スクリプト実行を feature flag で段階解放。citeturn12view0turn30view0  

- **部分書き込み（atomicity破壊）の扱い**  
  **リスク**: write を行ったスクリプトを途中停止するとデータが半端になる。Redisはこれを避けるため自動停止しない設計。citeturn12view0turn11view1  
  **緩和策**: 初期フェーズは read-only を既定（EVAL_RO/FCALL_RO中心）、write scripts は明示opt-in、write実行後の kill を禁止（Redisと同様）。citeturn12view0turn30view0  

- **スクリプトキャッシュ肥大（メモリ枯渇）**  
  **リスク**: 動的生成スクリプト（値を本文に埋めてEVALする等）はキャッシュを増やし続ける。Redis 7.4 は LRU eviction を導入。citeturn11view0turn11view1  
  **緩和策**: キャッシュ上限・LRU、メトリクス、クライアント向けに「引数化して同一スクリプトを再利用」をガイド。citeturn11view0turn11view1  

- **レプリケーション／AOF の非決定性（データ不整合）**  
  **リスク**: TIME等の非決定的入力があると、スクリプト本文複製（verbatim）では複製先で差が出うる。Redisは script effects replication を重視し、7.0以降はそれのみ。citeturn30view0turn14view4  
  **緩和策**: 基本は effects replication に寄せ、必要なら `redis.set_repl` 相当の制御も検討（Redis仕様）。citeturn30view0  

- **クラスタ／シャード整合性の破壊（キーアクセス規約違反）**  
  **リスク**: スクリプトが入力にないキー名へアクセスすると、クラスタ／シャードで正しくルーティングできず破綻する。Redisは明示的に禁止している。citeturn11view0turn14view0turn29view0  
  **緩和策**: `redis.call` 経由のキー引数を実行時検証し、許可キー集合（KEYSやkeys table）外アクセスを拒否する（少なくともデフォルトON）。citeturn11view0turn29view0  

- **crate保守停止による将来リスク**  
  **リスク**: rlua のようにアーカイブされると脆弱性対応・将来互換が困難。citeturn16view0turn17view0  
  **緩和策**: mlua を第一候補に固定し、バージョン固定＋定期アップデートを運用に組み込む。citeturn6search17turn15view0  

- **“pure Rust Lua” の成熟度不足**  
  **リスク**: piccolo は sandbox 耐性を重視する一方、実験的でAPI破壊が頻繁と明記。citeturn19view2  
  **緩和策**: 互換要件が強い本番初期では採用せず、将来の隔離・安全実行研究として分離評価する。citeturn19view2  

## References

- Redis: EVAL（Lua 5.1、KEYS、ARGV、キー列挙規約、Redis 7.4 の LRU eviction）citeturn11view0  
- Redis: Eval scripts入門（script cache、SHA1、SCRIPT LOAD、EVALSHA、cache volatility、NOSCRIPT）citeturn11view1  
- Redis: EVALSHA（SCRIPT LOADキャッシュ前提、EVAL同等）citeturn11view2  
- Redis: Redis programmability（Lua 5.1、原子実行／ブロッキング、read-only scripts、最大実行時間と busy-reply-threshold、killの原則）citeturn12view0  
- Redis: Lua API reference（sandbox：グローバル禁止／require無効、redis.call/pcall/error_reply/status_reply、replicationモードと replicate_commands/set_repl、script flags、Runtime libraries と os.clock 限定等）citeturn29view0turn30view0turn28view0  
- Redis: FCALL（keys/args を callback の第1/第2引数テーブルで渡す、キー列挙規約）citeturn14view0  
- Redis: Functions intro（keys/args の区別、TIME使用例など）citeturn14view4  
- mlua: GitHub README（サポートLua版、feature flags、vendored、send、テスト対象OS）citeturn15view0  
- mlua: docs.rs Cargo.toml（バージョン、ライセンスMIT、features）citeturn24search7  
- mlua: docs.rs Lua struct（安全モード `Lua::new` と `unsafe_new` の挙動、Cモジュール許可の差）citeturn7view0  
- mlua: docs.rs CHANGELOG（ffi再エクスポート、memory limitサポート等）citeturn31view0turn31view4turn31view2  
- rlua: GitHub（アーカイブ済み、mluaへの移行ラッパで非推奨）citeturn16view0turn17view0  
- piccolo: GitHub（pure Rust Lua VM、sandbox/DoS耐性目標、experimental、API破壊が頻繁）citeturn19view2turn19view1  
- Luau: LICENSE（MIT）citeturn2search2  
- CVE-2022-0543（Redis Lua sandbox escape：NVD、Debian tracker、Ubuntu security）citeturn10search1turn10search14turn10search23  
- Wasmtime: ResourceLimiter、consume_fuel（公式ドキュメント）citeturn20search6turn21search0  
- Rhai: ライセンス（Apache-2.0 または MIT）citeturn20search11