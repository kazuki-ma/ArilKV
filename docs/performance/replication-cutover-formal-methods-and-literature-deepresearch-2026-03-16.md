# Replication Snapshot Cutover の形式手法とSOTA文献調査

## Executive Summary

- Redis/Valkey 系の full sync は「RDB スナップショットを送る→その後にスナップショット生成中に溜めた書き込み（コマンド列）を送る」という基本形であり、PSYNC は「replication ID + replication offset」によって欠落分だけを再送する（バックログに残っている範囲なら partial、残っていなければ full）。citeturn1view0turn2view0  
- 「スナップショット／ライブの cutover（境界）」の正しさは、(a) 1つの論理境界（cutover offset / cutover epoch）を定義し、(b) スナップショットがその境界“まで”の状態であり、(c) 境界“以降”の更新が suffix（コマンド列）として順序通り・重複なく適用される、という **ログ前置（prefix）等価**に還元できる。citeturn1view0turn16view0  
- dual-channel（RDB と backlog を並行転送）を採用する場合、鍵は「**RDB が表す終端 offset（snapshot-end offset）を先に通知**し、その offset+1 からオンラインストリームを開始し、受信側は RDB ロード完了まで増分をローカルにバッファする」という設計にある。これは cutover を explicit に“固定”する実装パターンとして極めて示唆的。citeturn4view0turn7view0  
- ストリーミング処理の checkpoint/barrier は「barrier が流れの中で順序を保ち、barrier を境に“このスナップショットに含めるレコード”と“次に属するレコード”を分割する」という cutover の形式化を提供する。特に **整列（alignment）しないと重複が出る**という注意点は、レプリケーション境界の不整合が “重複” として顕在化することを明確にする。citeturn22view0  
- CDC 系の最新実装は「スナップショット（全量）とログ（増分）を並行させ、衝突（同一PKなど）をバッファ＋優先規則で解決し、透過的に重複排除する」方向に進化している。これは「スナップショット＋suffix の gap/duplicate を無くす」ための実装・モデル両面の先行事例になる。citeturn9view0turn17view0turn18search0  
- 分散ログ（Raft）や WAL ベースのオンラインバックアップ（PostgreSQL）は、スナップショットに「このスナップショットが置換するログ位置（last included index/term, あるいは WAL の連続列条件）」を付与し、以降をログ再生するという cutover の“教科書的”定式化を持つ。citeturn16view0turn23view0  
- 単一の外部可視な Redis インスタンスとして export するなら、「**グローバルな論理 cutover**」は必須であり、各 shard が wall-clock 的に異なる時刻に materialize しても、同一の論理 cutover に“ピン止め”された read view（MVCC 的スナップショット等）を提供できるなら整合を保てる。citeturn22view0turn13search12turn13search5  
- 最小の TLA+ は「抽象：単一ログ＋状態機械」「具体：snapshot 生成・転送・バッファ・再接続（PSYNC/バックログ枯渇）・cross-shard コマンドのコミット境界」を分離し、具体が抽象に refinement することを狙うのが最短距離になる。TLA+ 既存例として Snapshot KV / チェックポイント協調 / 取引コミットモデルが直接参考になる。citeturn15view0  

## Prior Art Map

| mechanism / system / paper | なぜこの問題に効くか（cutover の観点） | source classification |
|---|---|---|
| Redis replication (full sync + buffering) | full sync 時に master が RDB を生成しつつ新規 write をバッファし、RDB 転送後にそのバッファを送る、という snapshot→suffix の基本形を明示している。citeturn1view0 | official product doc |
| Redis replication IDs / offsets / PSYNC | replication ID と replication offset により partial resync を可能にし、バックログ不足等で full sync にフォールバックする“再接続・枯渇”の条件分岐を規定する。citeturn1view0turn25view0 | official product doc |
| antirez “PSYNC” (2013) | Redis の partial resync（PSYNC）導入の動機と、backlog から offset を基準に送るという設計直観を与える（仕様理解の補助）。citeturn0search20 | vendor engineering content |
| Valkey dual-channel replication PR (#60) | 「RDB 用の別接続 + main channel で増分を並行送信」「snapshot end repl-offset を先に通知」「受信側はローカルに増分を貯めて RDB ロード後に流し込む」という cutover を explicit に固定する設計が書かれている。citeturn4view0 | official source code |
| Redis “Second Channel for RDB” issue (#11678) | dual-channel の設計意図（COB 過大化・同期遅延・stale 読みの窓）と、end-offset 先出し＋PSYNC offset+1 などの具体設計が整理されている。citeturn7view0 | official source code |
| Apache Flink checkpoints/barriers | barrier が “境界” を表し、barrier 前後でレコード集合を分割し、複数入力では整列が必要（整列しないと重複が発生し得る）という cutover の形式化がある。citeturn22view0 | official product doc |
| Carbone et al. “Lightweight Asynchronous Snapshots … (ABS)” | Chandy–Lamport 系をデータフロー向けに再設計し、非停止で一貫スナップショットを取るという現代的スナップショット理論（SOTA の checkpoint 形式化）。citeturn26view0 | research / paper |
| Apache Flink CDC MySQL connector (Offset Signal Algorithm) | chunk ごとに LOW/HIGH のログ位置（binlog offset）を取り、ログを再読して upsert し、スナップショットとログの衝突を解消する具体アルゴリズム。cutover を“水位”で固定する。citeturn17view0 | official product doc |
| DBLog: watermark-based CDC | watermark による「テーブル全量 select（chunk）とログを interleave」「dedup でログ優先」を主張し、Netflix の production 利用も述べる。スナップショット＋suffix の“近代 CDC 版”。citeturn18search0turn18search4 | research / paper |
| Debezium MySQL connector (initial snapshot + binlog) | 初期の consistent snapshot を取り、その時点から binlog を読むと明示（スナップショット地点とログ位置の結び付け）。また障害時に重複が発生し得る現実も明記。citeturn8view0 | official product doc |
| Debezium incremental snapshots (watermarks + snapshot window + de-dup) | スナップショットとストリーミングの競合（同一PK等）をバッファと優先規則で解決し、論理順序を保つ“snapshot window”を提示。citeturn9view0 | official product doc |
| Raft log compaction / InstallSnapshot | スナップショットに last included index/term を持たせ、以降のログ整合性確認（AppendEntries）と接続し、遅れた follower には snapshot を送って復旧する。snapshot+suffix の定式化が明快。citeturn16view0 | research / paper |
| etcd raft implementation guidance | “Entries→HardState→Snapshot を順序通り永続化”“メッセージ送信は HardState 永続化後”など、実装レベルで cutover/順序制約を明確化（バグパターンを直接示唆）。citeturn19view0 | official product doc |
| PostgreSQL PITR / continuous archiving (base backup + WAL) | 「ベースバックアップ開始時刻まで遡れる連続 WAL 列が必要」「ベースに WAL を再生して整合にする」という snapshot+log の古典だが強力な安全条件。citeturn23view0 | official product doc |
| CloudNativePG: pg_basebackup “second connection WAL streaming” | ベースバックアップ中に生成されたトランザクションを “第二接続で WAL としてストリーム”する、と明示。dual-channel の強い類推先。citeturn21view0 | vendor engineering content |
| Spanner (globally-consistent reads at timestamp) | “timestamp による DB 全体の一貫スナップショット読み”は、「shard が別時刻に materialize しても同一論理時刻にピン止めできるなら整合する」という設計判断の理論的裏付けになる。citeturn13search12turn13search0 | research / paper / official product doc |
| CockroachDB blog (txn timestamp snapshot includes all/none of writes) | “timestamp は DB 全体のスナップショットを識別し、トランザクションの書き込みは all-or-nothing”という直観は cross-shard の indivisibility の説明に使える。citeturn13search5 | vendor engineering content |
| Isabelle AFP: formal proof of Chandy–Lamport (2026) | 「consistent cut」「marker」「（計算と並行に）グローバル状態を記録しても意味のある snapshot になる」ことを形式的に示す最新の証明資産。cutover の形式化パターン（不変条件の立て方）として有用。citeturn14view0 | research / paper |
| TLA+ Examples repository | Snapshot KV / Checkpoint Coordination / Transaction Commit Models など、今回のモデル構成に“ほぼそのまま使える”部品が揃う。citeturn15view0 | official source code |

## Recommended Correctness Model

この問題を最小の形で「形式化→実装へ写像」するには、**“単一の外部ログ（論理コマンド列）と、そのログ上の cutover 位置”**を中心に据えるのが最も安定します。Redis の full sync も、PSYNC も、結局は「どの offset までが snapshot に含まれていて、どこからが suffix（ライブコマンド列）なのか」を決めるだけで正しさが語れます。citeturn1view0turn25view0  

### 最小概念モデル

- **外部可視ログ `L`**: クライアントが観測可能な“論理コマンド”の全順序列（実際の Redis は replication stream の byte offset を使うが、モデルではまず *コマンド単位* を 1 ステップとみなすのがよい）。citeturn1view0turn16view0  
- **cutover `c`**: 単調増加する論理位置（例：`c = |AppliedLog|`）。dual-channel では “snapshot-end offset” として explicit に送る設計がこの `c` の具体化になる。citeturn4view0turn7view0  
- **snapshot `S(c)`**: `L[1..c]` を順に適用した結果の状態（state machine の状態）。Raft でいう last included index に相当し、PostgreSQL PITR の「ベースバックアップ（開始）＋連続 WAL」にも同型がある。citeturn16view0turn23view0  
- **suffix `L[c+1..]`**: snapshot 後に適用すべきログ尾部。full sync の buffering（Redis）や、受信側 local buffer（dual-channel）、CDC の snapshot window / de-dup は、いずれも “`L[c+1..]` を欠損なく、重複なく、順序通りに適用する”ための具現化です。citeturn1view0turn4view0turn9view0  

### cross-shard（owner-thread / actor）を一つの外部ログに畳む条件

「サーバ内部が shard（owner-thread）に分割されているが、外部へは単一 Redis インスタンスとして export する」場合、最も重要なのは:

- **外部ログ `L` の“コミット点”が、cross-shard の効果を原子的に代表すること**（＝そのコマンドが snapshot 側に入るなら全 shard で入る／suffix 側なら全 shard で suffix、と言える必要がある）。  
- これはストリーミングの barrier と同じ構造で、境界をまたいで一部 shard だけが前後に分裂すると “整列しない checkpoint”のように重複・不整合が発生します。citeturn22view0  
- 実装上は「(a) グローバルな論理シーケンス番号（または commit timestamp）を割り当てる」「(b) cross-shard コマンドはその番号で atomically commit される（もしくは commit 前の中間状態が外部に露出しない）」「(c) snapshot は `c` 以前に commit 済みのものだけを含む read view で生成する」という要件になります（MVCC 的 read view があると wall-clock のズレに耐性が出る）。citeturn13search12turn13search5  

## Invariants

ここでは「状態機械の正しさ（最終状態が同じ）」を中心に、安全（Safety）とライブネス（Liveness）を明示します。Redis/Valkey の PSYNC は backlog の“残存範囲”に依存して partial/full を切り替えるので、切替条件も含めます。citeturn1view0turn25view0  

### Safety invariants

- **No gap（欠落なし）**  
  cutover `c` を固定した full sync では、レプリカが最終的に到達するログ適用位置 `r` が `r ≥ leader_last` になる（少なくとも leader が公開した commit の範囲を欠落なく反映する）。  
  具体には、送信側が `c` を通知した時点から `L[c+1..]` が必ず送られ、受信側が必ず適用する、を不変条件として置く。citeturn1view0turn4view0  

- **No duplicate（重複なし）**  
  レプリカが各ログエントリ（論理コマンド）を高々 1 回だけ“状態に影響する形で”適用する。  
  実装上の現実として、クラッシュ／再起動／オフセット記録遅延で「同じイベントが再生成され得る」ことは CDC 文脈でも明示されているため、模型では “重複入力は起こり得るが、状態への反映は idempotent に抑制される”という形（dedup key：offset など）に落とすのが堅い。citeturn8view0turn22view0  

- **Snapshot-then-suffix ordering（境界順序）**  
  レプリカ状態は必ずある `c` に対して `S(c)` を基底にし、その後に `L[c+1]`, `L[c+2]`, … を順に適用したものになる。  
  “RDB ロード前に増分が到着する”設計（dual-channel / CDC）でも、到着順ではなく **適用順がこの順序**になることが本質。citeturn4view0turn7view0turn17view0  

- **Atomic command visibility（単一コマンドの不可分性）**  
  任意の論理コマンド `cmd` について、`cmd` の効果が snapshot と suffix に分裂しない。  
  形式的には「`cmd` が `≤ c` なら `cmd` の全効果が `S(c)` に含まれ、`> c` なら `S(c)` には含まれず suffix で全効果が入る」。Flink の barrier が “barrier 前後で集合を分割する”のと同型。citeturn22view0  

- **Cross-shard command indivisibility（cross-shard の不可分性）**  
  cross-shard コマンドは「全 shard へ反映されるか、どの shard にも反映されないか」の原子性を持つ（少なくとも外部観測に対して）。  
  これは “グローバル cutover `c`” の意味付け（何が `≤ c` か）自体を成立させる前提。MVCC/ timestamp snapshot の all-or-nothing 直観がここを支える。citeturn13search5turn13search12  

- **Backlog/partial-resync soundness（再接続の健全性）**  
  partial resync（PSYNC）を許可する時は、送信側の backlog が要求された offset 以降を連続に保持している（`repl_backlog_first_byte_offset ≤ requested_offset ≤ master_repl_offset` のような範囲制約）こと。保持できないなら full sync にフォールバックし、フォールバック時は新しい `c` を再確立する。citeturn1view0turn25view0  

- **Stale cutover metadata is never reused（古い cutover メタデータの再利用禁止）**  
  `c` は “この full sync セッションに結び付いた一意の境界”であり、接続や replid が変わった後に古い `c` を再利用しない。Redis が replication ID（履歴）を区別する理由付けと整合する。citeturn1view0turn25view0  

### Liveness properties

- **Eventual catch-up（最終追いつき）**  
  ネットワークが十分長く安定し、送信側が `L` の生成（書き込み）を有限に止めたなら、レプリカは最終的に送信側と同一状態に収束する（full sync でも partial sync でも）。citeturn1view0turn16view0  

- **Full sync completes（full sync 完了性）**  
  full sync 開始後、(a) RDB 送信が完了し、(b) レプリカが RDB をロードし、(c) ローカルにバッファした増分（あるいは master 側にバッファされた増分）を全て適用しきる、という完了状態へ必ず到達する（クラッシュが起きない・リソースが枯渇しない等の弱い公平性仮定の下）。dual-channel の状態機械はこれを明確に分割している。citeturn4view0turn7view0  

- **Reconnection makes progress unless backlog exhausted（再接続の進捗）**  
  再接続時、backlog が残っているなら partial resync により前進する。backlog が尽きているなら full sync により前進する（ただし full sync が成功する条件＝RDB 転送・ロード・バッファ適用が満たされる）。citeturn1view0turn25view0  

## Shard/Epoch Judgment

### 「単一グローバル cutover は必要だが、shard は同一論理 cutover にピン止めできれば wall-clock 的に別時刻に materialize してよい」は正しいか

結論から言うと、この設計文は **条件付きで正しい**です。

- **正しい条件**は、「各 shard が `c` に対応する **一貫 read view**（例：MVCC のスナップショット、もしくは “`c` 以降の更新を遮断/バッファして `c` の状態を読み出せる仕組み”）を提供できる」場合です。そうであれば、wall-clock で materialize がずれても、論理的には同一 `S(c)` を構成できます。これは timestamp による DB 全体の一貫スナップショット読み（Spanner 的）や、timestamp が “DB 全体のスナップショットを識別し、トランザクションの書き込みが all-or-nothing” になるという MVCC の直観と整合します。citeturn13search0turn13search12turn13search5  

- **条件が満たせない場合は不正**です。特に「snapshot 生成中も各 shard が通常通り書き込みを反映し続け、過去点の read view を持てない」なら、ある shard は `c` より後の効果を含み、別 shard は含まない、という **不整合 cut**になり得ます。これはストリーミングで barrier 整列をしないと “次の窓のレコード”が混ざって重複・不整合が出る、という Flink の説明と同型です。citeturn22view0  

### 独立 per-shard epoch が安全な条件

「per-shard で独立に epoch / cutover を持つ」ことが安全になるのは、概ね次のどちらかです。

- **外部的にも shard が独立パーティションとして扱われる**場合  
  例えば外部 API／レプリケーション設計が「shard ごとに独立した複数ストリーム」として公開され、クライアントもそれを前提に処理するなら、epoch の独立性は“仕様”になります（Kafka パーティションのような発想）。ただし今回の要件は「単一論理 Redis インスタンスとして export」なので、これは別のプロダクト形態です。citeturn22view0turn15view0  

- **言語仕様として cross-shard コマンドが存在せず、どのコマンドも必ず単一 shard に閉じる**場合  
  もしコマンド言語/ルーティングが「複数キー操作は同一 shard に強制される」等の制約を持ち、内部の“跨り”が起こらないなら、見かけ上は shard ごとの cutover でも矛盾が起きにくいです。  
  ただし、提示された課題文には「cross-shard internal execution が必要」とあるため、この前提は満たされない想定が自然です（満たされないなら unsafe）。citeturn22view0turn13search5  

結論として、**単一外部ストリームを維持しつつ“epoch を shard ごとに別々にする”のは、一般には unsafe**です。安全にしたいなら、結局は shard を貫く **共通の論理 cutover（グローバル `c`）**へ回帰します。

## TLA+ Modeling Blueprint

ここでは「最小だがバグを捕まえる」モデル形を、(a) まず抽象仕様、(b) それを満たす実装仕様（snapshot/handshake/バッファ/再接続）に分けて提案します。TLA+ 例題資産として、Snapshot KV・Checkpoint Coordination・Transaction Commit Models は構造的に近い部品を提供します。citeturn15view0  

### variables

最小構成（モデルサイズを抑えつつ、指定バグパターンを捕捉することを優先）：

- `Shard`：有限集合（例 `{s1,s2}`）  
- `KV[shard]`：各 shard のキー空間（抽象化して小さな集合）  
- `Store[shard]`：各 shard の状態（キー→値）。値は小さなドメインでよい  
- `Cmd`：論理コマンド。属性として `touchedShards ⊆ Shard` と “効果関数”を持たせる  
- `CommittedLog`：コミット済みコマンド列（シーケンス）。**外部可視ログ `L`**に対応  
- `AppliedIdx[shard]`：各 shard がコミット済みログのうちどこまで反映したか（internal 実行の遅れを表現）  
- `InFlightCmds`：cross-shard コマンドの“部分適用”を表すための構造（例：`phase[cmd] ∈ {proposed, partApplied, committed}` と、どの shard が適用済みかの集合）  
- `Cutover`：full sync セッションの cutover `c`（整数）  
- `SnapView[shard]`：snapshot が参照すべき read view（モデルでは `SnapStore[shard]` として具象化してもよい）  
- `ReplicaStore`：受信側の状態（単一 Store としてもよいし shard 分割してもよい）  
- `ReplicaApplied`：受信側が適用済みのログ位置  
- `Backlog`：送信側が保持する suffix（モデルでは“ログ列の部分列”で表現）  
- `Session`：再接続を表すセッションID、または `ReplId` 的な履歴ID（Redis の replication ID の抽象）citeturn1view0turn25view0  

### actions

抽象仕様（Reference spec）側は「ログ生成と状態機械の意味」だけに絞る：

- `ClientIssue(cmd)`：`CommittedLog' = Append(CommittedLog, cmd)`（ただし cross-shard は 2段階にしたい）  
- `Commit(cmd)`：`phase` を `committed` にし、`CommittedLog` に載せる（cross-shard の commit 点）  
- `ApplyToShard(shard, i)`：`AppliedIdx[shard]` を進めて `Store[shard]` を更新（遅れ・順不同を許すことで “部分適用”を表現）

実装仕様（Implementation spec）側は、full sync と partial sync の状態遷移を入れる：

- `StartFullSync`：`Cutover := Len(CommittedLog)` を固定し、`SnapView := Store at Cutover` を確立（MVCC が無いモデルなら “Cutover 以降を buffer する”ガードを入れる）citeturn1view0turn4view0  
- `SendSnapshot`：`SnapView` を送信（チャンク化は省略可）  
- `SendSuffixEntry(i)`：`i > Cutover` のログエントリを送る（dual-channel なら `SendSnapshot` と並行に走る）citeturn4view0turn7view0  
- `ReplicaBufferSuffix(i)`：RDB ロード前は suffix を buffer  
- `ReplicaInstallSnapshot`：`ReplicaStore := SnapView`、`ReplicaApplied := Cutover`  
- `ReplicaDrainBuffer`：buffer した suffix を順に適用し `ReplicaApplied` を進める  
- `PSYNCRequest(offset, sessionId)`：partial を試み、可能なら `SendSuffixEntry(offset+1..)`, 不可能なら `StartFullSync` へ。citeturn1view0turn25view0  
- `BacklogEvict`：バックログが循環バッファ等で古い部分を捨てる（partial 不可→full を誘発する nondet）citeturn25view0  

### properties

Safety（TLC でチェックしやすい形）を中心に置くのが最短です。

- `TypeOK`（必須）：各変数の型制約  
- `Refinement`：Implementation のふるまいを “抽象ログ適用の結果”へ写す `View`（例：`ExternalStore = Merge(Store[shard])`、`ReplicaStore` 等）を定義し、抽象 spec の `Next` と整合すること  
- `NoGap`：`ReplicaApplied` が進むとき、適用したコマンド集合が `CommittedLog[1..ReplicaApplied]` と一致  
- `NoDup`：レプリカが同一 index を 2 回以上“状態に反映”しない（適用済み集合 `AppliedSet` を置くと書きやすい）  
- `AtomicCutover`：`Cutover` を跨いで単一 `cmd` の効果が split しない（cross-shard も含める）  
- `CrossShardAtomic`：cross-shard `cmd` は committed 以前に外部可視状態へ露出しない（or committed を境に全 shard 一斉に見える）  

Liveness は最初は弱く（フェアネスつき）：

- `WF_vars(ReplicaDrainBuffer)` / `WF_vars(SendSuffixEntry)` などを付けて「止まらない限り追いつく」をチェック  
- “backlog 枯渇→必ず full sync” のような進捗は liveness として置けるciteturn1view0turn25view0  

### likely counterexamples

指定されたバグパターンに即して、まず最小モデルで出やすい反例を挙げます（TLC で最初に刺さりやすい順）。

- **snapshot built before backlog capture starts**  
  `Cutover` を確定する前に snapshot を materialize してしまい、`Cutover` 以降の一部更新が snapshot に混入する／あるいは suffix に欠落する。Redis の full sync は “BGSAVE と同時に write をバッファする”点がこのバグを避ける設計になっている。citeturn1view0  

- **backlog replay begins from wrong offset**  
  `PSYNC offset+1` の off-by-one、あるいは `Cutover` を誤って “次の位置”にしてしまい gap/dup を起こす。dual-channel の設計文は “snapshot end offset を送って offset+1 を要求”と明記しており、モデル上もこの規則を invariant 化できる。citeturn7view0turn4view0  

- **cross-shard command split between snapshot and suffix**  
  shard A は `cmd` を適用済みだが shard B は未適用、の中間状態で snapshot を取ってしまう（または buffer の drain が shard ごとに進んでしまう）。これを捕まえるには `InFlightCmds` と `AppliedIdx[shard]` を入れた“部分適用”表現が必要。citeturn22view0turn13search5  

- **duplicate replay after reconnect**  
  再接続で offset 記録が遅れた／セッション識別が変わったのに古い offset を使った等で同じエントリを再適用してしまう。CDC でも “クラッシュ後に重複イベントが出得る”ことが明記され、dedup 情報（binlog pos 等）で識別できるとしている。citeturn8view0turn9view0  

- **live switch occurs before backlog drain completes**  
  “RDB をロードし終えた”だけでクライアント向け read を許し、ローカル buffer（COB）が未適用で stale window が長く残る（Redis issue が stale data を問題として挙げる）。これを safety で捉えるなら「公開可能条件（servable）＝ buffer empty ∧ ReplicaApplied = leader position」といった補助変数が効く。citeturn7view0  

- **stale cutover metadata reused**  
  old replication ID / old Cutover を別セッションで使い回し、別履歴のログを繋いでしまう。Redis が replication ID を導入し、ペア（ID, offset）が一意の履歴を指すようにしている点をモデルで反映する。citeturn1view0turn25view0  

## Test Strategy Complement

TLA+ で “起こり得る反例” を先に潰しても、実装ではバイト列・チャンク・非同期 I/O・再起動・部分書き込みなどが絡むので、次の補完が必要です。なお、Flink/CDC/DBLog と同様に「境界メタデータ（offset/watermark）と、適用済み位置を永続・再利用する」設計を取るなら、その永続化の正しさが最大のテスト焦点になります。citeturn17view0turn18search0turn19view0  

### exact integration tests（仕様テスト）

- full sync: upstream が RDB を送っている最中に多数の write を発生させ、最終的に（1）欠落なし（2）重複なし（3）順序崩れなし、を確認（差分比較は「ログ適用による期待状態」との一致で取る）citeturn1view0  
- dual-channel 相当: RDB 転送と同時に suffix を大量に流し、受信側がローカル buffer を正しく drain してから “読み取り可能” になることを確認（stale 窓が仕様通りか）citeturn4view0turn7view0  
- PSYNC/partial: backlog が残るケースと、backlog が枯渇して full に落ちるケースを両方作り、`INFO replication` に相当するメタデータ（first_byte_offset 等）と一致する切替を確認citeturn25view0  

### randomized schedule tests（探索的・並行性テスト）

- owner-thread/actor のスケジューラを擬似乱数で攪拌（クロスシャードコマンド、スナップショット生成、バックログ送信、RDB ロード、buffer drain をランダム順で interleave）し、TLA+ が想定する不変条件を“オンラインアサート”する  
- Flink の “alignment しないと重複” の説明に対応して、境界（cutover）関連のハンドシェイク／順序制約が破れたときに必ず検出できるアサートを入れるciteturn22view0  

### crash/reconnect simulation（永続化バグの捕捉）

- 受信側が (a) RDB 受信中、(b) RDB ロード中、(c) buffer drain 中、(d) steady-state 中、の各局面でクラッシュ→再起動し、重複・欠落が無いことを検証  
- 送信側が backlog を循環で捨てる（サイズ制限）状況を作り、PSYNC が full に落ちる境界条件を繰り返しテストciteturn25view0turn1view0  
- “offset の永続化が遅れると重複が出る”という CDC の現実を踏まえ、offset/epoch を「いつ・どこで・原子的に」永続化するかを重点的に故障注入するciteturn8view0turn19view0  

### differential interop tests（互換・差分テスト）

- 可能なら実 Redis/Valkey を oracle にして、同一コマンド列（含：切断/再接続/フル同期）を与えたときに、最終データセット・レプリケーションメタ情報の整合が取れるかを差分比較  
- 互換性は “プロトコルの応答” だけでなく、“cutover の意味”が一致しているか（例：snapshot-end offset の定義、PSYNC の off-by-one）をログで突き合わせるciteturn1view0turn4view0turn7view0  

## References

- Redis replication docs — `official product doc` — `https://redis.io/docs/latest/operate/oss_and_stack/management/replication/` citeturn1view0  
- Redis INFO (replication fields) — `official product doc` — `https://redis.io/docs/latest/commands/info/` citeturn25view0  
- Redis PSYNC command — `official product doc` — `https://redis.io/docs/latest/commands/psync/` citeturn6view0  
- antirez blog “PSYNC” — `vendor engineering content` — `https://antirez.com/news/47` citeturn0search20  
- Valkey 8.0 blog (dual-channel replication mention) — `official product doc` — `https://valkey.io/blog/valkey-8-0-0-rc1/` citeturn3view0  
- Valkey PR #60 “Dual channel replication” — `official source code` — `https://github.com/valkey-io/valkey/pull/60` citeturn4view0  
- Redis issue #11678 “Second Channel For RDB” — `official source code` — `https://github.com/redis/redis/issues/11678` citeturn7view0  
- Apache Flink docs (barriers/checkpointing) — `official product doc` — `https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/` citeturn22view0  
- Carbone et al. “Lightweight Asynchronous Snapshots for Distributed Dataflows” (arXiv) — `research / paper` — `https://arxiv.org/abs/1506.08603` citeturn26view0  
- Apache Flink CDC MySQL connector docs — `official product doc` — `https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mysql-cdc/` citeturn17view0  
- Andreakis & Papapanagiotou “DBLog: A Watermark Based Change-Data-Capture Framework” — `research / paper` — `https://arxiv.org/abs/2010.12597` citeturn18search0  
- Debezium MySQL connector docs — `official product doc` — `https://debezium.io/documentation/reference/stable/connectors/mysql.html` citeturn8view0turn9view0  
- Raft paper (extended) — `research / paper` — `https://raft.github.io/raft.pdf` citeturn16view0  
- etcd raft package docs (persistence ordering guidance) — `official product doc` — `https://pkg.go.dev/go.etcd.io/etcd/raft/v3` citeturn19view0  
- PostgreSQL docs: Continuous Archiving / PITR — `official product doc` — `https://www.postgresql.org/docs/current/continuous-archiving.html` citeturn23view0  
- CloudNativePG bootstrap doc (pg_basebackup WAL streaming via second connection) — `vendor engineering content` — `https://cloudnative-pg.io/documentation/1.18/bootstrap/` citeturn21view0  
- Spanner paper (OSDI’12) — `research / paper` — `https://www.usenix.org/system/files/conference/osdi12/osdi12-final-16.pdf` citeturn13search12  
- Google Cloud Spanner TrueTime & external consistency doc — `official product doc` — `https://docs.cloud.google.com/spanner/docs/true-time-external-consistency` citeturn13search0  
- Cockroach Labs blog (timestamp snapshot is transactionally-consistent) — `vendor engineering content` — `https://www.cockroachlabs.com/blog/follower-reads-stale-data/` citeturn13search5  
- Isabelle AFP “A formal proof of the Chandy–Lamport distributed snapshot algorithm” — `research / paper` — `https://www.isa-afp.org/browser_info/current/AFP/Chandy_Lamport/outline.pdf` citeturn14view0  
- TLA+ Examples repo (Snapshot KV / Checkpoint Coordination / Transaction Commit Models など) — `official source code` — `https://github.com/tlaplus/Examples` citeturn15view0