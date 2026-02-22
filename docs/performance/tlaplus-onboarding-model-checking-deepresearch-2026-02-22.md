# TLA+ オンボーディングとモデル検査導入計画

## Executive summary

- 最初の狙いは「Redis 互換サーバの**owner-thread 直列化**と、**blocking コマンド（BLPOP 系）の wakeup/timeout**、**replication 適用順序**を、実装より先に“壊れない形”で固定すること。特に BLPOP は「同一キーの待ち行列 FIFO」「複数キー待ち時は引数のキー順で選択」「timeout で nil 返却」などの順序仕様が回 regress しやすい。citeturn3view0turn2view0  
- まずは **TLC（TLA+ の explicit-state model checker）**で、有限の小モデルを“週次で回せる”運用に落とす。TLA+ Toolbox は学習・デバッグに向き、CI は CLI 実行が再現性に強い。citeturn14search6turn1search20turn10view0  
- 初期スコープは「TCP/RESP 解析」などを捨て、**(1) クライアント要求 → owner へのルーティング → owner キューでの実行**と、**(2) blocking の登録・解除**、**(3) replication log の順序**だけに絞る。Redis の BLPOP は内部で“write により進捗可能になった blocked client をキューに入れ、idle 前に再チェックして提供”という形で実装される旨が文書化されており、ここを抽象モデル化するとバグ検出力が高い。citeturn3view0  
- ツール分担の推奨は「**Toolbox + TLC（主力）**」「**CLI TLC（CI/自動化）**」「**Apalache（状態爆発時の補助：bounded/inductive）**」。Apalache は SMT ベースで bounded model checking と inductive invariant に強い一方、TLC と役割が違う（liveness は変換で扱う等）。citeturn13search13turn13search5turn0search3turn13search21  
- 最初に書くべき性質は「安全性（invariant）中心」。例：**key→owner の一貫性**、**同一キーの実行が owner 内で直列化**、**BLPOP の FIFO/wakeup/timeout 規則**、**replication の“適用済み状態＝log の prefix を順に適用した結果”**。citeturn3view0turn2view0  
- Liveness は“入れ方”が重要。まず safety で回し、必要になった箇所だけ WF/SF（Weak/Strong fairness）を明示する。fairness は「ある action が十分頻繁に enabled なら起きるべき」を表す追加制約で、強すぎる仮定は「仕様は通るが実装に効かない」を生む。citeturn1search17turn1search1  
- 状態爆発は最初から前提にする。TLC 実務ではモデルサイズを小さく（コミュニティでは 3〜5 程度を推奨する助言もある）、対称性（symmetry）や model values を使って落とす。citeturn0search18turn0search6turn12view3  
- 反例から Rust テストへの橋渡しは「**TLC の error trace を JSON で吐く → その interleaving を“owner キューの step”として再生**」が王道。TLC は `-dumpTrace json` をサポートしており、自動変換パイプラインを組める。citeturn10view0turn13search23  
- タイムアウト再現はテストの鬼門なので、（実装が Tokio 前提なら）**current_thread + start_paused + advance**で“時間”を決定化し、blocking timeout の反例を安定再現させる。citeturn14search0turn14search26turn14search34  
- 週次運用の成功条件は「モデル変更が PR に追随できる小ささ」と「反例→テスト→修正の一往復が短いこと」。TLC/Toolbox は“push-button”で回せる設計を前提にする。citeturn14search6turn10view0  

## TLA+ quickstart for engineers

### ツール選定の実務的な結論

主力は TLC（Toolbox/CLI）で始めるのが最短です。TLA+ Toolbox は IDE として仕様編集、PlusCal 変換、モデル検査の実行とエラートレース閲覧を統合しており、オンボーディングが速いです。citeturn14search6turn14search21  
CI/自動化では CLI TLC を推奨します。`tla2tools.jar` には TLC/PlusCal translator などが含まれ、Java 11+ で実行できます。citeturn1search20turn14search2turn10view0  
Apalache は“次の一手”として有効です。Apalache は SMT（Z3 等）を用いる symbolic model checker で、bounded model checking や inductive invariant チェックに強く、TLC が状態爆発する局面の補助になります。citeturn13search5turn13search13turn13search21turn0search3  

### macOS/Linux セットアップ（最小手順）

**前提（共通）**  
TLC は Java 11 を要求します（ツールチェーンとして Java 11+ が前提）。citeturn1search20turn10view0  

**Toolbox を使う（学習・反例調査に強い）**  
- Toolbox は TLA+ ツールの IDE で、編集・PlusCal 変換・モデル検査を統合します。citeturn14search6turn14search21  
- 取得元は tlaplus の release から入れるのが一般的です（最新は release ページに集約）。citeturn10view0turn1search4  

**CLI TLC を使う（CI/再現性に強い）**  
- 仕様と同ディレクトリの `MySpec.cfg` を自動で読む形で、`java -jar tla2tools.jar MySpec.tla` のように実行できます。citeturn10view0  
- tlaplus の README には `tla2tools.jar` 内の各サブツール（parser / TLC / PlusCal translator 等）の起動例があります。citeturn1search20  
- Toolbox 外でツールを回す公式ガイド（standalone tools）もあります。citeturn14search3  

### 反例トレースを “機械可読” で取る（反例→テストの要）

TLC はエラートレースを JSON 形式でダンプできます（`-dumpTrace json`）。少なくとも TLA+ Wiki およびコミュニティの議論で、JSON 出力が実用機能として扱われています。citeturn10view0turn13search23turn13search12  
ここまで整うと「CI でモデル検査 → 失敗時に JSON trace を artifact として保存 → テスト生成ツールが読む」が現実的になります。citeturn10view0turn13search23  

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["TLA+ Toolbox screenshot","TLC model checker error trace output","Apalache model checker logo","PlusCal translator TLA+ Toolbox"],"num_per_query":1}

### 反復（iterate）の型

- 仕様は “小さくして頻繁に回す” のが原則です。状態空間爆発は避けられないため、小モデルを回してバグを早期に拾う、という実務ガイドが継続的に共有されています。citeturn0search0turn0search18  
- PlusCal を使う場合、PlusCal アルゴリズムは TLA+ モジュールのコメントとして書き、translator が TLA+ への変換を書き込みます（Toolbox から実行可能）。citeturn14search21turn14search6  

## Minimal model architecture

### 最初のモデル境界

**モデルに入れる（必須）**  
- **owner-thread ルーティング**：キー（または keyspace partition）→ owner の写像と、owner ごとのリクエストキュー、owner が 1 ステップずつ処理する実行モデル。  
- **blocking セマンティクス（BLPOP/BRPOP 等の代表）**：ブロック登録、wakeup 選択（FIFO、複数キー待ちのキー優先順）、timeout による nil 返却、明示的キャンセル（CLIENT UNBLOCK 的なもの）。Redis の BLPOP 仕様は「待ち行列順（最初に block した client が最初に供給される）」「複数キーは引数順で判断」「timeout の扱い」等が明確に文章化されているため、ここを抽象化する価値が高いです。citeturn3view0turn4view0  
- **replication 順序**：leader が “コマンドストリーム” を replica に送る、という抽象。Redis の replication は master→replica へコマンドストリームを送る方式が基本で、リンク断では部分再同期/完全再同期がある、という全体像が明示されています。citeturn2view0turn1search10  
- **expiration による dataset 変化**：周期タスクとしての “expire” を最小限導入し、expire が replication に DEL として現れる（master 側で中央集権的に処理）という抽象だけ保持する。citeturn5search8turn2view0  

**モデルから外す（当面不要）**  
- TCP、RESP パーサ、コマンドディスパッチの網羅（＝I/O 実装詳細）は、状態爆発と保守負担の割にバグ検出力が薄い。Tool/IDE の統合目的は「設計の根幹（並行性と順序）」に集中することです。citeturn0search0turn14search6  
- 性能（スループット/レイテンシ）、メモリレイアウト、実装最適化の正しさはモデル検査の対象から外し、ベンチ/プロファイル/負荷試験に残すのが妥当です（ただし“順序”に関わる最適化は例外で、順序バグの温床になりやすいので抽象し直して入れる）。citeturn0search0turn12view3  

### モジュール分割（保守性優先）

以下の “薄い層” に分割すると、M0→M4 の増築がしやすく、実装変更にも追随しやすい構造になります。

- `ServerCore.tla`：共通型（Clients/Owners/Keys）、リクエスト生成、実行結果（reply）履歴など。citeturn10view0turn1search20  
- `OwnerRouting.tla`：`OwnerOf(key)` と owner キュー、owner 実行ステップ（`ExecOwner(o)`）。  
- `KV.tla`：GET/SET の抽象 state（`kv[key]`）。  
- `Blocking.tla`：BLPOP/BRPOP の登録、待ち行列、wakeup、timeout、キャンセル。Redis の BLPOP 優先規則（FIFO、キー順、timeout）がここに対応します。citeturn3view0turn4view0  
- `Replication.tla`：`replLog`（leader 側の write 由来イベント列）、`replicaIndex`（適用済み prefix 長）、適用 action。Redis は master が replica にコマンドストリームを送る、と明記しているので、ここを “順序” と “欠落しないこと” のみに圧縮します。citeturn2view0  

### 変数（state）と action（遷移）の最小セット

**定数（最初の bounded モデル想定）**  
`Owners, Clients, Keys` は小さい有限集合（例：各 2）を前提にします。TLC は有限モデルを前提とする explicit-state model checker で、実務上も小モデルを使って状態爆発を抑えます。citeturn10view0turn0search18  

**主要変数（例）**  
- `kv ∈ [Keys → Values ∪ {Nil}]`  
- `ownerQ ∈ [Owners → Seq(Request)]`（owner 受信待ちのリクエスト列）  
- `inflight ∈ [Owners → (Request ∪ {None})]`（owner がいま処理中かどうか：実装に合わせて省略可）  
- `blockedQ ∈ [KeyOrWaitSet → Seq(Client)]` または `blockedReq ∈ [Client → BlockInfo ∪ {None}]`  
- `time ∈ Nat` と `deadline ∈ [Client → Nat ∪ {NoDeadline}]`（timeout を離散時間で抽象）  
- `replLog ∈ Seq(WriteEvt)` と `replicaApplied ∈ 0..Len(replLog)`  

**主要 action（例）**  
- `EnqueueReq(c, cmd)`：クライアントがコマンドを発行し、該当 owner の `ownerQ` に入る。  
- `ExecOwner(o)`：owner がキュー先頭を 1 件実行し、`kv`/リスト状態/blocked 状態/replLog を更新する。  
- `Block(c, keys, timeout)`：BLPOP 系が要素なしで block 状態へ。Redis は timeout=0 で無期限 block を明示しています。citeturn3view0  
- `Wakeup(c)`：push 等により進捗可能になった client を unblocked（再処理）へ。Redis は「最初に block した client を最初に供給」「複数キーは引数のキー順で選ぶ」を明示しています。citeturn3view0  
- `Timeout(c)`：締切に達したら nil 返し。BLPOP は timeout 経過で nil を返す、と明示されています。citeturn3view0  
- `ClientUnblock(ctrl, c, mode)`：外部から blocked client を解除（TIMEOUT/ERROR 的）。Redis は `CLIENT UNBLOCK` が TIMEOUT/ERROR の 2 モードを持つことを明記しています。citeturn4view0  
- `ReplicateStep()`：replica が `replLog` の次イベントを 1 個適用。Redis replication は master が “stream of commands” を送る方式と説明しています。citeturn2view0  
- `Tick()`：時間が進む（timeout を起こす）。  

**TLA+ スケルトン（雰囲気）**  
```tla
---- MODULE ServerCore ----
EXTENDS Naturals, Sequences, TLC

CONSTANTS Owners, Clients, Keys, Values, MaxQ, MaxTime

VARIABLES kv, ownerQ, blockedReq, time, replLog, replicaApplied

Init ==
  /\ kv = [k \in Keys |-> Nil]
  /\ ownerQ = [o \in Owners |-> << >>]
  /\ blockedReq = [c \in Clients |-> None]
  /\ time = 0
  /\ replLog = << >>
  /\ replicaApplied = 0

Next ==
  \/ \E c \in Clients: EnqueueReq(c)
  \/ \E o \in Owners: ExecOwner(o)
  \/ \E c \in Clients: Timeout(c)
  \/ ReplicateStep
  \/ Tick
====
```

この形は「owner の 1 ステップ実行」「blocking の状態遷移」「replication 適用」を分離しやすく、反例の interleaving を“工程表”として読みやすいのが利点です。citeturn0search0turn3view0turn2view0  

## Property catalog

### Safety invariants

ここでは “まず書くべき” 不変条件を、モデル段階（M0〜）で優先度順に並べます。TLC は invariance（不変式）チェックを主要用途として想定しており、push-button で回せるのが強みです。citeturn15search17turn10view0  

**owner 直列化（最優先）**  
- **KeyOwnerConsistency**：すべての keyed 操作は `OwnerOf(key)` にルーティングされ、その owner のキューからしか実行されない。  
  - 想定バグ：ハッシュ/partition 計算の変更で誤 owner へ送る、multi-port で port→runtime の対応がずれる、など。  
- **SingleKeySequentiality（抽象的線形化の下地）**：同一キーの更新は、owner の実行順序に沿って `kv[key]` が遷移する（“同一キー上の並行”が起きない）。  

**GET/SET（M0 の核）**  
- **ReadYourWrites（単純化）**：同一クライアントが同一キーへ `SET` 後の `GET` で、少なくとも古い値へ戻らない（「戻り」バグ検出に特化）。  
  - フル線形化を最初から証明するより、反例が読みやすく保守しやすい“不変条件”から始める方が週次運用に合います。citeturn0search0turn10view0  

**blocking（BLPOP 系の順序仕様）**  
Redis の BLPOP は、(a) 複数 client が同一キー待ちなら FIFO、(b) 複数キー待ちなら引数で左から、(c) timeout で nil、(d) 同一トランザクション等で複数キーが同時に non-empty になった場合も引数順、を明確に述べています。citeturn3view0  
これに対応する不変条件は以下です。

- **FIFOBlockedClientsPerKey**：同一キー待ちの client は “待ち始めが早い順” に提供される。citeturn3view0  
- **KeyOrderForMultiKeyWait**：1 client が複数キー待ちの場合、提供されるキーは「その時点で non-empty なキーのうち、待ち引数で最左のもの」。citeturn3view0  
- **NoLostWakeup**：push（または等価の供給）が発生したのに、対応する blocked client が復帰しないまま取り残される状態が存在しない（wakeup キューや beforeSleep 相当フェーズのミスを炙る）。Redis は “write が進捗可能にした client を unblocked として内部キューに入れ、idle 前に再チェックして提供する” 旨を説明しています。citeturn3view0  
- **TimeoutNoPop**：timeout による nil 返却は要素を消費しない。citeturn3view0  
- **CancelSemantics**：外部解除（CLIENT UNBLOCK 相当）で TIMEOUT と ERROR の 2 種を区別する（少なくとも ERROR の場合は“要素消費なし＆エラーコード”のような差分が出る）。Redis は `CLIENT UNBLOCK` の TIMEOUT/ERROR と `-UNBLOCKED` を明記しています。citeturn4view0  

**replication（順序と欠落）**  
Redis replication は、master が dataset 変化（client writes/expired/evicted 等）を replica に “stream of commands” として送る、と説明しています。citeturn2view0  
この抽象で最初に守るべき不変条件は次です。

- **ReplicaIsPrefixApply**：replica の状態は、`replLog` の prefix（`0..replicaApplied-1`）を順に適用した結果に一致する。  
- **NoReorderNoSkip**：`replicaApplied` は 1 ずつ増える以外の進み方をしない（= 順序入れ替えや飛ばしを禁止）。  
- **ExpireReplicatesAsDEL（抽象）**：expire で消えるキーは replication ストリーム上 “DEL 的イベント” として現れる（master 側で中央集権的に処理される）。citeturn5search8turn2view0  

### Liveness properties

Liveness は TLC のチェックコストや fairness 設計と強く結びつくため、最初から全部は入れず、モデルが安定したら段階導入を推奨します。TLC は WF/SF を含む一部の時相式を効率よく扱う工夫がある一方、liveness は状態グラフが大きくなりやすい点も言及されています。citeturn1search1turn12view0  

**owner 進捗**  
- **OwnerQueueDrains（弱い形）**：ある owner の `ownerQ[o]` が“ずっと空でない”なら、いつか先頭が実行される。  

**blocking 進捗**  
- **EventuallyUnblockOnSupply**：client が block 中に、待ちキーのいずれかへ要素が供給され、かつその状態が維持されるなら、いつか client は復帰して要素を受け取る。citeturn3view0turn1search17  

**replication 進捗**  
- **ReplicaCatchesUp（条件付き）**：リンク断等が起きない条件下で、`replicaApplied` は最終的に `Len(replLog)` へ追いつく。Redis はリンク断時の部分再同期/完全再同期の存在を明記しているため、モデルでも “切れない” 条件を明示してから書くのが安全です。citeturn2view0turn1search10  

### Fairness assumptions

fairness は「action が十分頻繁に enabled なら起きるべき」という“実行の公正さ”の仮定です。講義資料でも fairness を “global constraints” として説明しています。citeturn1search17turn1search1  

**推奨（この種のサーバに合う仮定）**  
- **WF（Weak Fairness）を基本**：  
  - `WF_vars(ExecOwner(o))`：owner キュー先頭が常に実行可能なら、いつか実行される。  
  - `WF_vars(Tick)`：時間は永遠に止まらない（timeout を起こすため）。  
  - `WF_vars(ReplicateStep)`：replica の apply ステップが常に可能なら、いつか進む。  

**SF（Strong Fairness）を慎重に**  
SF は “enabled になるのが断続的でも無限回ならいつか起きる” なので、スケジューラの偏り・飢餓を仕様上で消してしまう危険があります（= 実装のバグを見えなくする）。このため、まず WF で必要最小限の進捗だけ仮定し、SF は「実装が本当にそういう fairness を提供する」と説明できる箇所に限定するのが現実的です。citeturn1search17turn1search1  

## Incremental modeling roadmap

### M0

**ゴール**：Redis ライクな最小 KV を “並行に叩いても壊れない” 形で固定する。  
**入れるもの**：`GET/SET`、クライアント要求列、単純な `kv`。  
**外すもの**：owner、blocking、replication。  
**チェック**：`ReadYourWrites` や「値があり得ない遷移をしない」系の invariant。  
**モデルサイズ（推奨初期）**：`Clients=2, Keys=2, Values=2`。状態爆発対策として小モデルを採るのは実務的に推奨されます。citeturn0search18turn10view0  

### M1

**ゴール**：owner-thread actor の直列化仕様を“合意された形”で固定し、実装が変わっても仕様がブレないようにする。  
**入れるもの**：`OwnerOf(key)`、`ownerQ[o]`、`ExecOwner(o)`。  
**重要ポイント**：実装詳細（channel の種類、ワーカーの数、port-to-runtime）を写すのではなく、**「キー操作は owner キューで順序実行」**という観測可能意味だけ残す。  
**チェック**：KeyOwnerConsistency／SingleKeySequentiality。  
**CI 上の狙い**：M1 が通らない変更は “並行性の意味が変わった” サインなので、仕様か実装いずれかを明示的に更新する運用にする（実装ドリフト抑止）。  

### M2

**ゴール**：blocking コマンドの wakeup/timeout/fairness を固定し、回 regress を早期検出する。  
**入れるもの**：BLPOP/BRPOP 代表、blocked 登録、wakeup 選択、timeout、明示キャンセル。  
**仕様根拠**：Redis BLPOP は FIFO、複数キー待ちのキー順、timeout nil、同時に複数キーが non-empty になった場合もキー順、など順序仕様が明確です。citeturn3view0turn0search8turn4view0  
**チェック**：FIFOBlockedClientsPerKey、KeyOrderForMultiKeyWait、TimeoutNoPop、CancelSemantics。  

### M3

**ゴール**：replication の「順序」と「欠落しない」を固定する（apply の並行化やバッファリング変更で壊れやすい）。  
**入れるもの**：`replLog`（leader 側イベント列）、`replicaApplied`、`ReplicateStep`、リンク断/再同期の最小抽象。  
**仕様根拠**：Redis replication は master→replica へ “stream of commands” を送る、リンク断時は部分/完全再同期、などを明示しています。citeturn2view0turn1search10  
**チェック**：ReplicaIsPrefixApply、NoReorderNoSkip。  

### M4

**ゴール**：cluster mode の MOVED 的セマンティクスを、“routing の抽象”として追加し、owner/thread の境界変更が cluster 互換性を壊さないようにする。  
**入れるもの（最小）**：`SlotOwner(slot)` と “誤ノードへ送ったら MOVED（新しい endpoint）を返す” 抽象。クライアントが slot→node を学習する前提も最小で。  
**仕様根拠**：Redis cluster spec は MOVED リダイレクト、client-side の slot→node マップ更新戦略、ASK/MOVED の区別などを文書化しています。citeturn7view0turn1search23  
**注意**：M4 は状態数が急増しやすいので、Key 数をさらに絞るか、single-slot の研究モデルに分離するのが現実的です。citeturn0search0turn12view3  

## State-space control plan

### 初期の bounded 設定（“数分で回す”ための出発点）

状態爆発は explicit-state model checking の本質的課題で、仕様や性質のわずかな追加で指数的に増えます。実務ガイドでも state space explosion が中心テーマとして扱われます。citeturn0search0turn12view3  
そのため、最初の CI 目標は “小さなモデルを毎回通す” です。

**推奨スタート（M1〜M2 を想定）**  
- `Owners = {o1, o2}`  
- `Clients = {c1, c2}`  
- `Keys = {k1, k2}`（blocking 用の list key は 1〜2 に限定）  
- `Values / Elements = 2 種類`（モデル値でも可）  
- `MaxQ（owner キュー長）= 2〜3`、`MaxTime = 3〜5 tick`  
この程度のスコープから始めるのは、TLC 実務で「モデルサイズは小さく（例：3〜5）」「順序を記録する等の permutation を増やす操作を避ける」などの助言が共有されている点とも整合します。citeturn0search18turn12view3  

### 対称性（symmetry）と model values

TLC には model values があり、プロセス名などを “数” ではなく未解釈の値として扱うことで、偶発的な演算ミスを検出したり、symmetry reduction を効かせたりできます。citeturn0search6turn0search10  
ただし symmetry reduction は（少なくとも TLC の文脈で）liveness チェックと相性が悪い/制約があることが指摘されています。実務スライドでも「liveness checking ではサポートされない（警告）」と明示されています。citeturn12view3  
したがって運用としては、  
- **Safety-only モデル（symmetry を積極活用）**  
- **Liveness 重点モデル（symmetry を切る/最小化）**  
を分けるのが管理しやすいです。citeturn12view3turn0search6  

### “分割して勝つ” モデリング

- **モデルを機能ごとに分割（M1 と M2 を別モデルにする等）**：TLC は有限モデルを網羅探索するため、機能を足し算すると爆発しやすいです。citeturn0search0turn10view0  
- **履歴を全部記録しない**：線形化のために全オペ列を保持すると permutation が増えがちです。必要なら “観測点” を最小化し、反例が必要十分に理解できるログ構造に絞る方が実務的です。citeturn0search18turn12view3  
- **時間は離散 tick に圧縮**：timeout を実時間で扱わず、締切到達の有無だけを tick で表す。  

### TLC と Apalache の使い分け（研究質問への回答）

**Toolbox + TLC（production-proven）**  
- “push-button” で invariants を回す用途に強い。Toolbox は統合 IDE としてこれを支援します。citeturn14search6turn10view0  
- Liveness（WF/SF を含む一部の式）も扱えるが、状態グラフが大きくなりがちでコストが跳ねる点が明示されています。citeturn1search1turn12view0  

**CLI-first TLC（production-proven）**  
- CI に最適。`tla2tools.jar` は CLI で動き、設定ファイル（`.cfg`）や trace ダンプなどによって再現性のある運用が可能です。citeturn10view0turn1search20turn13search23  

**Apalache（条件付きで有効：advanced）**  
- Apalache は SMT ベースで bounded model checking と inductive invariants に強く、TLC が厳しい局面の補助になる。citeturn13search5turn13search21turn0search3  
- ただし bounded（長さ/パラメータが固定・上限あり）という性質や、扱える言語機能の制約があるため、TLC の代替というより“併用”が現実的です。citeturn13search5turn13search21turn0search27  
- Liveness は liveness-to-safety 変換で扱う、という位置付けがドキュメントにあります（この変換は使いどころを選ぶ）。citeturn0search3turn13search10  

### TLA+ に入れない方がよいもの

- 性能・最適化（スループット、ロックフリー最適化の速さ）はモデル化しても現実との対応が薄く、テスト/ベンチで扱う方が筋が良いです。state space explosion の実務資料も、性能“そのもの”より探索可能性の方を主問題として扱います。citeturn0search0turn12view3  
- ネットワーク細部（TCP の分割、RESP のエラー処理など）はスコープを肥大化させやすいので、最初は除外して “順序” のみに集中するのが週次運用に向きます。citeturn0search0turn10view0  

## Counterexample-to-test workflow

### 反例の抽出

**TLC → JSON trace**  
TLC はエラートレースを `-dumpTrace json` で出力できます。公式に近いドキュメント（TLA+ Wiki）でも “tla と json の 2 形式” が言及され、メーリングリストでも機械可読化の議論が継続しています。citeturn10view0turn13search23turn13search12  

運用としては、CI 失敗時に次を artifact 化します。  
- `MC.out`（人間向け）  
- `trace.json`（機械向け）  
- モデル設定（`.cfg`）  

### 反例 interleaving の “決定的再生” への落とし込み方

反例から Rust テストに落とすときの最大の課題は「並行スケジューリングと時間が非決定的」な点です。ここは **“テスト用の決定化スイッチ”**を設計に入れる必要があります（実装詳細が不明なため、ここは条件付き提案として整理します）。

**推奨アプローチ（owner-thread actor に合う：条件付きだが効果が高い）**  
- テスト環境では owner 実行を “自動で回る runtime” に任せず、**owner ごとに 1 ステップずつ進める API**（例：`step_owner(o)`）を露出させる。  
- TLC trace の各ステップ（`ExecOwner(o1)`, `Tick`, `ClientUnblock`…）を、そのままテストコードの手続き列に変換する。  
- これにより「反例 interleaving＝テストの実行手順」になり、並行バグを“毎回同じ順番”で再現できます。  
この設計は TLA+ の “遷移（action）列” をテストの “命令列” とみなす思想で、JSON trace 化が実務上重要になります。citeturn10view0turn13search23  

### タイムアウト／時間の再現

timeout を安定再現するには、テストで “時間” を決定化する必要があります。もし実装が entity["organization","Tokio","rust async runtime project"] のタイマーを使うなら、Tokio はテストで時間を pause/advance でき、`#[tokio::test(start_paused = true)]` などで paused 状態から始められます（current_thread runtime が要件）。citeturn14search0turn14search34turn14search26  

**テスト落とし込みの要点**  
- TLC 側の `Tick`（または timeout 到達）を、Tokio の `advance(duration)` に対応付ける。citeturn14search26turn14search8  
- “複数タイマーが同じ瞬間に ready になる”場合、どの順で poll されるかは自由度があるため、テストで順序を固定したいなら owner step 実行の順序も合わせて固定する。Tokio の `advance` は “同時に ready になり得る”ことを明示しています。citeturn14search26  

### 内部並行性の探索（補助）

owner-thread actor が正しくても、内部で共有状態・ロック・atomics を使う箇所は別種の並行バグを持ち得ます。ここには Rust の concurrency テストツール（例：loom）を補助的に使うのが現実的です。loom は「テストを多数回実行し、並行実行の順序を permute して探索する」ツールだと説明されています。citeturn5search3turn5search14  

**位置付け**  
- TLA+/TLC：**設計上の interleaving**（owner 直列化・blocking・replication）を潰す。  
- loom：実装の **低レベル並行性（メモリモデル/同期）**を潰す。citeturn5search3turn5search14  

### 具体的な “反例→テスト” の変換手順（最小）

1) TLC で失敗したモデルを `-dumpTrace json` 付きで回して JSON を得る。citeturn10view0turn13search23  
2) JSON を読み、各ステップの `actionName` と（必要なら）関係する ID（owner/client/key）を抽出する。  
3) 生成する Rust テストは「接続や RESP を使わず、内部 API で `EnqueueReq`/`step_owner`/`advance_time` を呼ぶ」最小統合にする（repo 非公開のため、これはテスト用の最小 API を“用意できるなら”という条件付き）。  
4) timeout が絡む場合は `start_paused` + `advance` で時間を固定化する。citeturn14search34turn14search26  

（参考：Rust 側の雰囲気。実装に合わせて読み替え）  
```rust
#[tokio::test(start_paused = true)]
async fn reproduces_tlc_trace_case_17() {
    let mut h = TestHarness::new(); // owner runtimes in deterministic mode
    h.enqueue(client(1), cmd("BLPOP", ["k1", "k2"], timeout=2));
    h.step_owner(owner_of("k1"));   // corresponds to ExecOwner(o)
    tokio::time::advance(Duration::from_secs(2)).await; // Tick/Timeout
    h.step_owner(owner_of("k1"));
    // assert on reply history / store state
}
```

## Risks/anti-patterns and Do this week checklist

### Risks/anti-patterns

**過剰モデリング（最頻出）**  
TCP/RESP、細かなエラー系、パーサ状態などを入れると、状態爆発で回らなくなり、週次運用が崩れます。実務資料でも state space explosion は中核課題として扱われ、探索可能性が最重要です。citeturn0search0turn12view3  

**fairness の置き過ぎ／間違い**  
WF/SF は強力ですが、強い fairness を置くほど “実装が保証しない進捗” を仕様が前提にしてしまい、現実の starvation/飢餓バグを隠します。fairness 条件は “enabled なら起きるべき” というグローバル制約である旨が説明されています。citeturn1search17turn1search1  

**仕様が通るのに役に立たない**  
- 観測可能な振る舞い（返信順、wakeup 規則、replication の prefix 性）に落ちていない。  
- 反例が出ても実装に落とせない（trace→テストの橋がない）。  
このため、TLC の JSON trace 出力を前提に “反例→テスト” の流れを最初から設計に含めるのが重要です。citeturn10view0turn13search23  

**liveness を最初から重くやりすぎる**  
liveness は探索が重くなりやすく、対称性最適化とも相性が悪いケースがあります（symmetry reduction が liveness でサポートされない/警告される旨）。まず safety を固め、必要箇所だけ liveness を足すのが現実的です。citeturn12view3turn1search1  

### Do this week checklist

- 週次で守る“契約”を決める：M1（owner 直列化）と M2（blocking 規則）を最優先にする。citeturn3view0turn0search0  
- M0 の TLA+ 仕様（Keys=2, Clients=2, Values=2）を 1 ファイルで作り、TLC で invariant が回ることを確認する。citeturn10view0turn0search18  
- M1 用に `OwnerOf(key)` と `ownerQ[o]` を導入し、KeyOwnerConsistency を invariant として追加する。  
- M2 用に BLPOP の “FIFO とキー順と timeout” を最小抽象で導入する（まず list key=1 だけでもよい）。citeturn3view0  
- `CLIENT UNBLOCK` 相当の “外部解除（TIMEOUT/ERROR）” をモデルに 1 本だけ入れる（反例が出やすい）。citeturn4view0  
- TLC を CLI で回せるようにし、CI で実行できるコマンド（`java -jar tla2tools.jar ...`）を固定する。citeturn10view0turn1search20  
- CI 失敗時に `-dumpTrace json` を必ず吐き、artifact として保存する。citeturn10view0turn13search23  
- JSON trace を “手で読む”ための最小スクリプト（action 列と関係 ID を抜く）を作る（言語は何でもよい）。citeturn10view0turn13search23  
- Rust 側に（可能なら）owner 実行を 1 ステップずつ進めるテスト用フックの設計案を 1 枚作る（実装詳細は未確定でよい）。  
- タイムアウト再現のために、Tokio の `start_paused`/`advance` が使える前提かを確認し、使えるならテスト指針に明記する。citeturn14search34turn14search26  
- 状態爆発対策として “model values/symmetry を safety モデルに入れるか” を決める（liveness とは分離する）。citeturn0search6turn12view3  
- 週末までに「M1 か M2 のどちらかで意図的にバグを入れ、TLC が反例を出し、JSON trace が出て、読める」まで到達する（導入の成功体験を作る）。citeturn10view0turn14search6