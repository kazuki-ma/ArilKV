# Redis互換RustサーバにおけるActor/Owner-thread移行と最小ロック設計

## エグゼクティブサマリー

- 最短で効く方針は「**書き込み（状態変更）を必ずowner-thread actorへ集約し、actor内を“ロック無し”にする**」こと。これによりホットパスのcoarse lockを設計上消し、ロック最適化を“局所問題”に押し込められる（shared-nothingの基本形）。citeturn1search22turn3search4  
- actor向けの内部データ構造は「**“オーナーが単独writer”であること**」を最大限使う。読み取りは(1)同一actor内で完結、(2)必要なら“read-mostlyメタデータ”に限り楽観読み（version検証）を使い、失敗時にactorへフォールバックする二段構えにする。citeturn0search12turn0search5  
- “楽観fast path＋フォールバックlock”の実務で最も再現性が高い参考は **entity["organization","Linux kernel","os kernel project"]のseqcount/seqlock**：lockless reader（再試行）と、必要時にロックへ落とすAPI（例：read_seqbegin_or_lock）の発想がそのまま使える。citeturn0search12turn0search8  
- Java系の実務パターンは **entity["organization","OpenJDK","open source jdk project"]のStampedLock** が代表例：tryOptimisticRead→validateで成功ならロック無し、失敗したらreadLockへフォールバック。ただし楽観区間は「一貫しない値を読んでいる前提」で書く必要があり、誤用しやすい（＝適用範囲を絞るべき）。citeturn0search21turn0search5  
- 共有が残る場所（例：グローバル設定、統計、低頻度の管理用辞書）は、mutexより **RCU/epochや“Arcスナップショットの原子的差し替え”** が適合しやすい。RCUはread-mostly最適化で、読み側が極めて軽い代わりに更新／回収（grace period）が難所。citeturn1search0turn1search5  
- ただし“RCUで全部解決”は罠：更新頻度が高い箇所・ポインタ追跡を伴う可変構造・長いread-side区間が混ざると詰まりやすい（grace period遅延、メモリ滞留など）。citeturn1search12turn1search13  
- owner-thread modelは「ロックを減らすが、**メッセージパッシングのwake-up/転送コスト**」が別のボトルネックになり得る。よって“どこまでactorへ投げるか”と“どこを楽観共有readにするか”は、ベンチ＋フレームグラフで段階的に最適点を探す。citeturn3search7turn1search10  
- 期限切れ（expiration）はRedis互換上の落とし穴。Redisは「アクセス時（passive）＋周期サンプリング（active）」で消すため、**各shardで同様のサイクル**にすると整合性と性能の両立がしやすい。citeturn0search3turn0search27  
- MULTI/EXECは互換性の最重要論点の一つ。Redisはトランザクション中のコマンドを直列実行し、他クライアント要求が“途中に割り込まない”ことを保証する。shard並列実行と衝突しやすいので、当面は**境界を“止める（barrier）”設計**が必要になる場面がある。citeturn4search7turn1search7  
- GCのZGC/Shenandoahからは「**短い停止で全体の位相を合わせる（handshake/バリア）**」という設計語彙が移植可能。一方で“colored pointers/読みバリアで参照を常に矯正”のような仕掛けはKVサーバではコスト過大になりやすく、安易な直輸入は非推奨。citeturn0search2turn4search0  

## エビデンステーブル

| 主張（設計に効く形） | 根拠ソース | 確度 |
|---|---|---|
| seqlock/seqcountは「lockless reader（再試行）＋writerは外部で直列化」が前提で、read-mostlyに向く | Linuxのseqlockドキュメントciteturn0search12turn0search0 | 高 |
| seqlockには“楽観に読んでダメならロックへ落とす”APIがあり、書き込みスパイク時のreader starvation回避に使われる（read_seqbegin_or_lock） | Linuxのseqlockドキュメントciteturn0search8 | 高 |
| StampedLockはtryOptimisticRead→validateで成功ならロック無し、失敗ならロック取得にフォールバックするモデルを明示している | StampedLock Javadocciteturn0search21turn0search1 | 高 |
| StampedLockの楽観読取りは“不整合な値を読んでいる可能性”を前提とし、validate後に使う形に限定すべき＝適用範囲を狭めないと危険 | StampedLock Javadocciteturn0search5turn0search21 | 高 |
| RCUはread-mostlyに最適化された同期機構で、grace period概念により更新後の安全な回収を可能にする | Linux RCU “What is RCU?”citeturn1search0turn1search5 | 高 |
| RCUはgrace periodが遅れるとメモリ滞留やスタールの原因になり得る（特に長いread-side区間等） | Red Hatの解説citeturn1search12 | 中 |
| Seastarのshared-nothingは「コア間共有＝ロックのコスト」を避け、要求をshardへ振り分けメッセージパッシングで通信する設計である | Seastarのshared-nothing説明citeturn1search22 | 高 |
| DragonflyはRedis互換でshared-nothingによりkeyspaceをスレッドへ分割し、各スレッドが自分の辞書スライスを管理する（＝ロック競合を減らす） | Dragonfly README/設計説明citeturn3search4turn3search8 | 中〜高 |
| Redisのexpirationはpassive（アクセス時）＋active（周期サンプリング）である | Redis EXPIREドキュメントciteturn0search3turn0search11 | 高 |
| Redisトランザクションは「全コマンド直列実行」「途中に他クライアント要求が割り込まない」保証がある | Redis Transactionsドキュメントciteturn4search7 | 高 |
| HotSpotのthread-local handshakeは、global safepointと違い“各スレッドが自分の処理を終え次第継続できる”機構として定義される | JEP 312citeturn0search2 | 中〜高 |
| ZGCはcolored pointers＋load barrierにより、アプリスレッド実行中の並行処理（例：移動）を可能にすると説明している | JEP 333citeturn4search0 | 高 |
| ConcurrentHashMapは「検索（retrieval）はロックを伴わない」ことを仕様として述べ、全体を止めるtable-wide lock提供もない | ConcurrentHashMap Javadocciteturn2search0turn2search28 | 高 |
| LongAdderは高競合下でAtomicLongより高スループットが期待できる（統計用途向き） | LongAdder Javadocciteturn2search37 | 中〜高 |
| memtier_benchmarkはRedis（Redis Labs）製のKV向け負荷生成ツールとして広く利用されている | memtier_benchmark GitHub/Redisブログciteturn5search1turn5search17 | 高 |
| redis-benchmarkはRedis同梱のベンチツールで、Nクライアント同時のMリクエストを模擬できる | Redisベンチドキュメントciteturn5search0 | 高 |

## 技術マトリクス

| 手法/原則 | 正しさモデル（どこまで保証するか） | 性能見込み（ホットパス観点） | 実装/運用コスト | 代表的な失敗モード | Fit（現状説明への適合） |
|---|---|---|---|---|---|
| coarse mutex（全体1ロック） | 最も単純。直列化で整合性は取りやすい | 競合でスループット低下・tail悪化しやすい | 低 | lock convoy、ホットキーで全停止 | ★☆☆☆☆ |
| shard mutex（shard単位ロック） | shard内直列化。跨ぎは別途必要 | coarseより改善。だがホットshardで詰まる | 低〜中 | マルチキーでデッドロック（順序不統一）、粒度不足 | ★★☆☆☆ |
| key-range lock / lock striping（複数ロックプール） | 範囲/ハッシュで部分ロック。正しさは設計次第 | 競合分散で改善し得る | 中 | ロック順序/アップグレードで複雑化、取りこぼし | ★★☆☆☆（“actor前”なら） |
| actor-only serialization（shared-nothing） | **owner-threadが唯一のwriter**。コマンド順序はactorのキュー順 | ロックコスト最小。キャッシュ局所性も良い | 中（ルーティング/跨ぎ設計が要） | マルチキー/管理系で“結局グローバル停止”が出る | ★★★★★ citeturn1search22turn3search4 |
| actor + optimistic shared reads（限定） | readは楽観、失敗時にactorへ。writeはactorに集約 | readの往復を削減できる可能性 | 中〜高 | 誤った一貫性仮定でバグ、再試行嵐 | ★★★★☆（read-mostly限定） citeturn0search5turn0search12 |
| seqcount/seqlock型 version検証（fast path） | readerは「開始/終了のversion一致」を確認。writerは直列化必須 | read-mostlyで強い。write多いとretry増 | 中 | ポインタ追跡/可変構造に誤適用、retryスパイク | ★★★★☆（メタデータ/スナップショット向き） citeturn0search12turn0search0 |
| “try fast then lock”フォールバック（seqlock方式） | まず楽観read。混雑時はロックを取り確実化 | writeスパイクでも読みを前進させられる | 中 | フォールバック条件が不適切だと無限retry/逆にロック過多 | ★★★★☆ citeturn0search8 |
| Java StampedLock型（tryOptimisticRead→validate→readLock） | 「validate成功時のみ値を利用」前提 | 読みは軽いが、誤用で致命傷 | 中 | 楽観区間が長い/外部呼び出しで不整合、stamp再利用等 | ★★★☆☆（発想は有用、全面移植は危険） citeturn0search21turn0search9 |
| RCU/epoch（スナップショット公開） | readerはロック無しで“古い or 新しい全体”を見る。回収はgrace period後 | readは非常に軽い。更新は重い | 高 | grace period遅延でメモリ滞留/スタール | ★★★★☆（グローバル設定/辞書/統計公開向き） citeturn1search0turn1search12 |
| hazard pointers（高度な回収） | readerが参照中を宣言し、writerが安全回収 | RCUより柔軟だが実装が難しい | 高 | 実装・検証コスト、運用ミスでUAF級バグ | ★★☆☆☆（Phase C候補） citeturn2search2turn2search10 |
| “handshake / barrier”型の全体位相合わせ | “全停止”ではなく、スレッド/actorごとに安全点で処理→継続 | グローバルロックより停止を短くできる | 中 | バリア未到達で待ち、飢餓/デッドロック | ★★★★☆（再構成・スナップショット・TX境界に有効） citeturn0search2turn4search7 |
| striped counters（統計を分散） | 合算時に近似/遅延を許容（統計用途） | ホット統計の競合が減る | 低〜中 | “正確な瞬時値”が必要な用途に誤適用 | ★★★☆☆（INFO/統計向き） citeturn2search37 |

## 具体的なターゲットアーキテクチャ

前提（コード非閲覧のための置き方）  
- 現状はactorルーティングがある一方、内部がmutexで守られており、owner-thread化の効果が出切っていない。  
- 文字列はshard化済みだが、オブジェクト系が単一共有ストアでボトルネックになり得る（説明上）。  
- 互換性は「単一Redisインスタンス相当」を目標と仮定し、Cluster制約（CROSSSLOT等）は“最終手段”として扱う（ただし移行中の安全策として一時導入はあり得る）。citeturn4search3turn4search7  

提案する到達像（“ロックを減らす”ではなく“ロックが要る場所を押し込む”）  

- **データプレーン（通常コマンド）は “actor-only serialization” を原則にする**  
  - 各owner-thread actorが `ShardState` を保持（文字列KVS、オブジェクトKVS、期限情報、キー登録/メタ）。  
  - `ShardState` 内は「そのスレッドだけが触る」前提にして、mutexを原則撤去。shared-nothingの標準形に寄せる。citeturn1search22turn3search4  
  - これによりホットパスは“ロックを取らない”がデフォルトになり、要件の「coarse lock回避」を設計で満たす。

- **“楽観fast path”を使うのは、actor境界を跨ぐ“read-mostly共有”だけに限定する**  
  典型例：サーバ設定スナップショット、コマンドテーブル、INFO用統計、メトリクス公開、読み取り専用の辞書（ACLやユーザ一覧など頻繁に参照・稀に変更）。citeturn1search0turn2search37  
  - 実装イメージは2系統（どちらも「成功したらロック無し、失敗したらフォールバック」を満たす）：  
    - (A) **seqcount/seqlock的 version検証**：共有値に`version`を持たせ、readは前後で一致確認、変化していたらリトライ。書き込みはactor（単独writer）か、低頻度なら短いmutexで直列化。citeturn0search12turn0search8  
    - (B) **Scaled-down StampedLockモデル**：try→validate→失敗ならread lockへ。概念は有用だが、楽観区間が“不整合読み”前提で書けない箇所に適用すると危険（Rustでも同じ）。citeturn0search21turn0search5  
  - 重要な適用制約：seqlockは「ポインタを辿る／寿命が変わる」データに直適用しにくい。従って“共有はスナップショット（不変）化してから”が安全。citeturn0search0turn1search0  

- **グローバル共有になりがちな“オブジェクトストア”は、まず“キー単位でshardに降ろす”のが本命**  
  - Redis互換の多くは「キー→値（型付きオブジェクト）」であり、キーhashでownerが決まるなら「オブジェクト管理も同じownerに寄せる」方が自然。  
  - もし現状の都合で“単一オブジェクトストアをすぐ分割できない”場合は暫定策：  
    - read-only系（例：型確認/サイズ参照/INFO用）だけをスナップショット公開（RCU/Arc差替え）に寄せ、writeはactor経由で単一writer化してロック範囲を縮める。citeturn1search0turn0search12  

- **期限切れ（expiration）は「shardごとに完結するactive/passive」へ揃える**  
  - passive：GET/EXISTS等のアクセス時に期限判定し、期限切れなら削除して“無い”として扱う。  
  - active：周期（例：50ms）でキーをサンプリングして削除。Redis互換のアルゴリズムとして説明されているため、互換性リスクが下がる。citeturn0search3turn0search27  
  - 実装は「グローバルのexpirationタスクが全shardのロックを取りに行く」のではなく、**“タイマtickを各shardへ送る”**（＝barrierメッセージ）にする。shared-nothingを崩さずに動かせる。citeturn0search2turn1search22  

- **レプリケーションとトランザクションは“最小限のグローバル順序付け”が残りやすい領域として、明示的に隔離する**  
  - Redisはレプリカがマスターのコピーになることを目的とし、レプリケーション管理が重要な平面である。citeturn4search2  
  - MULTI/EXECは「直列かつ割り込み無し」を保証するため、shard並列データプレーンとぶつかる。citeturn4search7turn1search7  
  - 推奨：これらを“データ構造ロック”で解かない。代わりに **Coordinator actor（順序付け専用）＋barrier/handshake** を用意し、普段は各shardが独立、必要時だけ全shardが安全点で位相を合わせる（GCのhandshakeの考え方）。citeturn0search2turn4search7  

- **GCからの移植可能/不可能の線引き（明確に）**  
  - 移植可能：handshake（短い安全点で各スレッドに作業させ、終わったら順次走らせる）＝shard間barrier、再構成、スナップショット、TX境界で有効。citeturn0search2  
  - 限定的に移植可能：“バリアをホットロードに入れて不変条件を保つ”という発想。ただしZGCはcolored pointers＋load barrierで参照の矯正を行うが、KVサーバで同レベルの常時バリアを入れるのはコスト・複雑性が重くなりやすい。よって、KV側は「スナップショット公開＋最小限のacquire/release」の方が現実的。citeturn4search0turn1search0  
  - 移植困難：moving GC前提のcolored pointersやread barrier全域適用（ZGC/Shenandoahの主戦術）は、Rustの所有・ライフタイム前提と目的が違い、費用対効果が低い。citeturn4search0turn4search5  

## 段階的な移行計画

**Phase A（数日、低リスク）— “ロック削減の下地”を作る**  
- 目的：最適化が効く場所・効かない場所を可視化し、actor内の“不要ロック”から順に剥がす。  
- 具体策：  
  - ルーティングが正しく機能している前提で、**“owner-thread上でのみ触られるはず”のデータ（例：sharded string store）**からmutexを“内部だけ”外す（APIは維持し、データ競合が起きたら即検出できるアサーション/スレッドID検査を入れる）。shared-nothing前提へ寄せる。citeturn1search22turn3search4  
  - expirationタスクを「全体ロック取得」型から「tickを各shardへ通知」型へ置換（削除処理自体は各shardのactor文脈で実施）。Redisのpassive/activeを踏襲して互換性を確保。citeturn0search3turn0search27  
  - 競合しやすい統計（INFO・メトリクス）を、まずは**shardローカルカウンタの集計**へ（LongAdder相当の“分散更新”の考え方）。citeturn2search37turn4search14  
- 互換性確認：期限切れの振る舞い（アクセス時削除、バックグラウンド削除の頻度）と、基本コマンドの応答が変わっていないことを重点テスト。citeturn0search3  

**Phase B（1〜2週間、中リスク）— “共有ストアを潰し、フォールバックを設計する”**  
- 目的：共有ロックの残骸（特に単一オブジェクトストア、メタデータマップ）をactor所有へ移し、残る共有には“楽観＋フォールバック”を適用する。  
- 具体策：  
  - 単一共有のオブジェクトストアを「キーshardへ分割」するのが本命。Redis互換の多くはキーが主語であり、owner-thread actorはそのキーの唯一writerとして設計できる。citeturn1search22turn3search4  
  - どうしても共有を残す部分は、用途別に二層化：  
    - read-mostly：RCU/Arcスナップショット公開（読取りはロック無しで“全体一貫スナップショット”を見る）。citeturn1search0turn1search5  
    - writeや高更新：actorに寄せる（共有で回さない）。  
  - version検証（seqlock/seqcount発想）を「小さなメタデータ」に限定導入し、**失敗時にactorへ落とす**。read_seqbegin_or_lock的な“観測された混雑でロックへ移行”の考え方を採る。citeturn0search8turn0search12  
- 互換性の注意：MULTI/EXECやWATCH相当の境界は、並列化で破壊しやすい。Redisが保証する「トランザクション中に割り込まない」を満たすには、Coordinator＋barrierの明示が必要。citeturn4search7turn1search7  

**Phase C（高度/任意）— “本当に必要な場所だけ”の先進手法**  
- 目的：残るボトルネックが「actor間メッセージコスト」か「共有readの回収/公開」かを見極め、必要な場合にのみ高度化する。  
- 具体策候補（条件付き）：  
  - RCUの回収が詰まる/メモリ滞留が顕在化するなら、epoch/hazard pointers系の導入を検討。ただし実装・検証コストが高く、誤ると致命的（UAF級）なので“必要性が証明されてから”。citeturn2search2turn1search12  
  - actor間通信のwake-up/転送がCPUを食うなら、接続のスケジューリングや、read-onlyの一部を“楽観共有read”で取り戻す（ただし適用範囲は狭く、validate失敗時のフォールバック必須）。スレッド間メッセージパッシングが高オーバーヘッドになり得る点は研究でも指摘されている。citeturn3search7turn1search10  
  - GC的な“常時バリア”を入れるのは原則避ける（ZGCのcolored pointers/ロードバリアは目的・前提が違う）。KV側はスナップショット公開で十分なことが多い。citeturn4search0turn1search0  

## ベンチマークとフレームグラフ計画

共通方針（全Phase）  
- 変更のたびに「**同一シナリオでの再現性ある比較**」を行う。Redis側でも“変更ごとに測る”ことが推奨され、ツールも整備されている。citeturn5search17turn5search24  
- ツールはまず **redis-benchmark**（同梱）と **memtier_benchmark**（高スループット負荷生成）を基準にする。citeturn5search0turn5search1  
- プロファイルはLinuxならperf由来のフレームグラフ（cargo-flamegraph等）で“ロック待ち/メッセージ転送/ハッシュ/辞書操作/期限処理”の支配率を見る。flamegraphがperf/DTraceを使うことはRust性能本でも明記されている。citeturn5search2turn5search37  

**Phase A：ベンチ計画（ロック除去の下地）**  
- ワークロード：GET/SET、混在（例：GET 80% / SET 20%）、キー空間を広めにしてホットキー比率も振る（Zipfを使えるなら追加）。  
- 主要指標：throughput（ops/s）、p50/p99/p99.9、CPU使用率、コンテキストスイッチ、mutex競合回数（可能なら）。  
- 期待する観測：expiration tick導入後に、定期処理が“全体ロック待ち”から“shard内作業”へ移り、ロック待ちの山が減る（フレームグラフ上のmutex/parking比率低下）。citeturn0search3turn5search37  

**Phase B：ベンチ計画（共有ストア解体＋楽観フォールバック）**  
- ワークロード：オブジェクト系（hash/set/list等がある前提）も混ぜ、型変換（同一キーにSET→HSET等）を意図的に含める。  
- 追加指標：validate失敗率（retry率）、フォールバック回数、actor間メッセージ数、フォールバックによるtailへの影響。StampedLock/seqcount系は“失敗時にフォールバック”が要で、失敗率が性能を決める。citeturn0search21turn0search8  
- 期待する観測：単一オブジェクトストア由来のロック待ちが消える/局所化する。代わりにメッセージ転送が出るなら、その比率を見てPhase Cの判断材料にする。citeturn3search7turn1search10  

**Phase C：ベンチ計画（高度化の妥当性確認）**  
- RCU/epoch/hazard導入を検討する場合：  
  - “更新頻度が低いのにreadが多い”対象で、mutex→RCUにした時にreadパスが軽くなるか、grace period遅延が発生しないか（メモリ滞留／スタール）を測る。RCUはread-mostly最適化だが、grace period遅延が問題になり得る。citeturn1search0turn1search12  
- 役に立つ比較軸：  
  - “actorへ投げる” vs “楽観共有read” の境界を、実測（CPUとtail）で決める（机上で決めない）。メッセージパッシングは高オーバーヘッドになり得るという指摘があるため。citeturn3search7turn1search10  

## リスク登録簿とロールバック基準

主要リスク（起きやすい順＋対策の具体化）  

- Redis互換性（MULTI/EXECの割り込み禁止、順序保証）が崩れる  
  - 背景：Redisはトランザクション中のコマンド直列実行と割り込み不可を保証する。shard並列実行はこの保証を破りやすい。citeturn4search7turn1search7  
  - 対策：Coordinator actor＋barrierで“境界だけ”全shard位相合わせ。Phase Bまでに設計として固定する。  
  - ロールバック基準：互換テストでトランザクション結果が不一致、または「トランザクション中に他クライアントが観測可能な中間状態」を再現したら即時戻す。

- 楽観read（version検証）の誤用で不整合バグ（稀に致命的）  
  - 背景：StampedLockは“楽観区間は不整合を読み得る”前提で書けと警告している。seqlockも適用データを誤ると危険。citeturn0search5turn0search0  
  - 対策：適用対象をread-mostlyメタデータ/不変スナップショットに限定。validate失敗で必ずフォールバック。  
  - ロールバック基準：整合性系の再現バグ（TTL/型/存在判定の矛盾等）が1件でも出たら、当該最適化をfeature flagで即無効化。

- RCU/スナップショット公開でgrace period遅延→メモリ滞留・スタール  
  - 背景：RCUはread-mostlyに強いが、grace periodが遅いとメモリ回収が進まず問題化し得る。citeturn1search12turn1search5  
  - 対策：read-side区間を短く保つ（I/Oや待ちを入れない）、回収のメトリクス化、デバッグ用に“強制同期”経路を持つ。  
  - ロールバック基準：P99が規定値を超え、かつRCU待ち/回収遅延が主要因と判明したら、RCU適用範囲を縮小しmutexへ戻す。

- shared-nothing化により “メッセージ転送コスト” が支配的になってスループットが伸びない  
  - 背景：thread-per-core/メッセージパッシングはwake-up等でオーバーヘッドになり得る。citeturn3search7turn1search10  
  - 対策：ホットコマンドのルーティング/バッチング、read-onlyの限定的な楽観共有readの検討（Phase C）。  
  - ロールバック基準：Phase Bでロック待ちは減ったがops/sが伸びず、フレームグラフで転送が支配的なら、最適化の方向を“通信削減”へ切り替える（ロールバックというより方針転換）。

- expirationの実装差で互換性/観測結果が揺れる（期限直後のGET等）  
  - 背景：Redisはpassive/activeの併用で期限切れキーを削除する。これと異なると“残骸キー”の見え方が変わり得る。citeturn0search3turn0search27  
  - 対策：アルゴリズムを踏襲し、shardごとに同等のサイクルへ。期限判定の単一化（actor内）とテスト。  
  - ロールバック基準：期限切れキーが“存在して見える/見えない”の揺れが互換テストで検出されたら、期限判定の経路をactor内に一本化するまで戻す。

## 今週やることチェックリスト

- owner-thread actor内でのみ触る想定のデータ構造を棚卸しし、「actor専有にできる順」にラベルを付ける（文字列shard、expiration、キー登録など）。citeturn1search22  
- actor専有の構造体に「スレッドID/owner検証（debugアサート）」を入れ、誤アクセスを早期にクラッシュさせる（移行安全性）。  
- 期限切れを「tick→各shardで処理」へ変更するチケットを切る（passive＋activeの互換踏襲を明記）。citeturn0search3turn0search27  
- INFO/統計を“shardローカル更新＋集計”へ寄せる設計メモを作り、ホット統計の共有ロックを外す（LongAdder的発想）。citeturn2search37turn4search14  
- ベンチ基盤を固定：redis-benchmarkとmemtier_benchmarkのコマンド、データサイズ、クライアント数、CPUピンニング方針を確定する。citeturn5search0turn5search1  
- フレームグラフ手順を固定：cargo-flamegraph（またはperf→flamegraph）で“mutex待ち/スケジューリング/転送”が見える状態にする。citeturn5search2turn5search37  
- 最初の比較対象を決める：coarse mutex現状 vs shard mutex（現状） vs actor専有（Phase A）で、どこが勝っているかを数字で出す。  
- “楽観fast path”候補を列挙し、適用条件（read-mostly、不変スナップショット、validate失敗でフォールバック）をテンプレ化する。citeturn0search5turn0search12  
- seqlock/seqcount（version検証）を適用できる“メタデータ最小セット”を定義する（例：設定スナップショット世代、統計世代）。citeturn0search12  
- MULTI/EXEC互換の論点（割り込み禁止、順序、跨shard時の扱い）を仕様として明文化し、Coordinator＋barrier案をチケット化する。citeturn4search7turn1search7  
- レプリケーション適用経路を「通常コマンドと同じowner-threadへ集約」する方針を文章化し、順序付け（単一ストリーム）とbarrierが必要な境界を洗い出す。citeturn4search2turn0search2  
- “Phase A完了条件”を定義：主要ホットパスからcoarse lockが消え、フレームグラフでmutex待ちの支配率が明確に低下したら次へ進む。