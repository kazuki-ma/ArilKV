# Garnet の高スループット要因分析レポート

## エグゼクティブサマリー

Garnet が Redis や Dragonfly を大きく上回るスループットを示した主因は、「**マルチコアを素直に使い切るセッション並列**」と「**共有メモリ上で“ルーティングしない”共有型ストレージ（Tsavorite）**」を核に、ネットワーク・プロトコル解析・ストレージ更新・クラスタ制御までを **“スレッド切替／割込み／割当” を極小化する形で一貫設計**している点にある（設計要約・図・表を含む一次資料）。citeturn13view1turn13view2turn20view3

観測された差は、条件付きだが極めて大きい。PVLDB 論文に掲載された「RESP キャッシュストア比較（Redis 正規化）」の表では、単一ノードのスループットが **Dragonfly 10× に対し Garnet 108×**、（特定条件の）バッチ 16 でのレイテンシが **Garnet 0.25×**など、複数指標で差が明示されている。citeturn13view1turn12view0

その差が出る技術的理由は大別すると次の通りである。第一に、Redis は「主にシングルスレッドでイベントループにより逐次処理する」設計で、I/O スレッド等の改善が入っても根本の逐次性が残りやすい（公式ドキュメントが明言）。citeturn33view3turn19view0turn19view2 第二に、Dragonfly は shared-nothing のシャーディングで垂直スケールするが、**高スレッド数・多数クライアント・大バッチ**では「受信→ルーティング→（シャード跨ぎ整列）→応答」のオーバーヘッドが本質的に発生し得る、と Garnet 論文は設計比較として整理している（また Dragonfly 自身も shared-nothing shards を採用していると説明）。citeturn13view1turn17view0

Garnet はこれに対し、(a) ネットワーク／解析／実行を **同一スレッド上で完結**させやすい「thread-switch-free」志向、(b) ストレージを latch-free・epoch 保護・メモリ再利用（revivification）で **共有しつつ高並列**に動かす Tsavorite、(c) FIFO セッションでのパイプライン応答、(d) さらに（プロトタイプだが）DPDK 等の **kernel bypass**まで含めた設計を採る。これらが合成され、CPU コアと NIC を早期に飽和させる。citeturn13view2turn13view4turn27view0turn15view1turn30view1

再実装可能性の見立てとして、Rust は「thread-per-core + io_uring + epoch reclamation」などで Garnet に近い設計写像が比較的自然だが、プロトコル互換・複雑型・運用機能まで含めると実装工数と検証が重い。Java/Kotlin（JVM）はオフヒープ（FFM）や Vector API 等で“GC を避ける方向”はあるが、低レベル最適化と JNI/FFI 境界コストが課題になる。WebAssembly は WASI ソケット等が前進している一方、現実的に DPDK／CPU affinity／低レベルメモリ制御まで同等に揃えるのが難しく、同等スループット狙いにはリスクが大きい。citeturn35search4turn35search1turn35search6turn35search3turn35search19

## 一次資料と調査方法

本レポートは、一次資料（論文・公式ドキュメント・公式ベンチマーク）を優先し、設計主張と測定条件が明記されているものを核に比較した。中心となる一次資料は次の通り。

- Garnet の主要一次資料：PVLDB 論文（設計・評価・比較表・設計スタディ・耐久性/レプリケーション/マイグレーション評価を含む）。citeturn12view0turn13view1turn13view4turn20view3  
  著者は entity["people","Badrish Chandramouli","database researcher"] ら entity["organization","Microsoft Research","research lab, redmond"] 所属である。citeturn13view0

- Garnet 実装ノート（公式）：ネットワーク層、処理層、Tsavorite のメモリ管理（バケット・ログ・SpanByte・オブジェクトストア）、ロッキング／epoch の扱い、revivification 等。citeturn27view0turn15view0turn30view1turn28view0turn15view1

- Garnet 公式ベンチマークページ（公式）：Resp.benchmark の条件、Redis/KeyDB/Dragonfly の起動オプション、負荷条件（例：4096 GET バッチでネットワークオーバーヘッドを抑える等）を明示。citeturn34view1turn15view3

- entity["company","Microsoft","technology company"] の研究ブログ（公式）：設計背景、採用領域、初期の性能プレビューと測定条件（Azure VM、比較対象バージョン、加速 TCP 等）。citeturn34view0

比較対象の一次資料は以下。

- Redis 公式ドキュメント：シングルスレッド性、I/O threading、永続化（fork + CoW）、レプリケーションの性質（既定は非同期・WAIT の限界）、クラスタの write safety 等。citeturn33view3turn19view0turn19view2turn33view2turn33view0turn33view1

- Dragonfly 公式：ベンチマーク手順（memtier_benchmark、分離配置、スレッド制御フラグ）、shared-nothing shards 採用、io_uring ベースのスレッド/I/O 管理ライブラリ参照、データ構造（Dashtable）・スナップショット方針・メモリ効率。citeturn16view1turn17view0turn18view0turn26view1turn26view0

なお、Garnet の論文内で比較対象に Valkey/KeyDB も含まれるため、必要に応じてそれらの位置づけも参照する（ユーザ要望の中心である Redis/Dragonfly との差分説明に従属させる）。citeturn20view3turn13view3

## ベンチマークで観測された差

Garnet の「高スループット」を語る際、まず重要なのは **測定ワークロードが“どのボトルネック（CPU/メモリ/NIC）を露呈させる設定か”**である。PVLDB 論文は、RESP 版 YCSB-A に近い負荷（大規模キー空間、GET/SET 比、payload、Zipf skew、バッチ、セッション数）を系統的に振り、さらに「クライアント（memtier）がボトルネックになり得る」点も明示している。citeturn20view3turn13view3

### 代表的な公式数値

以下は一次資料に明示された「差」を、条件付きで要約したものである。

- **単一ノード総合（Redis 正規化の比較表）**：Node Throughput は Dragonfly 10× に対し Garnet 108×、Checkpoint Speed は Dragonfly 59× に対し Garnet 257×、Scale-out Speed は Dragonfly 11× に対し Garnet 94×等。citeturn13view1  
  これは「Redis 互換キャッシュストアの複数機能を含む」比較指標で、単なる GET/SET だけでなく耐久性・スケールアウト等も含む点に注意が要る。citeturn13view1turn12view0

- **memtier_benchmark（1:9 SET:GET、256M keys、8B payload、6400 clients、128 threads）**：Garnet は Valkey より **少なくとも 20×高いスループット**、P99 レイテンシもより低い傾向とされる。citeturn13view3turn20view3  
  同条件群のスケールで Dragonfly は最も近い競合として言及されるが、Garnet に対して約 1.3×遅い（=スループットが低い）との記述がある。citeturn13view3turn20view3

- **GET スループット（クライアントセッション数増加）**：Garnet は Valkey より 108×高いスループット（log スケール図）という観測が記述され、Dragonfly は 16 スレッド程度までは伸びるが、128 clients 条件では Garnet より 10×低いスループットとの説明がある。citeturn13view3turn20view3turn34view1

- **ネットワーク飽和の観測**：remote cache-store として Garnet が「サーバ側メモリ帯域に当たる前に NIC を飽和させる」とし、iperf で 30Gbit/s を測定し、64B payload + batch 1024 で（RESP オーバーヘッド約 11%込みで）飽和した、という記述がある。citeturn13view3turn20view3

- **kernel bypass（DPDK/eRPC）**：99 パーセンタイルの PING レイテンシについて、DPDK による kernel bypass がさらに 4〜5×のレイテンシ低減をもたらす旨、および実験が「プロトタイプで open-source に含まれない」旨が明示される。citeturn13view4turn12view0

- **耐久性（チェックポイント）**：io_uring 等を用いたチェックポイントで Garnet が 1.06s、Dragonfly が 6.4s、Valkey/KeyDB が桁違いに遅い、という比較が論文本文に記載される。citeturn13view4turn12view0

### 「条件が差を増幅する」点（重要）

一次資料を読む限り、差を大きくしやすい条件は概ね次の組み合わせである。

- **多数クライアントセッション**（接続数・並列度を上げる）citeturn13view3turn34view1  
- **小さめ payload**（ネットワーク往復/パケット処理の相対コストが上がる）citeturn34view1turn13view3  
- **大きいバッチ**（ネットワークオーバーヘッドを意図的に下げ、サーバ側実装差を露出させる）citeturn34view1turn13view3  

とくに Garnet 公式ベンチマークページは「GET throughput の実験は 4096 バッチ・8B key/value でネットワークオーバーヘッドを最小化」と明言しており、これは「サーバ実装の純粋な処理能力・スケーリング」を見せる方向に寄る。citeturn34view1

## 技術要因の詳細分析

以下ではユーザ指定の技術次元を、「Garnet の選択」「Redis/Dragonfly との違い」「それがスループットを押し上げる因果」を明示して分析する。結論を先にまとめると、Garnet は **(i) 共有メモリに寄せた“ルーティング不要”設計で分割統治の手数を削り**、**(ii) 低レベル最適化（割当回避・インライン化・SIMD/intrinsics）で 1 op あたりの CPU コストを下げ**、**(iii) バッチ/パイプラインで NIC/CPU を飽和させる**ことで、スループット優位を作っている。citeturn13view1turn13view2turn20view3turn27view0

### 比較表（設計差分を次元別に要約）

| 技術次元 | Garnet | Redis | Dragonfly | スループットに効く理由（要点） |
|---|---|---|---|---|
| 並行モデル/スレッディング | 「shared-nothing session + shared-everything store」のハイブリッド。セッションは FIFO 前提でパイプライン応答。citeturn13view1turn13view2 | 主にシングルスレッドで逐次処理（multiplexing）。citeturn33view3 | shared-nothing shards（キー空間をスレッドに分割）。citeturn17view0 | Redis は根本的に逐次のため多コア飽和が難しい。Dragonfly は分割で伸びるが、ルーティング/整列等の“配達コスト”が増える局面がある。Garnet は共有ストアでこれを削る（論文が設計比較として説明）。citeturn13view1turn17view0turn33view3 |
| I/O モデル/ネットワークスタック | TCP に加え、eRPC を介した DPDK/RDMA など user-defined endpoint を設計に含め、thread-switch-free を志向。citeturn13view2turn13view4turn27view0 | Redis 6 以降 I/O threading。読み書きの重さを I/O スレッドへ逃がし得るが、根本の制約は残る（公式チュートリアル/リリース情報）。citeturn19view0turn19view2 | ベンチでは `--proactor_threads` 等でスレッド制御。設計背景として shared-nothing を支える I/O 管理ライブラリ（io_uring/epoll、fibers）を参照。citeturn16view1turn18view0turn17view0 | kernel bypass は主にレイテンシだが、パケット処理/システムコール削減で headroom 増。Garnet は“選択肢として持つ”設計。Dragonfly は io_uring 等で syscalls を減らす方向。Redis は I/O threading 改善でも段階的（最大~2×や ~112% 改善の言及）。citeturn13view4turn19view2turn18view0 |
| プロトコル解析/コマンド処理パイプライン | 予測的パーサ（最頻コマンドを優先し分岐を減らす）＋処理層で高速ディスパッチ、Network[Command] で引数解析→ストア呼び出し→RESP 返却。citeturn13view2turn15view0 | イベントループ上で逐次に read→parse→process→write。I/O threading で一部を他スレッドに委譲。citeturn19view0turn33view3 | shard へのディスパッチ・マルチキー原子性のための枠組み（VLL 参照）を採用。citeturn17view0turn18view1 | 高 QPS では「パース+ディスパッチ+コピー」の 1 op あたり固定費が支配的。Garnet は分岐削減・割当削減・インライン化で固定費を詰め、バッチで amortize。citeturn13view2turn15view0turn34view1 |
| メモリ管理/アロケータ | Tsavorite の main store は 64B バケット（キャッシュライン）＋ hybrid log（SpanByte）でキー/値を保持。revivification で tombstone 等を再利用。citeturn30view1turn15view1 さらに C# 実装で GC を避け pointer/SIMD を使う意図を明言。citeturn20view3 | 既定で fork+CoW を使う永続化があり、CoW と巨大ページ等でレイテンシ/メモリ面の課題が起こり得る（公式が説明）。citeturn33view2turn33view3 既定アロケータは jemalloc（ARM では既定が異なる旨の言及）。citeturn3view2 | Dashtable（セグメント分割・open addressing・stash・分割成長）や、スナップショットで fork を避ける設計を提示。citeturn26view0turn26view1turn17view0 | high-QPS は「キャッシュミス」「割当/解放」「ロック待ち」「CoW 由来のノイズ」が効く。Garnet は log+再利用+キャッシュライン設計で局所性と alloc 圧を下げる。Dragonfly はデータ構造面で効率化するが、shared-nothing の制御/ルーティング負荷が別途発生し得る。citeturn13view1turn30view1turn26view1turn17view0 |
| ロック/ロックフリー/epoch | Tsavorite は LockTable/epoch 管理を複数モードで持ち、CAS スピン＋短いスピンでリトライ、Context を struct 化してインライン化を狙う。citeturn28view0turn28view1 クラスタ状態遷移にも epoch を適用。citeturn30view0 | シングルスレッド性が“ロック不要”に寄与する一方、並列化しにくい。citeturn33view3 | マルチキー原子性のため VLL を参照し mutex/spinlock を避ける設計と説明。citeturn17view0turn18view1 | 多コアで伸ばすには、共有構造の同時更新を「低コストな同期（epoch/CAS）」で支える必要がある。Garnet はストア内部へ競合を“囲い込み”并列化し、Dragonfly は shared-nothing + 取引枠組みで避ける。citeturn13view1turn28view0turn17view0 |
| 永続化/耐久性 | tunable durability（DOL + 非ブロッキング checkpoint）を設計要素に含め、io_uring 等で高速 checkpoint を示す。citeturn13view1turn13view4 | RDB/AOF は fork + CoW を利用。citeturn33view2 | fork を避けるスナップショット機構（versioning/update hooks）を提示。citeturn26view1 | fork は CoW によるメモリ/レイテンシスパイクを生み得る。Redis はその性質を公式が説明。Dragonfly は回避策を主張。Garnet は非ブロッキング checkpoint を中核最適化として数値で示す。citeturn33view2turn26view1turn13view4 |
| レプリケーション/整合性 | クラスタは leader-follower の非同期複製で log shipping（AOF）＋ checkpoint 連携。性能-耐久性トレードオフ（FAT/ODC 等）を明示。citeturn31view0 | 既定は非同期複製。WAIT で“確率を下げる”が強整合にはならないと公式が明記。citeturn33view0 | 公式 README では複製は設計中の旨を含む（時点依存）。citeturn17view0turn16view1 | スループットと整合性/耐久性はトレードオフ。Garnet は“どの程度待つか”を設計に組み込み、Redis は WAIT の限界を公式に記述。citeturn31view0turn33view0turn15view4 |

### 主要因を「なぜ速いか」の因果で掘る

**並行性：セッション並列 + 共有ストアで“ルーティングしない”**  
Garnet 論文は「Redis（単一ワークキュー）」「KeyDB（ロック）」「コア毎キュー（分割）」など“典型解”を整理し、コア毎キュー方式が「受信パケット検査」「バッチ分解」「所有ワーカーへルーティング」「応答整列」のコストを抱えると述べる。citeturn13view1 これに対し Garnet は「shared-nothing session が shared-everything なデータストア上で動く」ハイブリッドを採り、共有メモリではキャッシュコヒーレンスで“データをスレッドへ持ってくる”という発想で、ルーティング型の固定費を避ける。citeturn13view1turn13view2  
この差は、設計スタディで「別設計に置き換えるとスループットが桁で落ちる」形で裏取りされている（例：Garnet 本体 47M ops/s に対し、代替設計は数 M ops/s 程度の記述）。citeturn13view4turn12view0

**プロトコル解析：予測的パーサと高速ディスパッチで 1 op 固定費を削る**  
RESP はテキスト的で柔軟だが、高 QPS では「分岐」「整数パース」「境界チェック」「コピー」が効く。Garnet は “optimized predictive parser” により分岐を減らすとし、処理層でもまずコマンド種別を高速同定して処理関数へ落とす作りを公開している。citeturn13view2turn15view0  
Redis も I/O threading で socket read/write（近年は command parsing も含めるとする説明）をスレッドへ委譲できるが、公式ドキュメント上は「mostly single threaded design」で逐次処理する性質が明記され、コア数を素直にスループットへ変換しにくい面が残る。citeturn33view3turn19view2

**メモリ：キャッシュラインとログ構造、そして“再利用”**  
Garnet の main store は「64B バケット（=cache line）」「7 エントリ + overflow」「キー/値は hybrid log に格納」「SpanByte で保持」と説明される。citeturn30view1 これはデータパスのキャッシュ効率に直結し、特に小 key/value で大量アクセスするワークロード（論文・公式ベンチの中心条件）に合う。citeturn34view1turn20view3  
さらに Tsavorite の revivification は tombstone レコード等を再利用して log growth を抑えるとされ、削除や更新が多いケースでメモリ再利用が効く。citeturn15view1  
C# 実装についても「GC を避け、ポインタ操作や SIMD を使える必要があった」と論文が明言しており、管理ランタイムの弱点（割当・GC）を“設計で潰す”姿勢が読み取れる。citeturn20view3turn15view3

**バッチ/パイプライン：NIC を飽和させる設計（＝CPU を遊ばせない）**  
論文は「64B payload + batch 1024 で 30Gbit/s を飽和」「RESP オーバーヘッド約 11%」と述べ、remote cache-store のボトルネックが NIC に移る局面を示す。citeturn13view3turn20view3  
公式ベンチでも、GET throughput に大バッチ（4096）を使いネットワーク負荷を抑える実験設計を明言しており、バッチ/パイプラインが“差を見せる”鍵になっている。citeturn34view1  
Dragonfly もパイプラインモードで 10M/15M QPS（SET/GET）に達する旨を README に記載しており、バッチが伸びの源泉である点は共通する。citeturn17view0 それでも Garnet が上回る条件があるのは、上記の「ルーティング固定費削減」「低割当・低分岐」等が合成されるためと解釈できる。citeturn13view1turn13view3turn15view0

**永続化/チェックポイント：fork を避け、非ブロッキングで高速化**  
Redis の永続化は、RDB/AOF ともに fork と CoW を使うことが公式に説明される。citeturn33view2turn33view3 これは実装が単純で速い局面がある一方、書込みが多いほど CoW でメモリ増やレイテンシスパイクを引き起こし得る（公式が latency issue として説明）。citeturn33view3  
Dragonfly は fork 回避（versioning/update hooks による）を利点として掲げる。citeturn26view1  
Garnet はこれをさらに「非ブロッキング checkpoint + DOL（deterministic operation log）」として体系化し、チェックポイント時間の比較で優位を示す。citeturn13view4turn13view1

## Java/Kotlin・Rust・WebAssembly での再実装可能性

ここでは「Garnet“風”の成果（多数接続・大バッチ・小 payload で NIC/CPU を飽和）を狙う」観点で、必要な要素を分解し、各言語の実装上の難所と推奨アーキテクチャを示す。前提として Garnet 本体は C# だが「GC 回避・ポインタ・SIMD」まで使うことを明言しており、“普通のマネージド実装”では届かない領域をターゲットにしている。citeturn20view3turn27view0

### 共通して必要になる設計要素（言語に依らない）

- **スレッド並列はセッション単位で伸ばし、共有ストアは epoch/CAS 等で支える**（shared-nothing shards か shared-everything store かは選択だが、どちらも同期コストが支配）。citeturn13view1turn28view0turn17view0  
- **プロトコル解析は“分岐と割当”を最小化し、コマンドディスパッチを高速化**。citeturn13view2turn15view0  
- **大バッチ/パイプラインで NIC を飽和**させ、クライアント側ボトルネックを潰す（Garnet は memtier の限界も述べる）。citeturn13view3turn34view1  
- **永続化/スナップショットは fork/CoW の副作用を避けるか、少なくともワークロード隔離を行う**。citeturn33view2turn26view1turn13view4  

### 言語別：リスク/ベネフィットと推奨アプローチ

#### Java/Kotlin（JVM）

| 観点 | ベネフィット | リスク/課題 | 推奨アプローチ |
|---|---|---|---|
| GC/ヒープ | 豊富なエコシステム、JIT 最適化、成熟した運用。 | Garnet が“GC 回避”を前提にしている以上、素朴なヒープ実装は tail latency と throughput に不利になりやすい（設計意図）。citeturn20view3 | 主要データパスは **オフヒープ**化。JDK 22 で安定化した Foreign Function & Memory API を軸に（JNI 依存を減らしつつ）ネイティブメモリを扱う。citeturn35search4turn35search16 |
| SIMD/ハッシュ高速化 | Vector API でデータ並列を表現でき、SIMD を狙える。citeturn35search1turn35search13 | Vector API は（少なくとも JDK 23 時点で）インキュベータであり、互換性/採用に注意。citeturn35search1turn35search5 | ハッシュやバイト列比較などホットスポットに Vector API を局所導入（フォールバックあり）。citeturn35search1 |
| ネットワーク | 高性能な NIO/Netty 系が利用可能。 | kernel bypass/DPDK はほぼ FFI 必須。FFI 境界がホットパスに入ると難しい。citeturn35search4turn13view4 | まずは **epoll/NIO + バッファプール**で徹底的に割当削減。DPDK は“別製品線（超低レイテンシ版）”として切り出す。citeturn27view0turn13view4 |
| 期待性能 | “十分最適化すれば”高い可能性。 | Garnet 並の「多セッション×小 payload×大バッチでの極限値」は、オフヒープ + 低分岐パーサ + CAS/epoch まで揃えないと厳しい。citeturn13view2turn28view0turn20view3 | ストアは off-heap log + epoch reclamation（自前）に寄せ、API は RUMDS 的な“狭い胴体（narrow waist）”で拡張容易性と性能の両立を狙う。citeturn13view1turn28view1 |

#### Rust

| 観点 | ベネフィット | リスク/課題 | 推奨アプローチ |
|---|---|---|---|
| メモリ/アロケータ | 所有権で安全に低レベル実装可能。オフヒープ/アリーナ等も自在。 | 複雑データ型・トランザクション・クラスタ機能まで含めると実装/検証が重い（Garnet は広い機能範囲を論文で扱う）。citeturn12view0turn31view0 | まずは「raw string + 限定コマンド」から始め、RUMDS 相当の内部 API を固定して上層を増やす（論文の構造に倣う）。citeturn13view2turn12view0 |
| I/O（io_uring） | tokio-uring は io_uring を Tokio 互換の runtime として提供し、カーネル 5.10+ 要件を明記。citeturn35search6turn35search18 | thread-per-core になりやすく、負荷偏り（長い future）で不均衡が出ることがあるという注意もコミュニティで語られる。citeturn35search22turn35search26 | work stealing なし前提なら「コア毎シャード（Dragonfly 型）」のほうが素直。共有ストア（Garnet 型）なら crossbeam-epoch 等で epoch を確立。 |
| kernel bypass | C/DPDK 連携が比較的容易。 | DPDK は運用要件（専用 NIC/ドライバ/権限）やデバッグ難度が高い。Garnet でも open-source に入らないプロトタイプ扱い。citeturn13view4turn13view2 | まず TCP 経路で性能を出し、DPDK はオプションとして分離（ビルド/実行時に切替）。citeturn13view2turn13view4 |
| 期待性能 | “最も近い”到達可能性。 | ただし、Garnet が示す多数の最適化（re-vivification、インライン化、設計スタディ等）を地道に積み上げる必要。citeturn15view1turn28view1turn13view4 | 設計スタディ同様、複数アーキテクチャ（共有/分割）をベンチで比較し、勝てる負荷領域を確定してから機能を増やす。citeturn13view4turn34view1 |

#### WebAssembly（WASM）

| 観点 | ベネフィット | リスク/課題 | 推奨アプローチ |
|---|---|---|---|
| 砂箱/配布 | 依存をまとめやすく、実行環境分離に強い（一般論）。WASI による機能拡張が進行。citeturn35search19 | TCP/UDP ソケットは WASI proposal として進むが、ホスト能力（capability）前提で、実装・成熟度は環境依存。citeturn35search3turn35search19 | ネットワークは WASI sockets を前提に、まずは「サイドカー/エッジの軽量キャッシュ・プロキシ」用途から。citeturn35search3 |
| 低レベル最適化 | 一部 SIMD 等は可能（環境依存）。 | DPDK/CPU affinity/巨大ページ/ユーザ空間 NIC 等の“素の性能”に直結する機能は、少なくとも標準 WASI だけで同等に揃えるのが難しい（現状は不確実）。citeturn13view4turn35search19 | 「Garnet 同等スループット」を目的にするより、ホスト側（ネイティブ）に高速データパスを置き、WASM は拡張ロジック（例：ストアド手続き）へ限定。citeturn20view3turn31view2 |
| 期待性能 | “ベンチの中心領域（NIC 飽和）”は難易度が高い。 | WASI の成熟やランタイム差が大きく、再現性確保が難しい。citeturn35search19turn35search3 | 高速 KV 本体ではなく、**拡張・隔離実行**を狙うのが現実的（Garnet も拡張点を持つ）。citeturn15view4turn31view2 |

## 検証のための実験計画とベンチマーク設計

一次資料の数値を「自分の環境で再現し、何が支配的か」を確かめるための実験計画を提示する。重要なのは (a) client ボトルネック除去、(b) 開ループ/閉ループの区別、(c) CPU/NIC/メモリのどれが飽和しているかの観測、の3点である。citeturn13view3turn34view1turn16view1

### 推奨テストハーネス

- **負荷生成**：memtier_benchmark を基本にしつつ、極限スループットではクライアントが詰まる可能性があるため、Garnet 論文が採用した軽量ツール（Resp.bench 系）か、同等に「事前生成コマンド」「バッファ再利用」「ワーカ固定」を備えた自作ツールを準備する。citeturn13view3turn15view3turn34view1
- **配置**：Dragonfly 公式が推奨するように、サーバとクライアントは別マシン、同一 AZ/低レイテンシ網、クライアント側を強くして client 側限界を押し上げる。citeturn16view1
- **実行条件の固定**：CPU 固定（taskset/numactl 等）、THP 無効、バックグラウンド fork 系タスクのコア隔離など、Redis 公式が latency issue として列挙する OS チューニングを、再現性のために“明示的に”管理する。citeturn33view3

### ワークロード行列（Garnet の差が出やすい条件を中心に）

| 目的 | 推奨負荷 | 観測指標 |
|---|---|---|
| 多数接続スケーリング | keyspace 256M、8B key/value、GET 100% と 1:9 SET:GET、セッション 1→128（以上）、バッチ 1/16/64/1024/4096 | ops/s、NIC throughput、CPU util、p50/p99/p99.9、instructions/op、cache-miss、syscalls/op citeturn20view3turn34view1 |
| バッチ効果/パイプライン限界 | バッチを段階的に増加（特に 1→4096）。payload も 64B→2KB へ掃引 | ops/s が NIC 飽和で頭打ちする点、レイテンシ分布の崩れ（tail）citeturn13view3turn34view1 |
| skew（競合）耐性 | Zipf（例：α=0.9）で SET 100% 等、ホットキー/ホットスロットを作る | lock 競合、リトライ率、オーバーフローバケット/ログ成長、tail latency citeturn13view3turn15view1turn30view1 |
| 複雑型（オブジェクト） | ZADD/ZREM、PFADD 等（Garnet は intrinsics を言及） | ops/s とスケール、CPU カウンタ（SIMD 使用時の効果）citeturn13view3turn20view3 |
| 耐久性/スナップショット | 256M keys など大規模ロード→checkpoint（RDB/AOF/Dragonfly snapshot） | checkpoint 時間、書込帯域、クエリ遅延のスパイク、メモリ増（CoW）citeturn13view4turn33view2turn26view1 |
| 複製/移行 | セカンダリ数・移行バッチを掃引 | write throughput 低下率、replication lag、移行時間、エラー率 citeturn13view4turn31view0turn30view0 |

### アーキテクチャ理解のための図（Mermaid）

#### リクエスト処理フロー（Garnet の“固定費削減”ポイントを可視化）

```mermaid
flowchart LR
  C[Client] -->|RESP batch / pipeline| N[Network receive buffer]
  N --> P[Predictive parse & fast dispatch]
  P --> H[Network[Command] handler\n(arg parse + RESP encode)]
  H --> S[Tsavorite session API\nRUMDS: Read/Upsert/Modify/Delete/Scan]
  S -->|shared-everything store\n(log + index)| M[(Memory)]
  H --> O[Send buffer pool\n(throttled async send)]
  O --> C
```

このフローは一次資料に書かれた「予測的パーサ」「Network[Command]」「RUMDS」「共有ストア」「送信バッファ再利用」等に対応している。citeturn13view2turn15view0turn27view3turn12view0

#### 設計選択の比較（“ルーティング負荷”の有無）

```mermaid
flowchart TB
  subgraph Redis_style[Redisの典型像]
    R1[Single main event loop] --> R2[Parse/Execute/Reply\nmostly sequential]
  end

  subgraph Dragonfly_style[Dragonflyの典型像]
    D1[Shard per thread\n(shared-nothing)] --> D2[Route to shard]
    D2 --> D3[Execute in shard]
    D3 --> D4[Order/aggregate replies\n(if needed)]
  end

  subgraph Garnet_style[Garnetの典型像]
    G1[Many sessions\n(shared-nothing)] --> G2[Direct call into\nshared-everything Tsavorite]
    G2 --> G3[Cache-coherence brings data\n(no per-op routing)]
  end
```

“設計の固定費”について、Garnet 論文はコア毎キュー/シャーディングが抱えるルーティング・整列コストを明示し、Garnet はそれを避ける方向で設計したと述べる。citeturn13view1turn13view2 Dragonfly は shared-nothing shards 採用を自ら説明する。citeturn17view0 Redis は mostly single-threaded を公式に説明する。citeturn33view3

### クリティカル部の疑似コード例（割当削減・ロックフリー志向）

#### セッション内処理ループ（FIFO + バッチ）

```pseudo
on_network_receive(bytes buf):
  while buf.has_data():
    cmd = fast_parse_next_command(buf)        // minimize branches/copies
    args = parse_args_in_place(buf, cmd)
    result = dispatch(cmd, args)              // direct call to storage API
    encode_resp(send_buffer_pool.get(), result)
  flush_send_buffers_with_throttle()
```

この形は「TryConsumeMessages/ProcessMessages」「Network[Command] で引数解析→ストア呼び出し→RESP 書き込み」の説明に沿う。citeturn15view0turn27view3turn12view0

#### epoch/CAS を使う“短スピンでリトライ”型ロック（概念）

```pseudo
try_lock(slot):
  for i in 1..SPIN_LIMIT:
    if CAS(slot.state, UNLOCKED, LOCKED):
       return OK
    cpu_pause_or_yield()
  return RETRY_LATER
```

Tsavorite のロックは `Interlocked.CompareExchange` と `Thread.Yield()` でのスピンと、スピン制限によるリトライを説明している（実コードは C# だが概念は同型）。citeturn28view0

## 前提・未確定事項と注意点

- **バージョン/設定の不確定**：ユーザから「どの Garnet バージョン・どの NIC・どのカーネル・TLS 有無」等の指定がないため、本レポートでは一次資料で明示された代表条件（PVLDB 論文、公式ベンチページ、MSR ブログ）を“基準”として扱った。citeturn20view3turn34view1turn34view0

- **DPDK/eRPC はプロトタイプ扱い**：論文は kernel bypass（DPDK/eRPC）評価を示す一方、open-source には含まれないプロトタイプであると明記しているため、実務導入では再現可能性・運用性を別途評価すべきである。citeturn13view4turn12view0

- **“高スループット=常に優”ではない**：remote cache-store の多くは payload や RTT により NIC/ネットワークがボトルネックになり、スループット差が縮む領域がある。Garnet 論文も payload 増ではネットワークが支配的になる旨を示唆している。citeturn13view3turn34view1

- **Redis/Dragonfly の主張は用途が異なる場合がある**：Redis は機能・互換・運用生態系が広く、Dragonfly は shared-nothing とデータ構造最適化で“少ない資源での高性能”を前面に出す。性能比較の公平性は、バッチ・キー空間・永続化設定・クライアント能力に強く依存するため、上記の実験計画のように条件を系統立てて検証する必要がある。citeturn26view1turn33view3turn16view1turn13view3