# Kernel-Path Reduction for GET/SET Hot Path（Redis互換KV・Rust）深掘り調査

## Executive Summary

- 小さな `GET/SET`（tiny request/response）では、アプリ側の処理よりも **(a) `recv*`/`send*` 系システムコール** と **(b) wakeup/スケジューリング/割り込み処理（NAPI/softirq）** が支配的になりやすく、プロファイルで `sendto/recvfrom` が目立つ状況は「観測として自然」です（`send()` と `sendto()` の等価関係も含む）。citeturn4search16  
- まず検証すべきは「**1リクエストあたりの“境界回数”を減らす**」です。具体的には **(1) 送信の小分けをやめて `writev()` 等で集約**、**(2) 受信も“読めるだけ読んで”複数コマンドを1回のreadで回収**、**(3) イベント駆動なら余計なwakeupsを減らす**。`readv/writev` は scatter/gather を 1 syscall にまとめる基本手段です。citeturn17search3  
- 低リスクの「オプション変更」では、Redis自身が採っているように **`TCP_NODELAY` を前提**にしつつ（遅延を避ける）、「小さい書き込みを乱発しない」ことが重要です。citeturn3search3turn3search22  
- `TCP_NODELAY` は **レイテンシ改善の定番**ですが、アプリが小さな書き込みを大量に行うとパケット化が増え、**スループット・CPU効率を悪化させ得る**ため、**“送信集約”とセット**で扱うのが現実的です。citeturn3search22turn1search5  
- 「wakeup/スケジューリングが重い」系の観測には、**busy polling（`SO_BUSY_POLL` + `net.core.busy_poll/busy_read`）**が効く場合があります。割り込みとコンテキストスイッチ由来の待ちを減らせますが、**CPU・電力コストが増える**ため、専用コア/専用ホストに寄せた評価が必須です。citeturn15search9turn9search0turn15search6  
- `epoll` vs `io_uring` は、単に “poll置換” するだけでは伸びが限定的になり得ます。`io_uring` の価値は **バッチ提出**や **completionモデル**、さらに **`IORING_SETUP_SQPOLL` による syscall/コンテキストスイッチ削減**で出ます（ただしケースバイケースで検証せよ、と公式に釘が刺されています）。citeturn13view0turn14view1  
- `MSG_ZEROCOPY` は「小さい応答」には基本的に不利です。Linuxカーネル文書で **概ね 10KB 超の書き込みでないと効きにくい**（ページピンや完了通知のオーバーヘッドが勝つ）と明言されています。tiny-value GET/SET のホットパス最適化としては **原則“後回し/回避寄り”**です。citeturn2search2turn4search8  
- `SO_REUSEPORT` は accept の分散や競合低減の有力手段ですが、**単一スレッド/単一コアの測定**では本質的に恩恵が出にくい領域です（スケール時の選択肢として “後半フェーズ” が妥当）。citeturn4search14  
- AF_XDP / DPDK 等の kernel-bypass は、パケット処理では大きな性能余地がありますが、Redis互換TCPサーバに当てるには **“TCP（＋TLS等）のユーザ空間実装/統合”**が必要になりがちで、実装・運用負担が跳ね上がります。AF_XDP はゼロコピー/コピーモード切替やドライバ要件が明確で、DPDKは割り込みを使わないポーリング前提です。citeturn7search2turn7search0  
- “低リスクで始める”なら、**(A) 送受信の syscall 回数と wakeup を減らす** → **(B) busy poll / NAPI連携でレイテンシを詰める** → **(C) `io_uring`（特にSQPOLLやmultishot）でモデル自体を見直す** → **(D) それでも足りない場合のみ bypass**、の順が「gain/complexity」の観点で最も堅いです。citeturn14view1turn2search1turn7search0  

## Technique Matrix

| Technique | Expected gain (throughput/p99 range, if known) | Prerequisites | Complexity | Ops risk | Portability | Adopt now / later / avoid |
|---|---|---|---|---|---|---|
| `TCP_NODELAY`（Nagle無効） | p99改善に寄与しやすい一方、**小さいwrite乱発だとスループット悪化**し得る（数値はワークロード依存）。citeturn3search3turn3search22 | サーバ側 setsockopt、クライアント側も同様が望ましい | 低 | 低（ただし“送信集約”不足で回線/CPU悪化）citeturn3search22 | 高（BSD/macOSも概ね可）citeturn12search19 | Adopt now |
| 送信集約（1応答=1回の`write`/`send`、または `writev()`） | syscall減・パケット減で throughput と p99 の両方に効くことが多い（特に tiny 応答）。`writev()`で複数バッファを1回で書ける。citeturn17search3 | 応答生成をiovec化、またはユーザ空間バッファリング | 低〜中 | 低 | 高（POSIX）citeturn17search3 | Adopt now |
| `MSG_MORE`（送信側 cork ヒント）/ `TCP_CORK` | “小分けwrite”が避けられない場合にパケット合成を促す。`MSG_MORE` は `TCP_CORK` 相当を呼び出し単位で指定可能。citeturn4search0turn12search23 | 送信が分割される実装で、最後のフラッシュ点を明確化 | 中 | 中（フラッシュ忘れ/遅延でp99悪化）citeturn4search0 | 中（Linux依存色）citeturn4search0 | Adopt later（“分割送信が避けられない”場合のみ） |
| ソケットバッファ調整（`SO_SNDBUF/SO_RCVBUF` + `rmem_max/wmem_max` 等） | バッファ不足由来の損失/待ちを減らす可能性。設定値が **kernelで倍に見える**点に注意。citeturn15search2turn15search6 | sysctl許可、アプリ側 setsockopt（または敢えてしない） | 低 | 中（メモリ増、バッファ膨張でtail悪化リスク） | 高（macOSも類似概念） | Adopt now（ただし計測前提で慎重に） |
| `somaxconn` 等の backlog（接続 churn がある場合） | 接続集中時の accept backlog 溢れ回避。Linuxでは `somaxconn` の説明が明確。citeturn15search1 | 接続が頻繁に張り直される/短命コネが多い場合 | 低 | 低 | 中〜高 | Adopt now（接続 churn が測定で確認できる場合） |
| busy polling（`SO_BUSY_POLL` + `net.core.busy_poll/busy_read`） | 割り込み/スケジューリング待ちを減らし低レイテンシを狙う。CPU使用と電力増、適用ソケット数は絞るのが前提。citeturn15search9turn9search0turn15search6 | 対象ソケットで `SO_BUSY_POLL`、sysctl調整、CAP_NET_ADMIN要件あり得るciteturn9search0 | 中 | 中〜高（CPU常時消費、同居ワークロードへ影響）citeturn15search9 | 低〜中（Linux特有） | Adopt later（専用コア/専用ホストなら前倒し可） |
| epoll-based busy polling + NAPI ID整列（`SO_INCOMING_NAPI_ID` 等） | `epoll_wait` が NAPI処理を駆動しうる設計で、レイテンシ低減やキャッシュヒット改善の可能性が示されている。NAPI ID整列が要件。citeturn2search1turn8search10 | NAPI IDが揃うepollコンテキスト設計、受け渡し手順が必要citeturn8search10 | 高 | 高（設計制約、NIC複数構成で難化）citeturn4search18turn5search13 | 低（Linux特有） | Adopt later（“p99が最重要”かつ運用制約を飲める場合） |
| `SO_INCOMING_CPU`（ソケットのCPU親和性） | 受信処理/アプリ処理のCPU局所性改善の余地。ただし効果は NIC/RSS/スケジューラ設定次第。`SO_INCOMING_CPU` は socket option として定義。citeturn8search13turn9search0 | スレッドpin、可能ならIRQ/RSSと整合 | 中 | 中（設定不整合で逆効果） | 低（Linux依存） | Adopt later |
| `SO_REUSEPORT` でlistener sharding | accept分散をカーネル内で行い、単一acceptorのボトルネックや競合を減らす設計。マニュアルで“multi-thread serverのaccept分散改善”が明示。citeturn4search14 | 複数リスナー/複数ワーカー設計、（必要なら）reuseport BPF | 中 | 中（偏り・デバッグ難） | 低〜中（Linux/BSDで挙動差）citeturn4search31 | Adopt later（マルチコア段階） |
| `EPOLLEXCLUSIVE` / `EPOLLONESHOT` 等（thundering herd 抑制） | 多スレッド待機での無駄wakeを抑える目的。`EPOLLEXCLUSIVE` が thundering herd 回避として言及。citeturn4search24turn8search15 | epoll複数待機/accept分散設計 | 中 | 中 | 低（Linux固有拡張強め） | Adopt later（多acceptor/多workerの場合） |
| `io_uring`（バッチ提出） | 複数操作を1 syscallにまとめられる設計が中核。`io_uring_enter` が submit+wait を同時に行える。citeturn2search21turn13view0 | `io_uring` ランタイム/バックエンド実装、リング管理 | 高 | 中（カーネル依存/成熟度差） | 低（Linux専用）citeturn2search15 | Adopt later（Phase2で検証） |
| `io_uring` + `IORING_SETUP_SQPOLL`（syscall削減） | カーネルスレッドがSQをポーリングし、**syscallなしでsubmit/reap可能**という設計（ただし idle 時は wakeup が必要）。ケースバイケースで評価せよと明記。citeturn14view1turn14view2 | Linux新しめ、SQPOLLスレッド運用、（必要なら）CPU固定`IORING_SETUP_SQ_AFF`citeturn14view2 | 高 | 高（CPU常時コスト/運用難、コンテナ制限も要注意） | 低（Linux専用） | Adopt later（明確な勝ち筋が出たら） |
| `io_uring` multishot recv | multishot受信は **kernel 6.0以降**で提供され、再サブミット削減が狙い。citeturn1search19turn1search28 | Linux 6.0+、provided buffers等の運用 | 高 | 中 | 低（Linux専用） | Adopt later（Phase2の中でも“勝てそうなら”） |
| `MSG_ZEROCOPY`（送信ゼロコピー） | **一般に 10KB超のwriteで有効**、小さいwriteではページピン/通知コストが勝つ。tiny応答では悪化しやすい。citeturn2search2turn4search8 | `MSG_ZEROCOPY` + `SO_ZEROCOPY` 等、完了通知（error queue）処理 | 高 | 高（バッファ寿命制約、複雑化）citeturn2search2 | 低（Linux特有） | Avoid（tiny GET/SET では原則） |
| AF_XDP | ドライバが対応すれば bind 時に zero-copy を試み、不可なら copy にフォールバック。ユーザ空間リングで受けることでネットワークスタックを迂回。citeturn7search2turn7search0 | NIC/ドライバ対応、XDP/AF_XDP運用、TCPをどうするかが巨大課題 | 非常に高 | 非常に高（運用制約・観測/デバッグ負担） | 低（Linux専用） | Adopt later（“bypass track”のみ、正当化できる場合） |
| DPDK（kernel bypass） | PMDが **割り込み無しでRX/TX descriptorをポーリング**する設計で高スループット狙い。citeturn7search0turn7search4 | hugepages、CPU占有、デバイスバインド等 | 非常に高 | 非常に高（デプロイ/運用重）citeturn7search4 | 低（Linux中心） | Avoid〜Later（要求が“2×以上”で初めて検討） |

## Phase Plan

**Phase 0: option-only experiments（コード最小、低リスク/高確度から）**

前提として、Redis互換の tiny GET/SET の場合、Redis本体が「クライアントソケットを non-blocking にし、multiplexing を使い、`TCP_NODELAY` を設定する」と明記しています。ここは “互換系KV” のベースラインとして揃えてから比較する価値が高いです。citeturn3search3  

- **`TCP_NODELAY` の有無と、“送信が分割されていないか”の確認**  
  `TCP_NODELAY` 自体は簡単ですが、Red Hatの資料でも「小さい論理的に関連する書き込みを避けないとパフォーマンスが低下」と明記されています。したがって Phase 0 では、まず **“オプションだけ変えて”** 伸びるかを見る前に、**現状の送信が1レスポンス1writeか**を観測（後述のMeasurement Protocol）し、分割送信なら Phase 1 へ優先度を上げます。citeturn3search22  

- **ソケットバッファと sysctl の“安全域”探索**  
  `SO_SNDBUF/SO_RCVBUF` は kernel 内部都合で倍に見える点、最大値は `rmem_max/wmem_max` に制限される点が man page と kernel sysctl 文書で明示されています。設定は “段階的に” 行い、p99が悪化しない範囲を探すのが現実的です。citeturn15search2turn15search6  

- **busy polling の可否とコスト測定（短時間・専用環境）**  
  busy polling は割り込み/コンテキストスイッチ由来の遅延低減を狙えますが、CPU増・電力増が前提です。Red Hatは `busy_poll/busy_read` と `SO_BUSY_POLL` の併用手順・推奨値イメージ（例: 50〜100）に触れています。まず **“p99がどれだけ動くか”** を最短で見て、勝てないなら撤退します。citeturn15search9turn9search0  

- **接続 churn がある場合のみ backlog（`somaxconn` 等）**  
  `somaxconn` は listen backlog の上限として kernel docs に定義があります。接続張り直しが多いベンチ（短命コネクション）なら Phase 0 で試験、長期コネ主体なら優先度を下げます。citeturn15search1  

**Phase 1: minimal code changes（小さなパッチで“syscallとwakeups”を減らす）**

このフェーズは「モード/オプションだけでは削れないが、アーキを壊さずに効く」領域です。

- **送信の“バッファ分割”をやめる（最優先）**  
  RESP生成が「ヘッダ→値→CRLF」など分割されていると、`TCP_NODELAY` 下で tiny packet が増えやすく、CPU/IRQ/softirq も増えます。`writev()` は複数バッファを1回で書くことができ、syscall回数と分割送信を減らす定石です。citeturn17search3turn3search22  

- **`MSG_MORE`（または `TCP_CORK`）で“分割送信を吸収”する（必要な場合のみ）**  
  どうしても分割送信が避けられない箇所に限定して、`sendmsg()` の `MSG_MORE` を使い、最後だけフラグ無しで flush する設計が選択肢になります。`MSG_MORE` は `TCP_CORK` 相当を per-call で指定可能と man page に明記されています。citeturn4search0  
  ただし flush 忘れや、想定外の遅延が p99 を壊しやすく、採用は “限定的” が安全です。citeturn4search0  

- **受信のreadループを “readableなら読み切る” & 1回の受信で複数コマンド回収**  
  readiness 型I/Oでは、readable イベントのたびに1回だけ読むと、イベント回数と wakeup が増えます。ここはプロトコルパーサが軽いなら特に「読み切ってまとめて処理」が効きやすい領域です（ベンチで syscall 数/req が落ちることを必ず確認）。  

- **accept周りの軽量化（接続 churn がある場合）**  
  `accept4()` の `SOCK_NONBLOCK` は “受理と同時に non-blocking 化”でき、余計な `fcntl()` 呼び出しを減らせます（Linux man page に明示）。citeturn17search5  

**Phase 2: moderate architecture updates（イベントモデルを変えて“wakeup/ctxswitch”を削る）**

ここからは “実装難度は上がるが、カーネルパス削減の本丸” です。maintainability を考えると、**既存 epoll バックエンドを残した上で io_uring バックエンドを追加**し、切替可能にして比較する設計が堅いです（実装差分を閉じ込める）。

- **`epoll` → `io_uring`：単なる notifier 置換ではなく、completion/batching を使う**  
  `io_uring` メンテナ（Jens Axboe）の解説でも「epoll notifier を io_uring notifier に置き換えるだけでは io_uring の利点を十分に引き出せない。利点を使うには event loop の変更が必要」と明記されています。citeturn13view0  

- **`IORING_SETUP_SQPOLL` の評価（syscall削減・ただし運用重）**  
  `IORING_SETUP_SQPOLL` は「カーネルスレッドが submission queue をポーリングし、アプリが syscall なしで submit/reap できる」と man page が説明します。一方で idle 時の wakeup 条件や、ケースバイケース評価の注意が明確に書かれています。citeturn14view1turn14view2  
  owner-thread アーキ（1スレッド=1ノード/1ポート経路）と相性が良い可能性がある一方、**“CPUを余らせてレイテンシを買う”**方向に寄りがちなので、p99目標が強い場合に限定して検討するのが現実的です。citeturn14view1turn15search9  

- **multishot receive（kernel 6.0+）で受信再サブミットを減らす**  
  `io_uring_prep_recv_multishot()` は kernel 6.0 以降で利用可能とされています。read相当の再提出回数を減らせるため、tiny request が大量に来る状況で syscall/submit を削りやすい候補です。citeturn1search19turn1search28  

- **NAPI / busy polling 連携（高度・p99狙い）**  
  Linux kernel docs は busy polling を `SO_BUSY_POLL` / `net.core.busy_*` で有効化できること、さらに **epoll_wait が NAPI 処理を駆動できる**設計（epoll-based busy polling）や、**io_uring 用の NAPI busy polling API**が存在すると述べています。citeturn2search1turn2search22  
  ただし “同一 epoll コンテキストで NAPI ID を揃える”など設計制約が強く、複数NICやキューが絡むと難易度が急上昇します。netdevconfの実運用スライドでも「異なる NAPI ID を同一スレッドに混ぜると busy poll が壊れる」旨が示されています。citeturn5search13turn8search10  

**Phase 3: bypass track（Phase 0〜2で“勝てない”と確認できた場合のみ）**

- **AF_XDP**  
  AF_XDP は bind 時に zero-copy を試し、対応していなければ copy にフォールバックする等、動作と制約が kernel docs に整理されています。citeturn7search2  
  しかし Redis互換サーバの “TCP/RESP” を維持するには、（1）ユーザ空間TCPスタックを持ち込む、（2）プロキシ/ゲートウェイでL7を分離する、など **アーキの大改造**が必要になりやすく、maintainability と portability の意思決定が変わります。DPDK側でも AF_XDP PMD は“一定のキューにバインドして raw packet を送受信し kernel stack を bypass”と明記されており、L4/L7 は別途になります。citeturn7search6turn7search2  

- **DPDK**  
  DPDK の PMD は割り込みを基本使わず RX/TX descriptor をポーリングして高速に処理する設計で、EAL が hugepages（hugetlbfs）やスレッドaffinityを使うことが公式文書に書かれています。citeturn7search0turn7search27turn7search4  
  すなわち、性能は狙える一方で **“専用コア/専用メモリ/専用運用”**が前提になり、Redis互換KVに“後付け”するにはコスト構造が別物になります。citeturn7search4  

- **（参考）NICベンダ支援（例：Intel ADQ/independent pollers）**  
  IntelのADQ資料では、busy polling や epoll-based busy poll、独立ポーラ（カーネルスレッド）など低レイテンシ設計が説明されています。一方でハード依存が強く、移植性リスクが高いので “bypass track の派生” として扱うのが妥当です。citeturn7search3turn7search26  

## Measurement Protocol

### Concrete command-level profiling checklist

**前提（計測の再現性）**

- 周波数スケーリング等でブレるので、計測は “同一条件” を固定して比較します（CPU pin、同一マシン/同一NIC/同一ケーブル/同一負荷生成）。  
- ループバックだけだと NIC/IRQ の本質が見えないので、最低でも **同一L2の別ホスト**で比較し、必要に応じて loopback も併用（“pure syscall cost” の上限を見る）に留めます。  

**(1) CPUホットスポット（ユーザ/カーネルの時間内訳）**

- `perf record` → `perf report`（カーネル含む）  
  どの関数にCPUが落ちているか（`sendto/recvfrom`、softirq、スケジューラ）を確認します。workload tracing の kernel docs に例があり、`perf record` の使い方が説明されています。citeturn6search24  

- `perf stat`（基本カウンタ）  
  `cycles`, `instructions`, `context-switches`, `cpu-migrations`, `task-clock` などで、“同じスループットでどれだけCPUを食うか” を比較します（コマンド自体は利用環境に合わせる）。perf の説明は各ディストリの公式ドキュメントでも整理されています。citeturn6search31turn6search4  

**(2) syscallコストの分離（`recv*` / `send*` / poll系）**

- `perf trace` を使って syscall を追う（必要に応じてフィルタ）  
  `perf trace` は “主に syscalls を表示しつつ、スケジューリング等のイベントも扱える”と man page に記載されています。citeturn16search18  

- eBPF/ftrace でイベントトレース（低オーバーヘッド・要root想定）  
  カーネルの event tracing は `/sys/kernel/tracing/available_events` を参照し、`sched_wakeup` 等を `set_event` で有効化できると kernel docs に書かれています。citeturn16search1  
  ここで `syscalls:sys_enter_sendto` などを使えば、`sendto/recvfrom` 発生頻度・レイテンシ分布（hist）を作れます（bpftrace ならワンライナーで可能）。bpftraceのチュートリアルは sched 系 tracepoint の存在を明示しています。citeturn6search13turn16search1  

**(3) スケジューラ/wakeupコストの分離（“待ち時間”を測る）**

- `perf sched record` → `perf sched latency`  
  `perf sched latency` が “per task の scheduling latencies を報告する”と man page に明記されています。citeturn6search2  
  「wakeup/スケジューリング/handoff が大きい」という仮説の検証に直結します（Phase 0 の段階で必ず記録）。

- ftrace events（必要なら）  
  ftrace は `sched_switch` / `sched_wake_up` 等も扱うことが記載されています。citeturn6search5  

**(4) NIC/ドライバ/softirq 側の飽和確認（“アプリ外”の天井）**

- インタフェース統計：`ip -s link` + `ethtool -S`  
  Linux kernel docs はインタフェース統計の主要ソース（標準統計/プロトコル統計/ethtool統計）を整理しています。citeturn10search2  

- `/proc/net/softnet_stat` の増分観測（softirq backlog/取りこぼし）  
  Red Hatの手順は `/proc/net/softnet_stat` の読み方・増え方・関連sysctl（`netdev_max_backlog` など）まで具体的に示しています。citeturn10search6turn15search6  

- `ss -tinm`（ソケットメモリ、再送、タイマ）  
  `ss` は TCP の timer 情報や socket memory（`skmem`）を表示できると man page にあります。citeturn10search1  

### What counters/metrics to collect and compare

**アプリ側（ユーザ空間）**

- QPS（GET/SET別）、p50/p99/p999、エラー率（timeouts/connection drops）  
- 1リクエストあたりの syscall 数（`send*`, `recv*`, `epoll_wait`/`io_uring_enter` 等）  
- CPU cycles/req、instructions/req、context switches/req、cpu migrations/req（`perf stat`）citeturn6search31turn6search4  

**カーネル/スケジューラ**

- wakeup latency（`perf sched latency`）citeturn6search2  
- softirq backlog 指標（`/proc/net/softnet_stat` の relevant column の増分）citeturn10search6  

**NIC/ドライバ**

- RX/TX drops、errors、driver stats（`ethtool -S`）、標準統計（`ip -s link`）citeturn10search2  

### Pass/fail criteria for each phase

**共通の“落第条件（即ロールバック）”**

- p99 がベースライン比で悪化（例えば +10% 以上）し、かつ原因が説明できない  
- エラー率/接続切断が増える（安定性を損なう）  
- CPU使用率が増えているのにスループットが伸びない（busy poll / SQPOLL で典型）citeturn15search9turn14view1  

**Phase 0（option-only）合格ライン（例）**

- throughput +10% 以上 *または* p99 -20% 以上  
- かつ context-switches が明確に減る（busy poll が効く時の典型）citeturn15search9turn6search2  

**Phase 1（minimal code）合格ライン（例）**

- 1リクエスト当たりの `send*` 呼び出し回数が **1に収束**（あるいは明確に減る）  
- throughput +15% 以上 *または* p99 -30% 以上  
- 変更は可読性/保守性が維持され、テストでプロトコル互換が崩れていない  

**Phase 2（moderate arch：io_uring等）合格ライン（例）**

- throughput +25% 以上、または同一throughputで CPU/req -20% 以上  
- `io_uring_setup(2)` が示す SQPOLL の期待（syscall回避）が、実測で “syscall数/ctxswitch” の低下として確認できるciteturn14view1  
- 依存するカーネル機能（例：multishot recvは 6.0+）の違いで性能/挙動がぶれない評価結果が出ているciteturn1search19  

**Phase 3（bypass track）突入条件（例）**

- Phase 0〜2 を経ても、要求スループット/レイテンシ目標に対し **CPUが明確にボトルネック**で、かつ改善余地が “カーネル境界のコスト” に残っている  
- 運用として **専用ホスト/専用コア/専用NIC** を許容できる（DPDK/AF_XDP の前提）citeturn7search4turn7search0turn7search2  

## Failure Modes

- **`TCP_NODELAY` + 分割write の組み合わせで、パケット数増→CPU/IRQ/softirq増→スループット悪化**  
  `TCP_NODELAY` を効果的に使うには “小さい論理的に関連する書き込みを避ける必要がある” と明記されています。citeturn3search22  

- **`MSG_MORE`/`TCP_CORK` の flush ミスでレイテンシが跳ねる**  
  `MSG_MORE` は `TCP_CORK` 相当を per-call で与えるものなので、最後の送信でフラグを外す等の規律が崩れると tail が壊れやすいです。citeturn4search0  

- **busy polling の過剰適用で CPU を燃やし、同居プロセスやGC/バックグラウンド作業が飢餓**  
  busy polling は CPU 使用率/電力増が前提で、グローバル有効化も含め注意が書かれています。citeturn15search9turn15search6  

- **epoll-based busy polling の “NAPI ID 整列” が崩れて無効化/逆効果**  
  kernel docs は `SO_INCOMING_NAPI_ID` を利用した設計や複数NICでの注意を述べ、実運用スライドでも “異なるNAPI IDを同一スレッドに混ぜると壊れる”ことが示されています。citeturn8search10turn5search13turn4search18  

- **`SO_REUSEPORT` の偏り・予期しない分散で p99 が悪化**  
  `SO_REUSEPORT` は accept 分散に有効ですが、実運用では “なぜ特定workerが偏るか” の議論があり、観測とデバッグが難しくなり得ます。citeturn8search15turn4search14  

- **`io_uring` 導入で伸びない（あるいは遅くなる）**  
  `io_uring` の設計メリットはバッチや completion モデルであり、単なる notifier 置換では十分に活かせないとメンテナが明記しています。さらに SQPOLL は“自動で速くなるフラグではない”と man page が釘を刺しています。citeturn13view0turn14view1  

- **`MSG_ZEROCOPY` を小さい応答に使って逆に遅くなる/複雑化**  
  “10KB超でないと効果が出にくい”こと、ページピンによりバッファ寿命などセマンティクスが変わることが kernel docs に明記されています。citeturn2search2turn4search8  

- **AF_XDP/DPDK の運用負担（デバッグ困難・環境制約・巨大ページ・専用コア）**  
  AF_XDP は zero-copy の可否がドライバ依存で、DPDKは割り込みを使わないポーリング設計・hugepages/affinity を前提とします。KVサーバに適用すると、運用モデルが一段変わります。citeturn7search2turn7search0turn7search27turn7search4  

## References

- Linux man-pages: `socket(7)`（`SO_REUSEPORT`, `SO_BUSY_POLL`, バッファ倍化など）citeturn15search2turn9search0turn4search14  
- Linux man-pages: `tcp(7)`（`TCP_QUICKACK` が非永続等）citeturn8search4  
- Linux man-pages: `sendmsg(2)`（`MSG_MORE` = `TCP_CORK` 相当）citeturn4search0  
- Linux man-pages: `readv(2)/writev(2)`（scatter/gather）citeturn17search3  
- Linux man-pages: `io_uring_setup(2)`（`IORING_SETUP_SQPOLL` の意味と注意）citeturn14view1turn14view2  
- Linux man-pages: `io_uring(7)`（io_uring概要）citeturn2search15  
- Linux man-pages: `io_uring_prep_recv_multishot(3)`（multishot受信は kernel 6.0+）citeturn1search19  
- Linux kernel docs: `MSG_ZEROCOPY`（10KB閾値・注意点）citeturn2search2turn4search12  
- Linux kernel docs: NAPI / busy polling（`SO_BUSY_POLL`, `net.core.busy_*`, epoll-based busy polling, io_uring NAPI API）citeturn2search1turn2search22turn8search10  
- Linux kernel docs: AF_XDP（zero-copy/copyフォールバック等）citeturn7search2  
- Linux kernel docs: sysctl（`/proc/sys/net`、`somaxconn` の定義）citeturn15search0turn15search1  
- Linux kernel docs: interface statistics（`ip -s link`/ethtool統計の整理）citeturn10search2  
- Linux kernel docs: trace events（`available_events`, `sched_wakeup` 等）citeturn16search1  
- Linux man-pages: `perf-sched(1)`（スケジューリング遅延の測定）citeturn6search2  
- Redis公式ドキュメント（non-blocking multiplexing と `TCP_NODELAY` 設定）citeturn3search3  
- entity["company","Red Hat","raleigh, NC, US"] ドキュメント（`TCP_NODELAY` 注意点、busy polling 手順、`/proc/net/softnet_stat` 読み方）citeturn3search22turn15search9turn10search6  
- entity["company","Cloudflare","san francisco, CA, US"] Engineering（event loop/ソケット分散の実運用課題、io_uring関連の深掘り）citeturn5search0turn8search15turn5search19turn5search12  
- netdevconf（busy polling / MSG_ZEROCOPY のスライド、実運用tips）citeturn0search32turn4search8turn5search13  
- DPDK公式ドキュメント（PMDの設計、EAL/hugepages/affinity）citeturn7search0turn7search27turn7search4  
- entity["company","Intel","santa clara, CA, US"] ADQ/低遅延ネットワーク資料（epoll busy poll の per-context 化言及など）citeturn7search3turn7search15  
- entity["company","Apple","cupertino, CA, US"] kqueue man page（macOS は kqueue ベース）citeturn12search0