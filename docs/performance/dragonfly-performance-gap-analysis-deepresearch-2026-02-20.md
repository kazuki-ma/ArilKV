# Dragonfly Performance Gap Analysis: Redis互換 GET/SET における Dragonfly 優位の根拠と garnet-rs 最適化ロードマップ

## エグゼクティブサマリー

[EVIDENCE] Dragonflyは「shared-nothing」なマルチスレッド構成を中核に据え、DBをスレッド数以下のシャードに分割し、各シャードを単一スレッドが所有・処理します。クライアント接続（コネクション）は受け入れ後、ライフタイムを通じて1スレッドに束縛され、必要に応じてコーディネータが所有シャードへメッセージを送って処理を進めます。これにより、典型的な「共有データ構造＋ロック」型の並列化よりも同期・競合を小さくする設計が明確です。citeturn6view0turn8view1

[EVIDENCE] Dragonflyはスレッド内でファイバー（協調スケジューリング）を用い、「1スレッドがI/Oでブロックして全体が止まる」状況を避ける前提で、I/O・同期・通信に“fiber-friendly”なプリミティブを使うことを強調しています。citeturn6view0turn6view1

[EVIDENCE] Dragonflyは、ハッシュ表としてDashTable/Dashtable（拡張可能ハッシング＋固定サイズセグメント＋オープンアドレス）を採用し、ポインタ追跡や挿入・削除に伴う割り当て、巨大な再ハッシュによるスパイクを減らす狙いを、設計ドキュメントで具体的に説明しています。特に「ディレクトリが小さくキャッシュに載りやすい」「増設がセグメント単位で漸進的」「RD(Redis dict)のような大きい配列再確保を避ける」点が、尾部（p99/p99.9…）の安定化に直結しうる、という論旨が明示されています。citeturn8view0

[EVIDENCE] `tidwall/cache-benchmarks`の公開結果（AWS c8g.8xlarge、UNIXソケット、256接続、pipeline=1、サーバ側16スレッド、サイズ1–1024Bランダム、31回実行の中央値）では、DragonflyはGarnetServer（MicrosoftのGarnet）に対して、pipeline=1のGET/SETでops/secが高く、かつp99/p99.99の尾部レイテンシが桁違いに低い例が確認できます（例：16 threads, pipeline=1, medianで、SET ops/secがDragonfly≈3.07Mに対しGarnet≈2.32M、SET p99がDragonfly≈0.127msに対しGarnet≈0.775ms）。citeturn19view0turn19view1turn14view1turn15view0turn18view0  
※ただし、同一ベンチでもpipelineを上げると（例：pipeline=10/25/50）、Garnetが非常に高いスループットを示す一方で、p99.9/p99.99に大きなスパイクが現れるケースがあり、指標（スループット重視か、SLA内スループット重視か）と設定で「勝ち負け」が反転しうる点が重要です。citeturn20view1turn20view3turn19view3

[INFERENCE] garnet-rsにおける“Dragonflyに対する不利”は、多くのRedis互換サーバ実装で起きがちな（1）ネットワークI/Oのシステムコール頻度とバッファ再利用の弱さ（2）RESPパーサの分岐・コピー・割り当て（3）ストレージ（辞書/アロケータ/値コピー）でのキャッシュミスと競合（4）同期（ロック・原子的カウンタ・キュー）によるコア間干渉、の組み合わせで説明できる可能性が高いです。Dragonflyは、io_uringベースのI/O（epollフォールバック）＋ファイバー＋shared-nothing＋Dashtable＋専用アロケータ（mimalloc使用）といった“コストの出やすい層”を体系的に削る方向に最適化されているためです。citeturn6view1turn6view0turn8view0turn13view0turn9search1turn9search2turn9search3turn9search13

[INFERENCE] 最高ROIの最適化順は、（A）ベンチ設定を「現実のSLA」に合わせて“比較の軸”を固定し、（B）perf+フレームグラフで「差分の大半がどの層か」を定量化し、（C）まず割り当て・コピー・ロックを削る“低リスク施策”でcycles/opとp99を下げ、最後に（D）shared-nothing/シャーディングのような“構造的施策”を入れる、が最も安全です。`cache-benchmarks`自体がtasksetでサーバとクライアントをコア分離し、31回中央値で外れ値を抑える方針のため、この思想に合わせて測定のブレを先に潰すのが合理的です。citeturn14view1turn15view0turn18view0turn7search3turn12search4turn12search16

## Evidence Table

| claim | evidence | source | confidence (high/med/low) |
|---|---|---|---|
| [EVIDENCE] Dragonflyはshared-nothingで、DBをシャード化し「各シャードを単一スレッドが所有」する。 | Dragonfly公式アーキテクチャ文書が、DBをNシャードに分割し、各シャードを単一スレッドが所有・アクセスすると明記。citeturn6view0 | citeturn6view0 | high |
| [EVIDENCE] コネクションは受け入れ後、ライフタイムを通じて単一スレッドに束縛され、ファイバーで非同期化される。 | 「accept後、接続は生涯1スレッドにバインド」「各ユニットをファイバーで包む」「スレッドをブロックしない」旨を説明。citeturn6view0 | citeturn6view0 | high |
| [EVIDENCE] Dragonflyはスレッド間相互作用を“メッセージパッシング”に限定し、mutex中心の設計を避ける。 | コマンド処理の流れとして「シャードへメッセージ送信」「コーディネータが応答を待つ（ただしスレッド自体は他ファイバーを実行可能）」を記述。citeturn6view0turn8view1 | citeturn6view0turn8view1 | high |
| [EVIDENCE] DragonflyはBoost.Fibersを使い、スケジューラをI/Oポーリングと統合し、fiber-friendlyプリミティブを用いる。 | helioがBoost.Fibersスケジューラをオーバーライドし、I/Oポーリングと統合、pthread mutex等の“スレッドを止める”操作を避ける理由を説明。citeturn6view0 | citeturn6view0 | high |
| [EVIDENCE] Dragonflyのハッシュ表（Dashtable）はオープンアドレス＋固定サイズセグメントで、割り当て削減・キャッシュ局所性向上・漸進リサイズにより尾部レイテンシ抑制を狙う。 | RD（Redis dict）のポインタ多用と巨大再ハッシュに対し、Dashtableは小さいディレクトリ＋セグメント分割でメタデータを減らし、リサイズ時のスパイクを抑え尾部レイテンシを下げる、という設計説明。citeturn8view0 | citeturn8view0 | high |
| [EVIDENCE] Dragonflyはトランザクション（複数キー/複数コマンド）で、コーディネータがシャードへ複数hopを送り厳密直列化を実現する設計を持つ。 | transaction.mdで、コーディネータ（実体は接続）が各シャードへメッセージhopを送り、スケジューリングと実行フェーズを持つと説明。citeturn8view1 | citeturn8view1 | high |
| [EVIDENCE] Dragonflyはio_uring（epollフォールバック）を前提としたI/Oスタック（helio）を採用し、Linux 5.11+推奨とする。 | Dragonflyの開発ガイドが、io_uring(推奨Linux 5.11+)とepollフォールバック、helioの採用を明記。citeturn6view1 | citeturn6view1 | high |
| [EVIDENCE] io_uringはユーザ空間とカーネルの共有リングにより、従来のI/Oよりオーバーヘッド（コピー/システムコール等）を減らす設計である。 | man7のio_uring説明が共有リングによるオーバーヘッド低減（「shared ring buffers」等）を述べ、Red Hat記事もSQ/CQの共有リング設計を解説。citeturn9search1turn9search13 | citeturn9search1turn9search13 | med |
| [EVIDENCE] Dragonflyの設定フラグには`--proactor_threads`や`--pipeline_squash`があり、I/Oスレッド数やパイプラインの“squash”を調整できる。 | 公式フラグ一覧に`--proactor_threads`（CPUコア数に追従する説明）と`--pipeline_squash`（閾値以上でsquashing）を記載。citeturn13view0 | citeturn13view0 | high |
| [EVIDENCE] `cache-benchmarks`は「永続化OFF」「UNIXソケットでローカル接続」「tasksetでサーバ/クライアントをコア分離」「31回実行の中央値」を採用している。 | READMEとbench-all.sh、choose実装が方針（31回中央値、taskset分離、warmupあり等）を明記。citeturn14view1turn15view0turn18view0turn15view1 | citeturn14view1turn15view0turn18view0turn15view1 | high |
| [EVIDENCE] 公開結果の一例（16 threads, pipeline=1, median）では、DragonflyはGarnetServerよりops/secが高く、p99/p99.99が大幅に低い。 | `bench_*_run_median.json`が、Dragonfly（SET≈3.07M ops/s, p99≈0.127ms）とGarnet（SET≈2.32M ops/s, p99≈0.775ms）等を示す。citeturn19view0turn19view1 | citeturn19view0turn19view1 | high |
| [INFERENCE] pipelineが小さい（=1）ほど、squash/バッチ化の恩恵が小さくなり、1リクエストあたりのシステムコール/コピー/割り当て/辞書参照の“固定費”が支配的になる。その固定費を削る設計（shared-nothing、Dashtable、io_uring等）がDragonfly優位に働きやすい。 | ベンチ条件（pipeline/サイズ範囲/接続数）と、Dragonflyの設計要素（shared-nothing/Dashtable/io_uring）。ただし“寄与率”は要測定。citeturn15view0turn15view1turn6view0turn8view0turn6view1turn9search1 | citeturn15view0turn6view0turn8view0turn6view1turn9search1 | med |

## Bottleneck Model

### 前提

このモデルは「まずどこを疑うべきか」の初期仮説です（INFERENCE）。実際の寄与率は、同一ハードウェア・同一設定での`perf stat`とCPUフレームグラフで更新してください。citeturn7search3turn12search4turn12search16turn14view1

`cache-benchmarks`系の典型構成（例：UNIXソケット、256同時接続、pipeline=1、値サイズ1–1024Bランダム）では、アプリ側の“純粋な計算”だけでなく、ソケット送受信（カーネルコピー＋wake-up）と値コピーが極めて大きくなりやすいです。実際、公開JSONにはops/secに対応するMB/secが併記され、数GB/s級の転送が観測されています（例：Dragonfly SETで≈1.6GB/s相当）。citeturn19view0turn21view0turn15view1

### 推定寄与率（INFERENCE）

| component | 推定寄与率 | 典型的な“症状” | どう見分けるか（perf/フレームグラフ） |
|---|---:|---|---|
| network | 25–45% | `recv/send`（もしくは`read/write`）、poll/epoll/io_uring待ち、wake-upが目立つ | `perf record`で`sys_*`/ソケット関連が上位。`perf stat`でコンテキストスイッチやsys time増。citeturn7search3turn12search4turn15view1turn9search1turn9search13 |
| parser | 10–20% | RESPデコード、数字パース、分岐多め | フレームグラフでRESP処理（トークナイズ/パース）が太い。branch-misses比率が高い。citeturn10search2turn15view1turn21view0 |
| storage | 20–40% | ハッシュ探索、キー比較、TTL/メタデータ処理、値コピー | ハッシュ計算・辞書操作関数が太い。LLC missが増える。データ構造のポインタ追跡が多い。citeturn8view0turn6view0turn7search3 |
| allocator | 5–20% | 値/一時バッファのmalloc/free、フラグメンテーション、スループット低下やスパイク | `malloc/free`（またはアロケータ内部）が上位。ページフォルト増。アロケータ差し替えで変動。citeturn9search3turn6view1turn21view0 |
| sync | 5–20% | ロック待ち、原子的カウンタ競合、キュー競合 | futex/lock系が上位。特定のMutex/RwLock/チャネル待ちが太い。citeturn6view0turn8view1turn7search3 |

補足として、Dragonflyの設計は「sync比率を低くし、storageのキャッシュ局所性を上げ、networkの固定費（syscall/イベント処理）を下げる」方向に整っています（shared-nothing、Dashtable、io_uring/helio、pipeline squashing）。citeturn6view0turn8view0turn6view1turn13view0  
一方で、bench設定（特にpipeline）により、相対優位の出方は変わります。公開結果でもpipeline=10以上で挙動が大きく変化しています。citeturn20view1turn20view3turn19view2turn19view3

## Transferability Matrix

以下は「Dragonflyで使われている（またはドキュメント化されている）技術」→「garnet-rsへ移植できるか」を整理したものです。期待効果は、まず`cache-benchmarks`のpipeline=1と“実運用に近い設定”の両方で検証する前提です（INFERENCE）。citeturn14view1turn15view0turn13view0

| technique | expected impact | implementation complexity | risk | fit for garnet-rs |
|---|---|---|---|---|
| shared-nothing（キー空間シャーディング＋シャード単一スレッド所有）citeturn6view0 | 大（sync削減、コアスケール、p99改善） | 高（アーキ再設計） | 中〜高（正しさ/コマンド網羅） | ◎（長期的に最大ROI） |
| コネクションのスレッドアフィニティ（接続を特定スレッドに固定し、同スレッドでバッファ再利用）citeturn6view0 | 中〜大（キャッシュ局所性・割り当て減） | 中 | 中 | ○ |
| cooperative scheduling（ファイバー的：OSスレッドをブロックしない設計原則）citeturn6view0turn6view1 | 中（テール改善、スループット安定） | 中〜高（ランタイム設計に依存） | 中 | ○（tokioでも“ブロッキング排除”は可能） |
| io_uringベースのネットワークI/O（epoll代替、syscall/イベント処理削減）citeturn6view1turn9search1turn9search13 | 中〜大（特にpipeline小＆多接続） | 高（実装/保守/互換） | 中〜高（カーネル依存・デバッグ難） | △（Linux専用・段階導入推奨） |
| pipeline squashing / 応答バッチ化（flush回数・syscall減）citeturn13view0 | 大（pipeline大や多接続で顕著） | 低〜中 | 低〜中（順序・フラッシュ条件） | ◎（まずやる価値が高い） |
| Dashtable的な“セグメント型オープンアドレス”辞書（再ハッシュスパイク抑制）citeturn8view0 | 中〜大（テール/CPU効率） | 中〜高（データ構造置換） | 中（バグ/メモリ安全） | ○（GET/SET特化なら適合） |
| アロケータ最適化（例：mimalloc/jemalloc、スレッド局所プール）citeturn6view1turn9search3 | 中（cycles/opとスパイク低減） | 低 | 低〜中（環境差） | ◎（短期で試せる） |
| 値/バッファの再利用（read/writeバッファ、レスポンス組み立て領域のプール化）citeturn13view0turn15view1 | 中〜大（割り当てとキャッシュミス削減） | 中 | 低 | ◎ |
| マルチキー整合性のための“コーディネータ＋hop”モデル（VLL系）citeturn8view1turn7search5 | GET/SETだけなら小（将来機能では大） | 高 | 中〜高 | △（今はhot path優先） |
| ベンチの統計設計（31回中央値・外れ値カット、コア分離）をそのまま踏襲citeturn14view1turn18view0 | 大（判断ミスを防ぐ） | 低 | 低 | ◎ |

## Prioritized Backlog

ここでは「Dragonflyとの差分要因になりやすい層」を上から潰す順に、最大10件で提案します（INFERENCE）。各項目は`cache-benchmarks`互換の測定（ops/sec・p99・perf stat）で完了判定できるようにしています。ベンチは最低3回実行し中央値、可能なら`cache-benchmarks`同様に31回＋外れ値除去に近づけてください。citeturn14view1turn18view0turn15view1

| priority item | why this is high ROI | measurable success criteria |
|---|---|---|
| 1) RESPデコード〜コマンド実行〜レスポンス送信を“pipeline単位でまとめてflush”する（応答のバッチ化/集約） | Dragonflyは`--pipeline_squash`を持ち、パイプライン処理を意識した最適化がある。固定費（syscall/ロック/バッファ管理）の削減に直結。citeturn13view0turn15view1 | pipeline=1/10で、(a) ops/sec +10〜25% (b) `perf stat` cycles/op -10% 以上 (c) p99悪化なし（または改善） |
| 2) 送受信バッファ/一時パース領域の再利用（per-connection/per-threadでプール化） | 1–1024Bの値サイズランダムでは、バッファ確保・拡張がスパイクになりやすい。DragonflyはクライアントI/Oバッファ上限などを明示している。citeturn13view0turn15view1 | `perf record`で`alloc`系フレームが相対的に半減。`perf stat` page-faults低下。p99.99のスパイク頻度減 |
| 3) グローバルアロケータの差し替え（mimalloc等）＋割り当てパターンの整形 | Dragonflyは依存関係としてmimallocを挙げており、低競合・局所性を重視している。citeturn6view1turn9search3 | pipeline=1で cycles/op -5〜15%。p99.99の“ときどき大きい遅延”が減る（長時間runで確認） |
| 4) GET/SET hot pathで“コピー回数”を削る（特にレスポンス組み立てのコピー削減、可能ならvectored write） | 公開結果ではGB/s級の転送が発生しており、コピー効率が支配的になりうる。citeturn21view0turn19view0turn15view1 | mb/sec同等以上で cycles/op -10%。LLC-load-misses比率低下（perf stat -d） |
| 5) ストレージ辞書の“リサイズ/再ハッシュ”スパイク対策（漸進リサイズ、セグメント分割、あるいは容量予約戦略の徹底） | Dashtableが「巨大再確保を避ける」「尾部レイテンシを下げる」ことを設計目的として明示。類似スパイクがgarnet-rs側にあれば高確度で効く。citeturn8view0 | p99.9/p99.99を中心に改善（例：p99.99 -30%）。フレームグラフでリサイズ関数が消える/細る |
| 6) “キー→シャード”で処理スレッドを決める（まずは受信スレッド→実行スレッドへのルーティングを最小化） | Dragonflyはコーディネータがhashでシャード所有者へメッセージを送るモデル。ロックで共有辞書を守るより競合が減りやすい。citeturn6view0turn8view1 | スループットがコア数増加で素直に伸びる（16→32で+~80%等を目標）。futex待ちが激減 |
| 7) “同一シャードに属するパイプライン”の命令列をまとめて実行（コーディネータ→シャードのhop削減） | Dragonflyは複数コマンドをまとめてhop回数を減らす「squashing」思想をtransaction docで説明。citeturn8view1turn13view0 | pipeline=10/25で ops/sec +20% 以上、p99悪化なし。フレームグラフでキュー/ディスパッチが細る |
| 8) 同期の局所化（統計カウンタ/メトリクスもper-threadに寄せ、集約は遅延） | shared-nothingは“グローバル原子”がボトルネック化し得る点を認めつつ、実害は小さいとしている。garnet-rs側は計測用原子がhot pathに混入しがち。citeturn8view1turn6view0 | `perf record`で原子操作/ロック周りの比率が目に見えて低下。ops/sec +5〜10% |
| 9) Linux限定のI/O改善（まずはepoll最適化→必要ならio_uring導入） | io_uringは共有リングでオーバーヘッドを減らす設計だが、置換は高難度。段階導入が安全。citeturn6view1turn9search1turn9search13 | pipeline=1でsys time比率低下、context-switch減。最終的にops/sec +10〜20% |
| 10) “公平な比較”を固定化するベンチプロファイル作成（SLA別：p99<1ms達成時最大ops等） | pipelineや値サイズで勝敗が変わりうるため、比較軸（SLA）を固定しないと最適化が迷走する。公開結果でもpipelineで挙動が変化。citeturn20view1turn19view0turn14view1 | 3つのSLAプロファイル（例：p99<0.5ms / p99<1ms / p99<5ms）を定義し、各最適化で“どれが伸びたか”が追える |

## Experiment Plan

### 公平性チェックリスト

`cache-benchmarks`の考え方（永続化OFF、UNIXソケット/ローカル、コア分離、warmup、31回中央値）をできる限り踏襲し、まず“比較の前提”を固定します。citeturn14view1turn15view0turn15view1turn18view0

- 同一マシン・同一カーネルで実行（DragonflyはLinux 5.11+推奨、io_uring前提の最適化があるため）。citeturn6view1turn9search1turn9search13  
- 同一トランスポート（UNIXソケットかTCP）を選び固定。`cache-benchmarks`はデフォルトUNIXソケットで、`--tcp`で切替可能。citeturn14view1turn15view1  
- CPUピニング：サーバを`taskset -c 0-15`、クライアントを`16-31`のように分離（`cache-benchmarks`準拠）。citeturn14view1turn15view0turn15view1  
- スレッド数は“同じコア数”を使うように揃える（Dragonflyは`--proactor_threads`で制御可能）。citeturn13view0turn15view1  
- warmup（事前SET）を入れて辞書初期化スパイクを測定から除外（`cache-benchmarks`準拠）。citeturn14view1turn15view1  
- 指標を固定：最低でも「ops/sec」「p99」「p99.99」「cycles/op（perf stat）」を常に記録。citeturn14view1turn7search3turn15view1  

### 3-run medianポリシー

- 各シナリオ（例：pipeline=1/10、値サイズ=32B固定 or 1–1024B、接続数=256等）を最低3回走らせ、中央値を採用。  
- 可能なら`cache-benchmarks`方式に寄せ、31回＋外れ値除去（上位/下位10%除外）を採用（ハードは時間がかかるが、判断ミスを大幅に減らせる）。citeturn14view1turn18view0turn15view0  

### Linux perf とフレームグラフの具体コマンド

#### 前準備（共通）

フレームグラフは`perf script`→`stackcollapse-perf.pl`→`flamegraph.pl`で生成します。citeturn12search0turn7search7turn12search16  
DWARFでのスタックウォークやフレームポインタの注意点（最適化でフレームポインタが省略される等）も踏まえ、必要ならビルド設定を調整します。citeturn12search4

```bash
# 1) FlameGraph scripts
git clone https://github.com/brendangregg/FlameGraph
export FLAMEGRAPH_DIR=$PWD/FlameGraph

# 2) perf の権限（必要な場合）
# 例: paranoidを下げる（環境のポリシーに従う）
sudo sh -c 'echo 1 > /proc/sys/kernel/perf_event_paranoid'
```

#### Dragonfly（ネイティブLinux想定）

Dragonflyのスレッド数は`--proactor_threads`で揃えます。citeturn13view0  
`cache-benchmarks`と同じくUNIXソケットで測る場合：

```bash
# サーバ起動（例：16コア固定。必要に応じてmaxmemory等も揃える）
taskset -c 0-15 dragonfly \
  --port 0 --unixsocket /tmp/cachebench.sock \
  --proactor_threads 16 \
  --dir "" --dbfilename "" \
  --maxmemory 32gb

# ベンチ（memtierも同様にtaskset、pipeline等は現状の統合に合わせる）
taskset -c 16-31 memtier_benchmark \
  -S /tmp/cachebench.sock \
  -t 16 -c 16 -n 100000 \
  --ratio 1:0 --pipeline 1 \
  --data-size-range 1-1024 \
  --distinct-client-seed --hide-histogram \
  --print-percentiles 50,90,99,99.9,99.99
```

`perf stat`（CPUカウンタの全体像）：

```bash
PID=$(pidof dragonfly)

sudo perf stat --no-big-num --no-scale -d \
  -p $PID -- sleep 30
```

`perf record`→flamegraph（CPUホットパス）：

```bash
PID=$(pidof dragonfly)

# 30秒間サンプリング。-F 99 は一般的な起点。
sudo perf record -F 99 -g --call-graph dwarf -p $PID -- sleep 30

sudo perf script | $FLAMEGRAPH_DIR/stackcollapse-perf.pl | \
  $FLAMEGRAPH_DIR/flamegraph.pl > flame_dragonfly.svg
```

`perf`の使い方はBrendan Greggの例が網羅的です。citeturn7search3turn12search4

#### garnet-rs（サーバPIDにattachする形）

garnet-rsはバイナリ名が環境依存のため、ここでは`<GARNET_RS_BIN>`として記載します（実際は現在の統合で使っている起動コマンドをそのまま当てはめてください）。

```bash
# サーバ起動（CPU固定＆スレッド数をDragonflyと同等に）
taskset -c 0-15 <GARNET_RS_BIN> --threads 16 --unixsocket /tmp/cachebench.sock &
GARNET_PID=$!

# ベンチはDragonflyと同条件で
taskset -c 16-31 memtier_benchmark \
  -S /tmp/cachebench.sock \
  -t 16 -c 16 -n 100000 \
  --ratio 1:0 --pipeline 1 \
  --data-size-range 1-1024 \
  --distinct-client-seed --hide-histogram \
  --print-percentiles 50,90,99,99.9,99.99

# perf stat
sudo perf stat --no-big-num --no-scale -d -p $GARNET_PID -- sleep 30

# perf record → flamegraph
sudo perf record -F 99 -g --call-graph dwarf -p $GARNET_PID -- sleep 30
sudo perf script | $FLAMEGRAPH_DIR/stackcollapse-perf.pl | \
  $FLAMEGRAPH_DIR/flamegraph.pl > flame_garnet_rs.svg
```

Rust側でフレームポインタが必要なら、Brendan Greggが述べる通り`-fno-omit-frame-pointer`相当（Rustなら`-C force-frame-pointers=yes`）を検討し、DWARFスタックウォークとのトレードオフを理解して選ぶのが定石です。citeturn12search4

### “差分の切り分け”実験（network / parser / storage / allocator / sync）

以下は各層の寄与を推定するための実験デザインです（INFERENCE）。`perf`で“実際に太いところ”を見てから着手すると、手戻りが減ります。citeturn7search3turn12search16

- network+parserの上限を見る：`PING`（または最小応答の`ECHO`相当）を大量に流し、GET/SETとの差分を観察（ストレージなし/最小コピー）。`cache-benchmarks`も起動確認にRESP `PING`を使っています。citeturn15view1turn10search2  
- parser寄与を見る：同一キー・同一サイズ・pipeline固定で、レスポンス生成をstub化（固定応答）したビルドを用意し、ops/secとcycles/opの変化を見る（開発用計測）。  
- allocator寄与を見る：グローバルアロケータ差し替え＋バッファ再利用ON/OFFのABで、`malloc/free`ホットスポットとpage-faults/スパイクの変化を見る（mimallocの設計思想も参照）。citeturn9search3turn21view0  
- sync寄与を見る：ロック粒度（全体ロック→シャードロック→所有スレッド化）の段階移行で、futex/lock待ちがどれだけ消えるかをフレームグラフで追う。citeturn6view0turn8view1  

## 今週実行する最初の3アクション

1. **Linux上で“同条件”のプロファイルを取る（Dragonfly と garnet-rs）**：pipeline=1（現実的な低レイテンシ想定）と、pipeline=10（高スループット想定）の2点で、`perf stat`とCPUフレームグラフを作成し、上位5ホットスタックを層（network/parser/storage/allocator/sync）に分類する。citeturn7search3turn12search4turn12search16turn15view1turn13view0

2. **最小リスクの“固定費削減”を2つ入れてAB検証する**：  
   (a) レスポンスのバッチ化（pipeline単位でまとめてflush）  
   (b) バッファ再利用（read/parse/write領域のプール化）  
   成功基準は「pipeline=1でcycles/op -10%」「p99悪化なし」「ops/sec +10%」を一次目標にする。citeturn13view0turn15view1turn14view1turn7search3

3. **“尾部レイテンシが跳ねる根”を特定し、辞書/リサイズ/割り当てに対策を当てる**：フレームグラフとp99.9/p99.99のスパイク時刻を突き合わせ、（i）辞書リサイズ（ii）アロケータ（iii）同期（ロック/チャネル）どれが支配的かを確定し、次スプリントの最上位バックログ（Prioritized Backlogの5〜6）を確定させる。DragonflyのDashtable設計が“スパイク抑制”を中心に書かれているため、類似スパイクが見えた時点で最優先で潰す価値が高い。citeturn8view0turn7search3turn12search16

## Repository Notes (2026-02-20)

- Source import path: `/Users/kazuki-matsuda/Downloads/deep-research-report (1).md`
- In-repo path: `docs/performance/dragonfly-performance-gap-analysis-deepresearch-2026-02-20.md`
- Local GET/SET hotspot artifacts:
  `/tmp/garnet-hotspots-20260220-091323`
- Flamegraph files:
  - `/tmp/garnet-hotspots-20260220-091323/garnet-get.flame.svg`
  - `/tmp/garnet-hotspots-20260220-091323/garnet-set.flame.svg`
- Local command used for reproducible capture:
  - `cd garnet-rs && ./benches/local_hotspot_framegraph_macos.sh`
  - `cd garnet-rs && SHARD_COUNTS='1 2 4 8 16' REQUESTS=20000 ./benches/sweep_string_store_shards_local.sh`

### Local findings mapped to this report

- GET-only local run:
  `319,177.80 ops/s`, `p99 1.015 ms`
- SET-only local run:
  `273,482.30 ops/s`, `p99 1.087 ms`
- GET hotspots (leaf, sample-based):
  - mutex wait `40.79%`
  - `sendto` `24.36%`
  - `recvfrom` `22.51%`
- SET hotspots (leaf, sample-based):
  - mutex wait `46.90%`
  - `sendto` `20.65%`
  - `recvfrom` `19.62%`
- Interpretation:
  - The shared-nothing / owner-thread direction in this report is consistent with
    local hotspot evidence showing lock contention as a dominant cost.
  - First implementation step completed in `garnet-rs`: debug/test lock-order
    instrumentation + deterministic sync points + shard-aware string-store
    locking (`GARNET_TSAVORITE_STRING_STORE_SHARDS`, default `1`).
  - First sweep data for string-store lock striping on local host
    (`threads=8`, `conns=16`, `requests=20000`):
    - shard 1: SET `317176`, GET `319846`
    - shard 2: SET `309438`, GET `331785`
    - shard 4: SET `302024`, GET `313987`
    - shard 8: SET `282727`, GET `281040`
    - shard 16: SET `231803`, GET `257843`
    - Current decision: keep default shard count at `1`; treat auto-scaling by
      thread hints as opt-in pending broader sweep coverage.

### Framegraph A/B validation for regression hypothesis (`shards=1` vs `shards=16`)

- Artifacts:
  - `/tmp/garnet-hotspots-shards1-20260220`
  - `/tmp/garnet-hotspots-shards16-20260220`
- Repro command:
  - `cd garnet-rs && STRING_STORE_SHARDS=1 OUTDIR=/tmp/garnet-hotspots-shards1-20260220 ./benches/local_hotspot_framegraph_macos.sh`
  - `cd garnet-rs && STRING_STORE_SHARDS=16 OUTDIR=/tmp/garnet-hotspots-shards16-20260220 ./benches/local_hotspot_framegraph_macos.sh`
- Throughput delta (memtier `Ops/sec` from log/json):
  - GET: `444280.31 -> 261275.93` (`-41.2%`)
  - SET: `286002.03 -> 217070.20` (`-24.1%`)
- Hotspot delta (sample-based):
  - GET leaf:
    - `__psynch_mutexwait`: `49.83% -> 32.35%` (down)
    - `HashIndex::find_tag_address`: `0.05% -> 22.18%` (up)
  - SET leaf:
    - `__psynch_mutexwait`: `56.22% -> 41.72%` (down)
    - `HashIndex::find_tag_entry` + `find_or_create_tag`: `0.54% -> 18.61%` (up)
- Interpretation:
  - Regression at high shard count is not explained by lock-wait increase.
  - The bottleneck shifted to Tsavorite hash-index lookup paths, which is
    consistent with over-sharding without owner-thread routing and indicates
    shard-count policy should remain conservative until routing/partition design
    is introduced.
