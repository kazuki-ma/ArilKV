## なぜ Garnet は Redis / Dragonfly より “さらに” 高スループットになり得たのか

ベンチの数字だけ見ると「C#/.NET なのに？」となりますが、Garnet は *言語で勝った* というより **エンドツーエンド（NIC→RESP→KV→RESP）で “1リクエスト当たりの無駄” を徹底的に削り、そのままマルチコアに素直に伸びる形** を取りに行っているのが効いています。設計上の効きどころを分解するとこうです。

### 1) ネットワーク〜ストレージまで “スレッド・ハンドオフ” を減らす（＝キャッシュ局所性とスケジューリングの勝ち）

Garnet の説明で一番「性能の哲学」が出ているのがここで、

* **ネットワーク層が shared-memory 設計（ShadowFax 由来）**
* **TLS 処理とストレージ操作を IO completion thread 上で行い、一般ケースで thread switching を避ける**
* **CPU の cache coherence に “データをネットワークまで運ばせる” ことで、シャッフル型（スレッド/コア間でデータを移す）設計の data movement を減らす**

と明言しています。これが効くワークロードはまさに「小さい GET/SET を大量」「コネクション/セッション多数」「バッチ小〜中」みたいな世界で、ここが詰まると C++ でも落ちます。([Microsoft][1])

### 2) ストレージ層が “スケールする共有メモリKVS” の系譜（Tsavorite / FASTER）

Garnet のストレージは **Tsavorite**（FASTER 由来）を土台にしていて、プロジェクトページでも「thread-scalable」「cache-friendly shared-memory scalability」「tiered storage」を前面に出しています。([Microsoft][2])

FASTER 自体が「cache-optimized な並行ハッシュインデックス + hybrid log（ログ構造）」「in-place update も含む高性能KV」方向の研究・実装で、要するに **“共有メモリで多スレッドが触っても伸びる” ことにコミットしたストレージ設計** です。([Microsoft][3])

さらに Garnet はストレージを2系統に分けています：

* **main store**：raw string（＝典型的な GET/SET）向けで **GC を避けるようにメモリを慎重に管理**
* **object store**：Sorted Set / Hash / List など複合型は .NET のライブラリエコシステムを活用（ヒープも使う）

この “GET/SET の超ホットパスだけは徹底的に非GC寄り” が、ベンチの伸び方に直結します。([Microsoft][1])

### 3) 「多数セッション・小バッチ」に最適化されているのを、ベンチ設計と結果が示している

Garnet 側の公開ベンチでは、Azure の 72 vCPU VM を2台（クライアント/サーバ分離）で、accelerated TCP を有効化し、Redis/KeyDB/Dragonfly と比較しています。([microsoft.github.io][4])

特に GET のスループット測定では

* **4096 req/batch**
* **8-byte key/value**（ネットワークオーバーヘッドを最小化）

という “サーバ実装の純粋な処理能力が出やすい” 条件にして、スレッド（クライアントセッション数）を増やした時に **Garnet がより良くスケールし、Dragonfly は同様の傾向だが 16 threads まで**、と書いています。([microsoft.github.io][4])

また、レイテンシ面でも「多数セッション」「バッチ無し（batchsize 1）でも」安定し、さらに **adaptive client-side batching に fine-tuned** と説明されています。([microsoft.github.io][4])

### 4) Dragonfly 側のドキュメントでも、条件次第で “Garnetがかなり上” の結果が出ている

面白いのは Dragonfly の公式 docs（2026/01 更新）に **Comparison with Garnet** があり、同一インスタンス（c6in.12xlarge）で

* 非パイプラインは「近い」結果
* **pipelined read（pipeline=10）で Garnet 25.4M ops/sec、Dragonfly 6.9M ops/sec** と書かれています

（もちろん、この数値を一般化するのは危険ですが、「少なくともある条件ではここまで差が付く」ことは示唆します。）([Dragonfly][5])

### 5) Redis との差は “単体プロセスでの垂直スケール思想” が根本

Redis は歴史的に「単体プロセスは基本1コアでさばき、スケールは複数インスタンス/クラスタで」という思想が強く、Redis 自身のブログでも「単体の single process Redis は1コアしか使えない」前提で議論しています。([Redis][6])
Garnet は「単一ノード内で thread-scalable」を前提にしているので、同じマシンのコアを *単体で* 使い切る方向に寄せやすいです。([Microsoft][2])

---

## Java/Kotlin で同等（≒Garnet級のスループット）を狙う勝算は？

### 結論から言うと

* **“可能性はあるが、勝算は「言語」より「設計と実装の気合」依存**
* JVM で勝ち筋を作るなら、**コアは Java で書き、Kotlin は周辺**に寄せるのが現実的（Kotlin をホットパスに入れるほど、結局 Java 的な書き方に収束しがち）

Garnet 自体が .NET/C# で「GC オーバーヘッドを出さない設計」を掲げているので、「マネージド言語だから無理」は成り立ちません。([Microsoft][2])

### JVM で “勝ち筋” を作るために必須の条件（ほぼ逃げ道なし）

Garnet の強みを JVM で再現するなら、最低でも以下が必要になります（= ここを外すと Redis/Dragonfly には届きにくい）。

1. **ホットパス（GET/SET）の “割り当てゼロ” を徹底**

   * RESP パースで `String` を作らない
   * 返却もバッファに直接書く（可能なら zero-copy）
   * メトリクス/ログもホットパスから切り離す

2. **データは “オンヒープに置かない” を基本にする**

   * オンヒープ `byte[]`/オブジェクトを大量に持つと GC で尾を引く
   * Garnet が main store で GC 回避を明言しているのはここが理由。([Microsoft][1])
   * つまり JVM でも off-heap（DirectByteBuffer/Unsafe/FFM 等）で arena/slab を自前に近い形で持つのが現実的

3. **スレッド設計：shared-nothing か shared-memory かを最初に決める**

   * **shared-nothing（スレッド=シャード所有）**：実装は比較的作りやすいが、キー跨ぎコマンド/トランザクション/スクリプトで難所が来る
   * **shared-memory（FASTER/Tsavorite系）**：作れれば強いが、ロック/競合/メモリ再利用（epoch等）を相当うまくやらないと伸びない

4. **ネットワーク受信→処理→送信の “ハンドオフ最小化”**

   * Garnet は IO completion thread 上で TLS/ストレージまでやる方針を明言している。([Microsoft][1])
   * JVM でも「イベントループ→ワーカーへ投げる」を安易にやると、ここで負けやすい

### Kotlin は？

Kotlin/JVM でも “書ける” けど、最速帯を狙うなら

* **RESP パーサ、バッファ管理、ストレージ、スケジューラ**は Java（もしくは一部 JNI）
* Kotlin は **設定/運用API/管理プレーン**に寄せる

が堅いです。Kotlin を性能臨界領域に入れるなら、インライン化・割り当て削減・境界チェック等で結局 “ほぼJava” になります。

---

## Rust なら勝算は上がる？

**純粋な「勝算」だけなら、Rust の方が上がりやすい**です。理由はシンプルで：

* **GC が無い**ので tail latency を安定させやすい
* メモリ配置・アロケータ・zero-copy を *最初から前提* にできる
* 高性能ネットワーク（io_uring / busy-poll / kernel-bypass 等）や CPU affinity 前提の設計が取り込みやすい

ただし、勝つには “Rust で書けば勝てる” ではなく、結局 Garnet がやっているのと同じく

* **スレッドハンドオフ削減**
* **キャッシュ局所性重視**
* **競合が出る場所の設計（shared-nothing か shared-memory か）**
* **RESP パースとメモリ再利用**

を詰める必要があります。

実装戦略としては、2択になりがちです：

* **shared-nothing（thread-per-core, shard ownership）**：Dragonfly 的。実装しやすく伸びやすい一方、キー跨ぎ操作が難しくなる
* **FASTER/Tsavorite 的 shared-memory + epoch**：作れたら強いが、unsafe/EBR/アロケータ含め難度が上がる

---

## WebAssembly はどう？

### コア実装（“Redis互換サーバをWASMで作って爆速”）としては、基本的に不利

WASM はサンドボックスなので、

* ネットワークI/O がホスト呼び出し（境界跨ぎ）になりやすい
* io_uring などの低レベル最適化を素直に使いにくい
* 「1リクエストあたりの固定費」が増えがち

で、**Garnet/Dragonfly と殴り合う土俵では勝ちにくい**です。

### ただし “拡張機構” としての WASM はむしろ相性がいい

Garnet も「拡張性」「stored procedure」を強みに挙げています。([Microsoft][2])
この領域は WASM の独壇場で、

* Redis の Lua のような「サーバ内実行」を **より安全に**やれる
* ユーザ定義関数/モジュールをサンドボックス化できる
* コアは Rust/Java/C++ で最適化し、**プラグインだけ WASM** にする

という構成なら、実務的にも性能的にもかなり筋がいいです。

---

## まとめ（意思決定に使える形で）

* **Garnet が速い主因**は「Tsavorite/FASTER系のスケールするストレージ」×「IO完了スレッド上でTLS/ストレージまでやる等の end-to-end 最適化」×「GC回避を含むメモリ設計」あたりが合わさったこと。([Microsoft][1])
* **Java でも到達は可能**だが、勝ち筋は「オフヒープ前提」「割り当てゼロ」「ハンドオフ最小化」など、かなりシステム寄りの作り込みが必要（Kotlin は周辺向けが無難）。
* **Rust は最速帯を狙う上では有利**（GCが無い/制御しやすい）。ただし設計が全て。
* **WASM はコアには不利**、ただし **拡張/スクリプトの安全な実行基盤**としては有望。

もし「勝算」をもう一段具体化するなら、あなたが狙っているのは次のどれかで難度が変わります：

1. **GET/SET + TTL + eviction 程度の限定互換で “性能最優先”**
2. **Redisの複合型まで広く互換 + パフォーマンスも**（難度が跳ねる）
3. **クラスタ/レプリケーション/永続化まで含める**

どこまでを “同等” と見なすか（互換性の範囲と、p99/p99.9 のSLO）を教えてくれれば、その前提で「Java/Rust/WASM それぞれの最適アーキ」をもう少し具体に落とせます。

[1]: https://www.microsoft.com/en-us/research/blog/introducing-garnet-an-open-source-next-generation-faster-cache-store-for-accelerating-applications-and-services/ "https://www.microsoft.com/en-us/research/blog/introducing-garnet-an-open-source-next-generation-faster-cache-store-for-accelerating-applications-and-services/"
[2]: https://www.microsoft.com/en-us/research/project/garnet/ "https://www.microsoft.com/en-us/research/project/garnet/"
[3]: https://www.microsoft.com/en-us/research/publication/faster-concurrent-key-value-store-place-updates/ "https://www.microsoft.com/en-us/research/publication/faster-concurrent-key-value-store-place-updates/"
[4]: https://microsoft.github.io/garnet/docs/benchmarking/results-resp-bench "https://microsoft.github.io/garnet/docs/benchmarking/results-resp-bench"
[5]: https://www.dragonflydb.io/docs/getting-started/benchmark "https://www.dragonflydb.io/docs/getting-started/benchmark"
[6]: https://redis.io/blog/redis-architecture-13-years-later/ "https://redis.io/blog/redis-architecture-13-years-later/"
