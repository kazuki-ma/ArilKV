# 17 -- JVM Performance Gap Analysis: Type Erasure, JIT, GraalVM, Valhalla

> **最終更新**: 2026年2月18日。Valhalla / GraalVM のステータスは [OpenJDK](https://openjdk.org/projects/valhalla/) および [GraalVM](https://www.graalvm.org/) の公式情報に基づく。

## 1. C# Reified Generics vs JVM Type Erasure

### 1.1 根本的な違い

C# と JVM のジェネリクスは実行時の振る舞いが根本的に異なる。

**C# — Reified Generics（型情報保持 + monomorphization）**:
- JIT が型パラメータの組み合わせごとに**専用のネイティブコード**を生成する
- `TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator>` → この組み合わせ専用の機械語
- `struct` 制約をつけると、boxing なし・virtual dispatch なし・インライン化可能
- Garnet はこれを活用して `ISessionFunctions` コールバックを毎秒数百万回、virtual dispatch なしで呼んでいる

**JVM — Type Erasure（型消去）**:
- `List<String>` はコンパイル後に `List<Object>` になる
- ジェネリクスの型情報は**実行時に消える**
- 結果: primitive の boxing、virtual dispatch、インライン化不可
- Kotlin も JVM 上なので同じ制約

### 1.2 性能への影響: dispatch コスト

```
C#:  ISessionFunctions.SingleReader()  → 直接関数呼び出し（0ns overhead）
JVM: ISessionFunctions.singleReader() → vtable lookup（10-25ns/op overhead）
```

これが毎秒数百万回のホットパスで積み重なる。Doc 16 では monomorphization を「JVM 再実装の最大の構造的ハンディキャップ」と評価している。Rust は C# と同じくコンパイル時 monomorphization を行うため、この問題は発生しない。

### 1.3 性能への影響: メモリレイアウト

dispatch コスト以上に深刻なのがメモリレイアウトの違いである。

```
// C# — monomorphization でデータレイアウトが専用化される
HashBucket[] buckets;  // 各要素が連続64バイト、配列全体が1つのメモリブロック
buckets[i].entries[3]  // → base + i*64 + 3*8 = 単一ポインタ演算

// JVM — type erasure でオブジェクト参照の配列になる
HashBucket[] buckets;  // 各要素がヒープ上の別オブジェクトへの参照
buckets[i].entries[3]  // → ポインタ参照 → オブジェクトヘッダ跨ぎ → 配列参照 → 値
                       //   キャッシュミス 2-3回
```

C# では struct 配列が連続メモリに flat に並ぶが、JVM ではオブジェクト参照の配列（ポインタの配列）になる。これは JIT では解決できない言語レベルの制約である。

---

## 2. JIT / GraalVM による Gap 補償の限界

### 2.1 JVM JIT (C2) による補償

JVM の JIT (C2) は3つのテクニックで virtual dispatch を軽減できる。

| テクニック | 効果 | 限界 |
|-----------|------|------|
| **Monomorphic inline cache** | コールサイトが常に同じ型なら、型チェック+直接呼び出しにインライン化 | Garnet の `ISessionFunctions` は実質1型なので効く |
| **Speculative devirtualization** | 投機的にインライン化、ガード失敗時に deopt | インライン化バジェットに制限（JVM デフォルト ~9層、Garnet は 5-8層ネスト） |
| **Escape analysis** | ヒープ割り当てをスカラー置換で除去 | 関数境界を跨ぐと効かない |

**JIT で埋まる部分**: virtual dispatch のコスト（10-25ns → 1-2ns）。Garnet のようにホットパスが**単一型**なら、コールサイトはほぼ monomorphic になるので JIT はかなり効く。

**JIT で埋まらない部分（致命的）**: メモリレイアウト。配列中の struct が連続メモリに flat に並ぶか、バラバラのヒープオブジェクトへの参照配列になるかは、言語レベルの問題で JIT では解決不能。

### 2.2 GraalVM JIT（C2 の代替）

- C2 より優れたインライン化ヒューリスティクスと partial escape analysis
- 2024年の比較で C2 に対し **17-23% のスループット改善**が報告されている ([jvm-weekly.com](https://www.jvm-weekly.com/p/whats-new-in-the-performance-jdk))
- ただし根本的には同じ type erasure 制約の上で動作するため、メモリレイアウト問題は解決不能
- **見込み改善: +15-25%**（dispatch + escape analysis 改善分）

### 2.3 GraalVM Native Image（AOT）

- **Closed-world 前提**: 全コードがビルド時に既知。動的クラスロード不可 ([GraalVM Limitations](https://www.graalvm.org/22.0/reference-manual/native-image/Limitations/))
- **Generic specialization は行わない**: バイトコードが type erasure 済みのため、AOT でも erased なまま。`List<Foo>` と `List<Bar>` の区別はない
- **`sun.misc.Unsafe` サポート**: static final フィールドに格納されたオフセットは自動書き換え対応。ただしパターンによっては制限あり ([GraalVM Compatibility](https://www.graalvm.org/latest/reference-manual/native-image/metadata/Compatibility/))
- **Panama (FFM API) サポート**: GraalVM for JDK 25 で FFM API の強化サポート。Linux/macOS AArch64 対応。`Arena.ofShared()` 実装済み ([GraalVM JDK 25 Release Notes](https://www.graalvm.org/release-notes/JDK_25/))
- **PGO (Profile-Guided Optimization)**: Enterprise Edition のみ。インライン化と分岐予測を改善するが、generic specialization には効かない ([GraalVM PGO](https://www.graalvm.org/latest/reference-manual/native-image/optimizations-and-performance/PGO/))
- **見込み改善**: ウォームアップ解消 + メモリフットプリント削減。定常スループットは C2/GraalVM JIT と同等かやや低下

### 2.4 JIT / GraalVM の総合評価

|                    | virtual dispatch | メモリレイアウト | generic specialization | 総合改善 |
|--------------------|-----------------|----------------|----------------------|---------|
| JVM C2 JIT         | ほぼ解消         | 解決不能        | なし                  | baseline |
| GraalVM JIT        | ほぼ解消         | 解決不能        | なし                  | +15-25% |
| GraalVM Native     | 解消             | 解決不能        | **なし**（erased bytecode のまま AOT） | ウォームアップのみ |
| GraalVM Native+PGO | 解消             | 解決不能        | なし                  | ウォームアップ + 若干の throughput |

**結論**: JIT / GraalVM では Gap の約 30-40% しか埋まらない（dispatch コスト分）。残り 60-70% はメモリレイアウト問題であり、ランタイム最適化では解決できない。GraalVM Native Image は理論上 closed-world で specialization が可能だが、**現行実装では行っていない**。

---

## 3. Project Valhalla の影響

### 3.1 現在のステータス（2026年2月時点、ソース付き）

| JEP | 内容 | 状態 | ソース |
|-----|------|------|--------|
| JEP 401 | Value Classes and Objects | **Submitted**（GA/Preview なし） | [JEP 401](https://openjdk.org/jeps/401) (2025年11月更新) |
| JEP 402 | Enhanced Primitive Boxing | **Draft** | [JEP 402](https://openjdk.org/jeps/402) (2025年11月更新) |
| JEP 218 | Generics over Primitive Types | **Candidate**（未ターゲット） | [JEP 218](https://openjdk.org/jeps/218) |
| JEP 8316779 | Null-Restricted Value Class Types | **Draft** | [JEP draft](https://openjdk.org/jeps/8316779) |
| JEP 8261529 | Universal Generics (Preview) | **Closed / Withdrawn** (2023年9月) | [JEP draft](https://openjdk.org/jeps/8261529) |

**重要な事実**:
- **JDK 23/24/25/26 のいずれにも Valhalla は GA していない** ([JDK 26 Project Page](https://openjdk.org/projects/jdk/26/))
- EA (Early Access) ビルドは存在し、2025年10月更新 ([Valhalla Project](https://openjdk.org/projects/valhalla/))
- **2026年後半に mainline preview マージを目標** と Oracle が公表 ([Inside Java Newscast #104, 2026年1月](https://inside.java/2026/01/08/newscast-104/))
- Preview は最速で **JDK 27 (2026年9月)** 、GA は **2027-2028年** と推定

### 3.2 Valhalla の array flattening: 期待と現実

#### 当初の期待（この文書の旧版での記述）

```java
// 期待: Valhalla 後
value class HashBucket { long e0, e1, e2, e3, e4, e5, e6, e7; }
HashBucket[] buckets = new HashBucket[1024];
// → 1024 × 64byte が連続メモリに flat 化（C# と同等）
```

#### 実際の JEP 401 仕様（2026年2月時点）

**JEP 401 は flat レイアウトを「最適化として許可する」のみであり、保証しない。**

さらに、heap flattening にはアトミック性制約がある:

> フラット化された参照は null フラグを含めてアトミックに読み書きする必要があり、**一般的なプラットフォーム（x86-64/AArch64）では ≤ 64bit に制限される**。
> — [JEP 401](https://openjdk.org/jeps/401)

これは以下を意味する:

| Garnet の struct | サイズ | Valhalla で flat 化？ |
|-----------------|--------|---------------------|
| `ArgSlice` | 12 bytes | **不可**（64bit 超過） |
| `RecordInfo` | 8 bytes | **条件付き可能**（null フラグ込みで 64bit ギリギリ） |
| `SpanByte` (header) | 4 bytes | **可能** |
| `HashBucketEntry` | 8 bytes | **条件付き可能** |
| `HashBucket` (8 entries) | 64 bytes | **不可**（大幅超過） |
| `TxnKeyEntry` | 10 bytes | **不可**（64bit 超過） |
| `AofHeader` | 16 bytes | **不可**（64bit 超過） |
| `EpochEntry` | 64 bytes | **不可**（大幅超過） |

**Garnet のパフォーマンスクリティカルな struct の大半が Valhalla JEP 401 では flat 化されない。**

#### Null-Restricted Types（Draft JEP 8316779）による緩和

[Null-Restricted Value Class Types](https://openjdk.org/jeps/8316779) は null フラグを排除し、より大きな value class の flat 化を可能にすることを目指す:

- `HashBucket!` のように null 不可型を宣言すると、null フラグ分の余裕が生まれる
- 128-bit 程度までの flat 化が視野に入る（プラットフォーム依存）
- ただし **Draft 段階** であり、GA 時期は未定

仮に Null-Restricted Types が出荷されても、64 bytes の `HashBucket` や `EpochEntry` の flat 化は依然として困難。

### 3.3 Generic Specialization の現実

**現時点で JVM の generic specialization は出荷されていない。**

- JEP 218 (Generics over Primitive Types) は **Candidate** のまま、release target なし
- JEP 402 は明示的に「`List<int>` は erasure では `List<Object>` と同等性能」と記載 ([JEP 402](https://openjdk.org/jeps/402))
- Valhalla のパフォーマンスノートも「polymorphic な変数（generic API 含む）では flat 化は通常できない」と明記 ([Valhalla Value Objects](https://openjdk.org/projects/valhalla/value-objects))

つまり:
```java
// Valhalla 後でも
List<MyValueClass> list;  // → 内部的には List<Object> と同じ erased コード
                          // → flat backing array にはならない
                          // → boxing/reference semantics のまま
```

**C# の `List<SpanByte>` が SpanByte 専用のネイティブコードを生成するのとは根本的に異なる。**

### 3.4 Valhalla の解決/未解決マトリクス（修正版）

| 問題 | JEP 401 (Value Classes) | Null-Restricted (Draft) | Universal Generics (未出荷) |
|------|------------------------|------------------------|-----------------------------|
| 小型 value class の flat 化 (≤8byte) | **解決** | — | — |
| 大型 struct の flat 化 (16-64byte) | **未解決**（アトミック制約） | **部分解決**（~128bit まで） | — |
| GC 圧力軽減 | **改善**（スタック/ローカル変数） | **さらに改善** | — |
| プリミティブ boxing in generics | — | — | **解決予定** |
| コンテナの monomorphization | — | — | **未定**（レイアウト特殊化のみ？コード特殊化は不明） |
| `StructLayout(Explicit)` / `FieldOffset` | **未解決** | **未解決** | — |
| `unsafe` ポインタ演算 | **未解決** | **未解決** | — |
| キャッシュライン alignment | **未解決** | **未解決** | — |
| `[ThreadStatic]` | **未解決** | **未解決** | — |

### 3.5 定量的影響予測（リサーチ結果に基づく修正版）

OpenJDK の HashMap ベンチマーク（Kuksenko, 2020）では、inline/value 表現が有効な場合にイテレーションで **~1.5-3x のスループット改善**が報告されている。ただし lookup (`get`) は改善しないか逆に悪化するケースもある ([OpenJDK Benchmark](https://cr.openjdk.org/~skuksenko/valhalla/hashmaps/hash.html))。

|                           | 現行 JVM  | JEP 401 のみ | JEP 401 + Null-Restricted | 全 Valhalla 出荷 | C#/Rust |
|---------------------------|----------|-------------|--------------------------|-----------------|---------|
| メモリレイアウト効率        | 40-50%   | 45-55%      | 55-65%                   | 65-75%          | 100%    |
| Generic dispatch cost     | 70-80%   | 70-80%      | 70-80%                   | 85-90%          | 100%    |
| GC 圧力（per-request）    | 2-5 allocs | 1-3 allocs | 0-2 allocs              | 0-1 allocs      | 0 allocs |
| キャッシュ効率             | 50-60%   | 55-65%      | 60-70%                   | 65-75%          | 100%    |

**旧版からの修正点**: JEP 401 の flat 化制約（~64bit）を反映し、大幅に下方修正。

総合スループット:

|                | 現行 JVM | JEP 401 のみ | 全 Valhalla | C#/Rust  |
|----------------|---------|-------------|-------------|----------|
| vs Redis       | 2.5-4x  | 3-6x        | 4-10x       | 50-100x  |
| vs C# Garnet   | 35-65%  | 40-65%      | 45-75%      | 100%     |

### 3.6 タイムライン予測（確信度付き）

| マイルストーン | 予想時期 | 確信度 | ソース |
|-------------|---------|--------|--------|
| JEP 401 mainline Preview | 2026年後半 (JDK 27) | **中** | [Inside Java](https://inside.java/2026/01/08/newscast-104/) |
| JEP 401 GA | 2027-2028 | **低-中** | Preview 複数ラウンド必要（推定） |
| Null-Restricted Types Preview | 2027以降 | **低** | Draft 段階 |
| Universal Generics Preview | 不明 | **非常に低** | Candidate のまま、Withdrawn された前身あり |
| Kotlin の Valhalla 対応 | Valhalla GA 後 | **低** | [DevClass Interview](https://www.devclass.com/development/2024/11/21/interview-with-kotlin-lead-designer-how-far-will-the-language-diverge-from-java/1625354) |

### 3.7 エコシステム対応状況

- **主要ライブラリ (Netty, Guava, Eclipse Collections, Chronicle)**: JEP 401 が GA でないため、正式採用の発表はない
- **Kotlin**: `@JvmInline value class` は存在するが、Valhalla の value class とは異なる。JetBrains は Valhalla 対応に「eager」だがタイムライン未コミット
- **IntelliJ IDEA**: Valhalla value class のシンタックス認識は開始 ([YouTrack](https://youtrack.jetbrains.com/articles/IDEA-A-2100662348/IntelliJ-IDEA-2025.1-EAP-1-251.14649.49-build-Release-Notes))

### 3.8 結論（リサーチに基づく修正版）

**Valhalla の Garnet 再実装への影響は、当初の期待より大幅に限定的。**

理由:
1. **Array flattening は ~64bit に制限**され、Garnet の主要 struct（HashBucket 64B, EpochEntry 64B, AofHeader 16B, TxnKeyEntry 10B, ArgSlice 12B）の大半が恩恵を受けない
2. **Generic specialization は未出荷**で、`List<MyValueClass>` は依然として erased
3. **GA までの道のりが長い**（Preview 2026後半 → GA 2027-2028 → Kotlin 対応はさらに後）
4. **明示的メモリレイアウト制御は提供されない**（JEP 401 は明示的にレイアウト保証を非ゴールとしている）

ただし、Valhalla は**スタック割り当て、ローカル変数、小型 value の GC 圧力軽減**では確実に改善をもたらす。「Redis 比 4-10x」は Valhalla GA 後に現実的な数値となる。

---

## 4. Gap 解消率の総合比較（最終版）

| 対策 | Gap 解消率 | 残存 Gap | 確信度 |
|------|-----------|---------|--------|
| JIT 最適化のみ（現行） | ~30-40% | メモリレイアウト全般 | 高 |
| GraalVM JIT | ~35-45% | 同上 + dispatch 若干改善 | 中-高 |
| GraalVM Native Image | ~30-40% | 同上（ウォームアップのみ改善） | 中-高 |
| JEP 401 (Value Classes) | ~35-45% | 大型 struct flat 化不可、generic erasure | 中 |
| JEP 401 + Null-Restricted | ~45-55% | 64B超の flat 化不可、generic erasure | 低-中 |
| 全 Valhalla 出荷 | ~55-65% | `FieldOffset`, ポインタ演算, TLS, alignment, 完全 monomorphization | 低 |
| 全 Valhalla + Panama 成熟 | ~60-70% | 完全 monomorphization, インライン化深度 | 非常に低 |
| **理論的上限** | **~70%** | **言語設計レベルの差は残る** | — |

**旧版からの修正**: 理論的上限を ~80% → ~70% に下方修正。JEP 401 の flat 化制約が当初の想定より厳しいため。

---

## 5. 再実装言語選定への示唆（最終版）

| 性能要件 | 推奨言語 | 根拠 |
|---------|---------|------|
| Redis 比 50-100x（Garnet 同等） | **Rust** | monomorphization + unsafe + no GC が必須 |
| Redis 比 10-30x | **Rust** または **C# 継続** | JVM では全 Valhalla 出荷後でも困難 |
| Redis 比 4-10x | **Kotlin/JVM（Valhalla GA 後）** | value class (小型) + Netty で実用域 |
| Redis 比 2-5x | **Kotlin/JVM（現行）** | 現行 JVM + Netty + off-heap で達成可能 |
| 開発速度優先 | **Kotlin/JVM** | エコシステム成熟度、GC による安全性 |
| ハイブリッド | **Rust（Storage+Parser）+ Kotlin（Cluster+Config）** | 性能と開発速度のバランス。Panama FFM で接続 |

### Kotlin/JVM がストレージエンジン層で viable になる条件

以下の**すべて**が満たされた場合のみ:

1. JEP 401 が mainline GA ([Inside Java](https://inside.java/2026/01/08/newscast-104/))
2. Null-Restricted Value Types が出荷（16B+ の struct flat 化に必須）([JEP draft](https://openjdk.org/jeps/8316779))
3. Specialized Generics が出荷（`List<T>` の boxing 解消に必須）([JEP 218](https://openjdk.org/jeps/218))
4. Kotlin コンパイラ/stdlib が Valhalla types を正式サポート ([DevClass](https://www.devclass.com/development/2024/11/21/interview-with-kotlin-lead-designer-how-far-will-the-language-diverge-from-java/1625354))
5. 明示的 struct レイアウト/alignment なしで許容できる（または off-heap で代替）

**現実的にすべてが揃う時期: 2028-2030年**。それまでは Rust がストレージエンジン層の推奨言語。

---

## 参考文献

- [JEP 401: Value Classes and Objects](https://openjdk.org/jeps/401)
- [JEP 402: Enhanced Primitive Boxing](https://openjdk.org/jeps/402)
- [JEP 218: Generics over Primitive Types](https://openjdk.org/jeps/218)
- [JEP draft 8316779: Null-Restricted Value Class Types](https://openjdk.org/jeps/8316779)
- [Valhalla Project](https://openjdk.org/projects/valhalla/)
- [Valhalla Value Objects Performance Notes](https://openjdk.org/projects/valhalla/value-objects)
- [Inside Java Newscast #104 (2026-01-08)](https://inside.java/2026/01/08/newscast-104/)
- [OpenJDK HashMap Benchmark (Kuksenko, 2020)](https://cr.openjdk.org/~skuksenko/valhalla/hashmaps/hash.html)
- [JDK 26 Project Page](https://openjdk.org/projects/jdk/26/)
- [GraalVM Compatibility](https://www.graalvm.org/latest/reference-manual/native-image/metadata/Compatibility/)
- [GraalVM JDK 25 Release Notes](https://www.graalvm.org/release-notes/JDK_25/)
- [GraalVM FFM API](https://www.graalvm.org/jdk25/reference-manual/native-image/native-code-interoperability/ffm-api/)
- [GraalVM PGO](https://www.graalvm.org/latest/reference-manual/native-image/optimizations-and-performance/PGO/)
- [GraalVM Native Image Limitations](https://www.graalvm.org/22.0/reference-manual/native-image/Limitations/)
- [Kotlin Lead Designer Interview (DevClass, 2024-11)](https://www.devclass.com/development/2024/11/21/interview-with-kotlin-lead-designer-how-far-will-the-language-diverge-from-java/1625354)
