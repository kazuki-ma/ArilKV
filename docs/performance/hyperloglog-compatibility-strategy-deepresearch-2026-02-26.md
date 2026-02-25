# Redis/Valkey互換 HyperLogLog（PF*）の互換戦略とRustライブラリ決定

## Executive Summary

- Redis/ValkeyのHyperLogLogは「内部型」ではなく**RedisのStringとして直列化可能**で、`GET`で取り出して`SET`で復元できる（=オンワイヤ表現の互換がそのまま互換性になる）。citeturn33view0turn33view1turn33view3  
- 互換性の肝は、**疎（sparse）/密（dense）二重表現**、**16バイトヘッダ**（magic/encoding/キャッシュcardinality）と、**cardinalityキャッシュの無効化フラグ（MSB）**を含む振る舞いを再現すること。citeturn33view1turn33view4  
- 既存の一般的なRust HyperLogLogクレートは、HLLアルゴリズム実装としては有用でも、Redis/Valkeyの**バイトレベル互換のエンコーディング（特にsparse）や`PFDEBUG`の期待**を満たすのは現実的に難しい（互換要件が“データ構造”だけでなく“表現”に及ぶため）。citeturn33view1turn33view4turn33view2  
- したがって最短で互換性リスクを下げる道は、**Valkey/Redis互換のHLL表現・更新・検証を内製**（Rustでメモリ安全に再実装）し、上流の仕様（docs + ソース挙動）に合わせてテストで固める方針。citeturn33view1turn33view4turn39view0turn41view0  
- `PFCOUNT`は**cardinalityキャッシュ更新の副作用で“技術的にはwrite”**になり得るため、レプリケーション/永続化や「更新判定」周りに注意が必要。citeturn33view1  
- sparse→denseの切替は概念ではなく**具体的な閾値（`hll-sparse-max-bytes`。ヘッダ16バイトを含む）**で制御されるため、その同等機構が必要。citeturn20search4  
- “不正Payload耐性”は互換性だけでなく安全性（DoS/RCE耐性）でも必須で、実際にHLL処理起因のOOB書き込み脆弱性が報告されているため、**境界チェックと検証モデルを設計の中心に置くべき**。citeturn17search32  
- 移行は、現行の`garnet-pf-v1`（正確集合）から新HLLへ、**読み取り時変換（write-on-touch）＋明確なロールバック/キルスイッチ**が現実的（ただし大きな集合キーは最悪O(n)で重い）。citeturn33view0turn37view0turn39view0  

## Candidate Matrix

| Candidate/Approach | Maintenance status | License | Compatibility feasibility (Redis/Valkey `PF*`) | Performance/memory expectations | Operational risk | Adopt now / pilot / avoid |
|---|---|---|---|---|---|---|
| **内製: Valkey/Redis互換HLL（sparse/dense表現＋PF*挙動をRustで再実装）** | 自社で継続（上流仕様追従が主作業） | 自社実装（依存は最小化） | **高**（GET/SET互換・キャッシュ・切替・`PFDEBUG`まで狙える）citeturn33view1turn33view0turn33view4turn33view2 | Redis/Valkey同等の12KB上限（dense）、小規模ではsparseで大幅節約が可能（同等設計なら）citeturn33view1turn33view4turn33view0 | 実装難度は高いが、Rust化でメモリ安全。検証不足が最大リスク。citeturn17search32turn33view1 | **Adopt now** |
| **ハイブリッド: 互換エンコード/デコードは内製、レジスタ更新・推定計算だけクレート利用** | クレートに依存（更新追従が必要） | クレート次第 | **中**（推定器/ハッシュ/補正が上流と一致しないとズレる。オンワイヤ互換は別途実装必須）citeturn33view1turn33view4 | 計算部分の最適化を享受できる可能性。だが互換層が支配的になりやすい。citeturn35view0 | 「結果のズレ」が互換性バグとして顕在化しやすい。citeturn33view1turn35view0 | **Pilot**（ただし互換最優先なら主系にはしない） |
| `hyperloglockless`（Rustクレート）をPF*の実装の核に採用 | **活発**（コミットが2026-02まで見える）citeturn36view0 | MIT / Apache-2.0 citeturn35view0 | **低**（自前のsparse表現はあるがRedis/Valkeyのバイト表現とは別物。`PFDEBUG`/GET互換は満たしにくい）citeturn35view0turn33view1turn33view2 | 高性能・並行（Atomic）等の強み。精度モデルも独自に進化。citeturn35view0 | 互換テスト不合格リスクが高い（“正しいHLL”でも“Redis互換”でない）。citeturn33view1turn35view0 | **Avoid**（PF互換目的では） |
| `hyperloglog`（jedisct1/rust-hyperloglog）採用 | コミットが2025-12まで見えるciteturn10view0 | リポジトリ検出はBSD-2-Clause、クレートメタデータはISC表記（要確認）citeturn9view0turn8view0 | **低**（Redis/Valkeyのsparse/dense表現やキャッシュcardなどは別実装になる）citeturn33view1turn9view0 | 一般用途のHLLとしては軽量。だが互換層が別途必要。citeturn9view0turn33view1 | ライセンス表記差異の確認コスト＋互換ギャップの実装コストが残る。citeturn9view0turn8view0turn33view1 | **Avoid**（PF互換目的では） |
| `hyperloglog-rs` 採用 | 注意点として「sparse registers未実装」等が明記されているciteturn6view2 | MIT citeturn6view2 | **低**（sparse関連・互換エンコード・`PFDEBUG`を満たしにくい）citeturn6view2turn33view1turn33view2 | 一般用途の推定器としては可。citeturn6view2 | 機能ギャップが明確で、互換実装がほぼ別物になる。citeturn6view2turn33view1 | **Avoid** |
| `rust-hll` 採用 | 公開情報からはタグ/リリースが弱く、コミット数も小さいciteturn7view0 | MIT citeturn6view0 | **低**（目的が“AK HLL storage spec”寄りでRedis互換ではない）citeturn6view3turn33view1 | 互換目的では評価対象外に近い。citeturn6view3turn33view1 | 互換要件を満たさないリスクが支配的。citeturn33view1turn6view3 | **Avoid** |

## Recommended Direction

### Primary recommendation

**Valkey/Redis互換のHLL表現（sparse/dense）とPF*挙動をRustで内製し、オンワイヤ互換（GET/SETでのバイト列互換）を“仕様”として固定する。**citeturn33view0turn33view1turn33view4  

根拠は、互換要件が「HLL推定のアルゴリズム」だけではなく、(a) 16バイトヘッダ＋二重表現、(b) cardinalityキャッシュと無効化、(c) sparse→dense切替閾値、(d) `PFDEBUG`系のテスト用内蔵コマンド、(e) 破損検出/安全性、という**データ表現とエッジケース**に強く依存しているためです。citeturn33view1turn33view2turn20search4turn17search32  

### Fallback recommendation

**短期で互換を詰める必要が強い場合は「互換のエンコード/デコードと検証は内製」した上で、推定計算の内部だけをクレートで置換できる余地を残す（ただし互換最優先なら“最後まで置換しない”可能性を前提にする）。**citeturn33view1turn35view0  

なぜなら、推定器部分（HLL++/補正モデル/ハッシュの扱い）はクレートごとに差が出やすく、Redis/Valkey互換では「同じ入力→同じレジスタ更新→同じ表現（時に同じ結果）」を強く求められるため、結果のズレが互換失敗として顕在化しやすいからです。citeturn33view1turn39view0turn41view0  

## Implementation Blueprint

### Data representation strategy

Redis/Valkey互換を狙う場合、まず“データはString”という前提をそのまま採用し、キーの値として **`HYLL` magic + encoding + reserved + cached cardinality + payload** の構造を保持するのが最も近道です。citeturn33view1turn33view4turn33view0  

- **二重表現（sparse/dense）**  
  - sparse: “ゼロが多いレジスタ列”を効率的に表すランレングス型（run-length）で、要素が少ないうちは小さく保てる。citeturn33view1turn33view4  
  - dense: **12288バイトで16384個の6-bitカウンタ**（= 16384レジスタ）を保持する固定長表現。citeturn33view1turn33view4  

- **16バイトヘッダ**  
  - magic・encoding/versionに加え、**cardinality推定のキャッシュ**をlittle-endianで持つ。さらに、**MSB=1なら“キャッシュ無効（更新済み）”**という無効化フラグを含む。citeturn33view1turn33view4  

- **sparse→dense切替**  
  - `hll-sparse-max-bytes`（ヘッダ16バイト込みの上限）を超えたらdenseに変換する、という運用上の閾値が存在する。citeturn20search4  

実装上は、(A) “値バイト列を真実”として**バイト列を直接更新**し続ける方式（Redisに近く、GET互換が自然）か、(B) 内部構造体に展開してから再エンコードする方式が考えられます。互換優先・事故リスク最小化の観点では、(A)の方が「再エンコード時の差分（正規化）が互換を壊す」リスクを減らしやすいです。citeturn33view0turn33view1  

### Command semantics mapping

`PFADD` / `PFCOUNT` / `PFMERGE` の外形仕様は、Redis/Valkeyのコマンド仕様に一致させ、内部でヘッダキャッシュとエンコード切替の副作用を忠実に再現します。citeturn39view0turn33view1turn41view0turn37view0  

- `PFADD`  
  - キーが存在しなければ空HLLを作成する。citeturn39view0turn37view0  
  - 要素なし呼び出し（`PFADD key`）は正当で、既存ならno-op、未作成なら作成だけ行う（その場合は`1`）。citeturn39view0turn37view0  
  - 返り値：推定cardinalityが変化した（内部レジスタが少なくとも1つ更新された）なら`1`、変化なしなら`0`。citeturn39view0turn37view0  
  - 互換上重要：レジスタ更新が起きたら、ヘッダのcardinalityキャッシュを“無効”にする（MSB）。citeturn33view1turn33view4  

- `PFCOUNT`  
  - 単一キー：存在しなければ`0`。citeturn33view1  
  - 複数キー：与えられた複数HLLを**一時的にマージ**した上でunionの推定値を返す。citeturn33view1  
  - 互換上の罠：`PFCOUNT`はcardinalityキャッシュ更新の副作用があり得るため、**“技術的にはwrite”**になり得る。citeturn33view1  
  - パフォーマンス特性（互換テストに直結しやすい）：単一キーはキャッシュにより高速になり得るが、複数キーはオンザフライマージで遅い。citeturn33view1  

- `PFMERGE`  
  - 複数HLLのunionをdestinationに格納し、destinationが無ければ作成する。citeturn41view0  
  - destinationが既に存在する場合、それもソース集合として扱いunionに含める。citeturn41view0  
  - 返り値は`OK`。citeturn41view0  
  - 実装の注意：マージ結果のエンコーディング（sparseのまま維持するか、dense化するか）は互換差分になりやすいので、`hll-sparse-max-bytes`と同じ判定軸で切替させる。citeturn20search4turn33view1  

- `PFDEBUG` / `PFSELFTEST`  
  - `PFDEBUG`はRedis/Valkeyともに内部コマンドとして定義され、開発/テスト用途であるため、互換テスト（`tests/unit/hyperloglog.tcl`相当）に合わせたサブコマンド実装が必要。citeturn33view2turn12search7  
  - 現状の失敗テーマ（ENCODING、TODENSE、GETREG系）から逆算すると、少なくとも「現在のエンコード種別の報告」「dense化トリガ」「レジスタ参照（インデックス→値）」に相当する面を揃えるのが最短ルート。citeturn33view2turn33view1  

### Corruption handling and validation model

Redis/Valkeyは「壊れたHLLを操作してもサーバ安定性に影響しない」ことを明示しており、特にsparse破損は多くの場合エラーとして検出される、としているため、互換実装でも“壊れた入力を安全に扱う”のは必須です。citeturn33view1turn33view4  

さらに、HLL処理系に起因する境界外書き込みが実際にセキュリティ問題として報告されているため、**「エンコードの検証＝セキュリティ境界」**として、RustでもCPU/メモリDoSを含めた堅牢化が必要です。citeturn17search32  

推奨モデルは次の通りです（“Redis/Valkey互換”と“安全”の両立を狙う）:

- **早期拒否（fail-fast）**:  
  - magic/encoding/長さ不整合を検出したら、推定値を返すのではなく明示的エラー（少なくともsparse系）を返す。citeturn33view1turn33view4  
- **境界チェック付きパーサ**:  
  - sparseのrun-lengthデコードは「入力を1バイトずつ消費」「レジスタ総数が16384に一致」「各オペランドが仕様範囲内」「読み取りが入力終端を越えない」を満たさない場合に即エラー。citeturn33view1turn17search32  
- **一時領域の上限**:  
  - `PFCOUNT`複数キーの一時マージは仕様上必要だが、キー数や処理量に比例して遅くなるため、実装では“割り込み不能な巨大作業”にならないよう、入力数上限やタイムスライスの検討余地を残す（互換レベルでの挙動差が出ない範囲で）。citeturn33view1  

## Migration Plan

### Placeholder-format transition steps

現状の`garnet-pf-v1`（exact set）を、互換HLLへ移行する際の現実的な段取りは「既存キーをそのまま残しつつ、PF*実行時に新形式へ変換」です。

- **判別**: 値がStringであり、かつ先頭が`garnet-pf-v1`であれば旧形式、`HYLL`であれば互換HLL、といった判別分岐を設ける（互換HLLはStringとしてGET/SETされ得るため、この識別は必須）。citeturn33view0turn33view1  
- **変換（write-on-touch）**: `PFADD` / `PFCOUNT` / `PFMERGE` / `PFDEBUG`のいずれかが旧形式キーに触れたタイミングで、旧集合の全要素を新HLLへ`PFADD`相当で投入し、新形式で上書きする（最悪O(n)のコストは受容し、観測と制御を入れる）。citeturn39view0turn37view0  
- **巨大キー対策**: 旧形式セットのサイズが大きいと変換が重いので、(a) 変換を拒否して明示エラーにする閾値、(b) 変換中は段階的に進める仕組み（可能なら）、(c) 変換後の推定誤差への周知、を計画に含める。citeturn33view0turn33view1  

### Backward-compat policy

互換性の最終目標がRedis/Valkeyの`PF*` parityである以上、**新形式は`HYLL`互換のStringで保存**し、`GET`/`SET`での相互運用を壊さないことを後方互換の中心に置きます。citeturn33view0turn33view1turn33view3  

旧形式（`garnet-pf-v1`）は「移行期間のみ読み取り可能」と割り切り、一定期間後に読み取りサポートを削除できるよう、メトリクス（旧形式キー残数、変換失敗数、変換時間）を必須にします。

### Rollback strategy

ロールバックは“データ形式”が絡むため、次の二段階が現実的です。

- **キルスイッチ**:  
  - 新HLLへの変換を無効化し、旧形式キーは従来のexact動作（互換は失う）に戻すフラグを用意する。  
- **二重保持（必要時のみ）**:  
  - どうしても旧バージョンへ戻せる必要がある期間は、旧形式のスナップショットを保持する（メモリ/容量増のコストと引き換え）。  
  - ただし最終的にRedis/Valkey互換を最優先するなら、二重保持は短期間に限定する。

## Acceptance Test Plan

### Must-pass compatibility and robustness checks

互換到達のゲートは「コマンド結果」だけでなく「表現」「壊れた入力」に置くべきです（HLLはStringであり、ユーザが`SET`で直接payloadを注入できるため）。citeturn33view0turn33view1turn33view4  

最小の必須ゲートは次です。

- **互換テスト（機能）**:  
  - `PFADD`の返り値（`1/0`、要素無し呼び出しの扱い）をRedis/Valkeyと一致させる。citeturn39view0turn37view0  
  - `PFCOUNT`単一/複数キー挙動、キャッシュ副作用（cardinalityキャッシュ更新）を含め一致させる。citeturn33view1turn33view4  
  - `PFMERGE`のdestination取り扱い（既存destをsourceに含める）と`OK`応答を一致させる。citeturn41view0  
  - `PFDEBUG`は内部コマンドである前提の上で、互換テストが参照するサブコマンド（ENCODING/TODENSE/GETREG系）を通す。citeturn33view2turn12search7  

- **互換テスト（表現）**:  
  - `GET`→`SET`でHLLが復元できること（“HLLがStringである”契約の再現）。citeturn33view0turn33view1turn33view3  
  - sparse→dense切替が設定閾値（`hll-sparse-max-bytes`）と整合して起きること。citeturn20search4turn33view1  
  - cardinalityキャッシュの無効化（ヘッダMSB）と更新タイミングが一致すること。citeturn33view1turn33view4  

- **破損/悪意入力（安全性）**:  
  - ランダムバイト列、短い/長すぎる値、magic不一致、encoding不正、sparse run-length不正などを`SET`した上で`PFADD`/`PFCOUNT`/`PFMERGE`を実行し、**panic/abort/無限ループ/過大アロケーションが起きない**こと。citeturn33view1turn17search32  
  - sparse破損は高確率でエラーとして検出される、という上流の期待に近づける（テスト化）。citeturn33view1turn33view4  
  - 既知のHLL処理系脆弱性（境界外書き込み等）の再発防止として、境界チェックの単体テストとfuzzを必須化する。citeturn17search32  

### Performance guardrails

- `PFADD`は要素ごとO(1)であること（高スループット運用の前提）。citeturn39view0turn37view0  
- `PFCOUNT`単一キーがキャッシュにより高速化され得る前提を崩さない（キャッシュの設計が崩れると“互換は合っているが遅い”になりやすい）。citeturn33view1  
- 複数キー`PFCOUNT`や`PFMERGE`が高コストなのは仕様通りだが、巨大入力でのCPUスパイクを観測（メトリクス化）し、運用上の保護策（タイムアウト/上限制御）とセットでリリースする。citeturn33view1turn41view0  

## References

- Redis Docs: HyperLogLog（HLLがStringとして`GET`/`SET`可能、メモリ上限・誤差など）citeturn33view0  
- Redis Docs: `PFCOUNT`（表現仕様の要約、16バイトヘッダ、キャッシュMSB、破損時の安定性）citeturn33view1  
- Redis Docs: `PFADD`（返り値・要素無し呼び出しの扱い）citeturn39view0  
- Redis Docs: `PFMERGE`（destinationをsourceに含める挙動、返り値`OK`）citeturn41view0  
- Redis Docs: `PFDEBUG`（内部コマンドであること）citeturn33view2  
- Valkey Docs: HyperLogLog（HLLがStringである点など）citeturn33view3  
- Valkey Docs: `PFCOUNT`（表現仕様の要約）citeturn33view4  
- Valkey Docs: `PFADD`（返り値・要素無し呼び出しの扱い）citeturn37view0  
- Redis config（`hll-sparse-max-bytes`の設定ディレクティブ。ヘッダ16バイト込み）citeturn20search4  
- Rust HLL候補: `tomtomwombat/hyperloglockless`（機能・ライセンス・メンテ状況）citeturn35view0turn36view0  
- Rust HLL候補: `jedisct1/rust-hyperloglog`（機能・ライセンス・メンテ状況）citeturn9view0turn10view0turn8view0  
- Rust HLL候補: `hyperloglog-rs`（既知の未実装事項・ライセンス）citeturn6view2  
- Rust HLL候補: `rust-hll`（公開情報ベースの成熟度）citeturn7view0turn6view0  
- Redis Security Advisory（HLL処理に関する境界外書き込みの報告）citeturn17search32