# Redis互換Rustサーバー向け OIDC JWT `AUTH` / Workload Identity 設計・実装調査

## Executive Summary

- Redis互換の `AUTH username <jwt>` / `HELLO ... AUTH username <jwt>` に「**受信JWT（Bearer）を検証してACLユーザーへマップする**」経路を追加するのは、Redisでも「パスワード以外の追加認証コールバック」を試行する設計が存在するため、互換思想として自然です。citeturn14search2turn14search3turn14search26  
- 最小の実装面積で「サーバー側の受信JWT検証」だけに集中するなら、**OIDCリライングパーティ（ブラウザ・リダイレクト等）機能を含むライブラリは過剰**になりやすく、JWT/JWKにフォーカスしたライブラリ＋最小限のJWKS管理を自前実装するのが保守・運用上有利です。citeturn8view0turn8view4  
- Rustの候補の中では、**`jsonwebtoken`が「成熟度・直近の更新・JWK/JWKS型の内蔵・暗号バックエンド選択の余地」**の点で、受信検証用途の“デフォルト”に最も置きやすいです。citeturn5search0turn8view4turn2search0  
- セキュリティ既定値は **RFC 8725（JWT BCP）に沿って「アルゴリズム固定（allow-list）」「`none`拒否」「入力サイズ上限」「`iss`/`aud`/`exp`/`nbf`の厳格検証」**を基本にすべきです。citeturn13search8turn22view0turn7search30  
- OIDC Discovery / メタデータの利用は便利ですが、**issuerミスマッチや偽メタデータによる“なりすまし”**を避けるため「取得元URLに対する `issuer` の完全一致検証」などを必須にし、リダイレクト禁止などSSRF対策も前提にする必要があります。citeturn26view0turn25view0turn8view0  
- 初期スライスは **ローカルJWKSファイル**（＋手動/明示リロード）で「運用の単純さ」と「HTTP面の増加回避」を優先し、終了形として **Issuer固定＋Discoveryで`jwks_uri`派生＋キャッシュ/更新**へ段階的に進めるのが妥当です。citeturn26view0turn25view4turn14search19  
- JWKS更新は “常時バックグラウンド更新”一択ではなく、**(a) 起動時ロード、(b) `kid`未解決時のオンデマンド更新（レート制限付き）、(c) HTTPキャッシュ指示に従う最小周期の更新**の組み合わせが、運用負荷と更新の堅牢性のバランスを取りやすいです。citeturn15search0turn14search19turn25view4  
- ACLマッピングは既定で **「要求された`AUTH username` と、JWTから得たprincipalが一致しない限り拒否」**が最も安全（権限昇格を防ぐ）で、将来は「明示的マッピング表」や「クレーム→ユーザー名テンプレート」を追加するのが良いです。citeturn7search30turn13search8  
- ワークロードIDの相互運用は、entity["company","Microsoft","software company"] Entra系（OIDCメタデータ/JWKS、`kid`、特定audience）、entity["company","Google","internet company"]（メタデータサーバーIDトークン、Kubernetes SAトークンの`sub`形式）、entity["company","Amazon Web Services","cloud provider"]（`aud`/`azp`扱い、IRSAのJWKS取得、thumbprint/CA信頼）などで“落とし穴”が異なるため、Phase 3でプロバイダ別プロファイルを入れるのが現実的です。citeturn16view0turn15search0turn17search0turn19search25turn18search13turn18search3turn18search25  

## Candidate Matrix

| Candidate | Maintenance status | License | Scope fit for server-side JWT verification | Dependency / ops burden | JWKS / discovery support | Security posture / notable caveats | Adopt now / pilot / avoid |
|---|---|---|---|---|---|---|---|
| `jsonwebtoken` | crates.io上で2026-01-27更新（v10.3.0）。citeturn5search0turn8view4 | MIT。citeturn8view4 | JWTの作成/検証が主目的。`decode`で検証・バリデーション、JWK/JWKS型も同梱。citeturn8view4turn6search36turn2search0 | 中程度。暗号依存をoptionalに分離し、Crypto provider選択の仕組みがある。citeturn8view4 | JWK/JWKS“型”はあるが、Discovery/JWKS取得/更新は自前が基本。citeturn8view4 | `dangerous`系APIが存在するため誤用ガードが必要（使用禁止方針推奨）。`exp`/`nbf`等の検証はValidation設定に依存。citeturn8view4turn2search0 | Adopt now |
| `jwt-simple` | GitHub Releasesで2026-01-20に0.12.14。citeturn6search3turn23search3 | ISC。citeturn7search13turn23search7 | “JWTのセキュリティ落とし穴を避ける”を明示し、JWS/JWEまで含む総合実装。citeturn6search7turn23search1 | やや広い（JWE等の機能面積が増える）。ただし受信検証に必要な部分だけ使う運用は可能。citeturn23search1 | 本体はJWKS/discoveryのHTTP更新機能を必ずしも内蔵しない。周辺にJWKS向けクレートがある（互換限定）。“取得/更新ロジックの責務”は設計判断が必要。citeturn7search3turn23search2 | セキュリティ志向だが、受信検証だけの用途には機能過多になりやすい。プロファイルを絞って運用する設計/レビューが重要。citeturn6search7turn13search8 | Adopt now（ただし用途限定のガイド必須） |
| `jwt-compact` | crates.io上でv0.8.0が2023-10-12。citeturn12search0turn9view3 | Apache-2.0。citeturn9view3 | 受信検証に“ちょうど良い”ミニマル設計。JWKの基本機能も含む。citeturn8view3turn23search17 | 低〜中。純Rust暗号（RSA/p256等）を使い、no_stdも意識。citeturn8view3turn12search1 | Discovery/JWKS取得更新は自前。JWKモジュールはある。citeturn8view3turn23search17 | `alg`をHeaderから外部に露出しない設計で“algorithm switching attacks”を抑止する方針を明示。citeturn8view3turn13search8 | Pilot（設計は優秀だが更新頻度が相対的に低い） |
| `openidconnect` | GitHub Releasesで2025-07-06。citeturn6search1turn9view0 | MIT。citeturn9view0 | OIDC全体（RPフロー、Discovery、ID token検証等）を扱えるが、今回の「受信JWT検証のみ」には過剰。citeturn8view0turn7search12 | 高め。HTTPクライアント選択肢と既定の`reqwest`依存など（機能面積も大きい）。SSRF対策（リダイレクト禁止）注意が明記。citeturn8view0turn9view0 | Discovery/Provider metadata/JWKS周りの型と概念は揃う（ただし“プロトコル実装”寄り）。citeturn8view0turn7search12 | SSRFや秘密比較等、セキュリティ注意が公式ドキュメントで強調されている。とはいえ今回の最小要件には広すぎる。citeturn8view0turn9view0 | Avoid（本用途では過剰） |
| `josekit` | crates.io上で0.10.3（2025-05-20更新表示）。OpenSSLベースを明示。citeturn5search2turn6search6turn6search34 | MIT OR Apache-2.0。citeturn6search34turn10search6 | JOSE全体（JWT/JWS/JWE/JWK等）を広く扱う。受信JWT検証のみには過剰で、OpenSSL依存が運用面を難しくしやすい。citeturn6search6turn6search14 | 高め（OpenSSL 1.1.1+のシステム依存）。コンテナ/OS差分、脆弱性対応、FIPS要件など運用論点が増える。citeturn6search6turn2search3 | JWK/JWKS型はあるが、Discoveryや更新は基本自前。citeturn6search14turn26view2 | OpenSSL依存を許容できる組織なら選択肢だが、今回の「運用負荷最小」方針とは逆方向。citeturn6search6turn13search8 | Avoid（運用負債が出やすい） |

## Recommended Default

**Primary choice: `jsonwebtoken` + 自前の最小JWKSマネージャ（ファイル/URL/Discoveryを段階導入）**  
`jsonwebtoken`はJWT検証の中核として成熟しており、JWK/JWKSの型が同梱され、暗号プロバイダを抽象化しているため「将来の暗号バックエンド変更」「用途限定の安全なAPIサーフェス設計（dangerousを封印）」がやりやすいです。citeturn8view4turn2search0turn13search8  
また、Redis互換の `AUTH` / `HELLO ... AUTH` の位置づけ（接続単位で認証状態を切り替える）に対して、受信検証のCPU負荷も“認証時のみ”に局所化でき、運用上の見通しが立てやすいです。citeturn14search2turn14search3  

**Fallback choice: `jwt-simple`（受信検証用途に限定して使用） + JWKS取得/更新は同じ自前JWKSマネージャ**  
`jwt-simple`は「JWTの一般的な落とし穴を避ける」ことを明記しており、メンテも直近で継続しています。citeturn6search7turn6search3  
一方でJWEまで含むため、採用時は「受信検証に必要なアルゴリズム/機能だけを有効化・露出する」という設計制約（社内ガイド＋lint/レビュー）を強く推奨します。citeturn23search1turn13search8  

**Why（判断軸への適合）**  
- 「受信JWT検証」だけに必要な表面積へ絞りやすい（OIDCフロー/ブラウザ要素不要）。citeturn8view0turn14search2  
- RFC 8725（BCP）に沿った安全既定値（alg固定、`none`拒否、入力検証、`iss`/`aud`/時間クレーム検証）を“サーバー側で一元実装”しやすい。citeturn13search8turn7search30  
- Discovery/JWKS更新という“HTTP面”は、セキュリティ要件（issuer完全一致、TLS証明書検証、リダイレクト禁止等）を満たすと途端に設計が難しくなるため、ライブラリ任せにせず自前で制御ポイントを持つほうが長期運用に向く。citeturn25view0turn26view0turn8view0  

## Validation Blueprint

**JWT verification flow（`AUTH username <jwt>` / `HELLO ... AUTH username <jwt>`）**  
1) **キルスイッチ判定**: `auth.jwt.enabled=false` なら既存のACLパスワード認証のみを実行し、JWT経路に入らない（即時ロールバック可能）。citeturn14search2turn14search3turn14search26  
2) **入力ガード**: トークン（第二引数）の最大サイズ（例: 8〜16KiB）・ASCII/UTF-8妥当性・`.`, Base64URL構造をチェックし、明らかな異常はパース前に拒否（DoS抑止）。JWTは3セグメントであることが基本です。citeturn7search30turn15search0  
3) **“解析のみ”フェーズ**: ヘッダーとクレームをデコードして `iss`/`aud`/`exp`/`nbf`、およびヘッダーの`kid`等を抽出（ただしこの時点では**未検証**として扱う）。citeturn13search2turn7search30  
4) **Issuerポリシー選択**: サーバー設定（単一issuer or issuer集合）に基づき、`iss`が許可されるissuerかを検証。OIDC Discoveryを使う場合は“メタデータ取得元issuerとドキュメント内issuerの完全一致”が必須です。citeturn26view0turn25view0turn21view0  
5) **JWKS解決**: issuerに紐づくローカルキャッシュ（JWKS）から公開鍵候補を取得。`kid`がある場合はまず一致keyを試し、なければ（a）キー総当たりは上限数を設けて許可、（b）必要ならオンデマンド更新へ進む。`kid`は鍵選択のヒントとして一般に利用されます。citeturn15search0turn18search3turn13search1  
6) **署名検証（JWS）**: “許可されたアルゴリズム一覧（allow-list）”に基づき、署名を検証。JWT/JWSでは`alg`ヘッダーは保護ヘッダーに含まれる前提で、受信側はサポートされない/鍵がない場合に無効と扱うべきです。citeturn13search2turn13search6turn13search8  
7) **クレーム検証**: 署名が通った後に、時間・issuer・audience等の検証を行う（順序を固定して監査しやすくする）。OIDC ID Token検証規則（`iss`完全一致、`aud`にclient_id含有、`exp`等）も参考になります。citeturn21view0turn7search30  
8) **principal抽出→ACLユーザー決定→接続状態更新**: principal（例: `sub`）を得てACLユーザーへマップし、接続の認証状態を更新。Redisの`AUTH`/`HELLO`は接続単位で認証するコマンドであるため、ここで“接続のユーザー”が切り替わる挙動に統一します。citeturn14search2turn14search3turn14search26  
9) **セッション継続中の有効期限 enforcement（推奨）**: 署名再検証は不要でも、`exp`到来時は以降のコマンドを `NOAUTH` 相当で拒否する（長寿命接続での安全性を上げる）。JWTの`exp`は標準クレームです。citeturn7search30turn13search8  

**Claim-validation defaults（既定値）**  
- `iss`: **必須（完全一致）**。OIDC Discovery使用時は「Discovery文書の`issuer`」「トークンの`iss`」「設定issuer」の3者完全一致を必須（偽メタデータ/なりすまし防止）。citeturn26view0turn25view0turn21view0  
- `aud`: **必須（少なくとも1つ一致）**。既定では「許可audience集合」を設定で与え、`aud`がそれを含まない場合は拒否。ID Tokenの`aud`検証は仕様上MUSTです。citeturn21view0turn15search8  
- `exp`: **必須**。現在時刻 < `exp` を必須（許容スキュー数十秒〜数分は設定）。ID Token検証規則でも`exp`検証はMUSTです。citeturn21view0turn7search30  
- `nbf`: **存在すれば必須検証**（現在時刻 ≥ `nbf`）。JWT標準クレームとして定義され、BCP上も時間検証の抜けは典型的失敗モードです。citeturn7search30turn13search8  
- `iat`: 既定は“情報用途+緩い検証”（例: 過去/未来に極端に離れていれば拒否）。OIDC Coreでも`iat`で“発行時刻が離れすぎたトークンを拒否”に触れています。citeturn21view0turn13search8  
- principal claim（ユーザー識別）: 既定 **`sub`**。Kubernetes SAトークンなどでも`sub`の形式が明確で、相互運用上の“最小公倍数”になりやすいです。citeturn19search25turn21view0  
- `typ`: 既定は**未強制（観測/ログのみ）**。RFC 9068のプロファイルに従うOAuth2 JWTアクセストークンでは`typ=at+jwt`（または`application/at+jwt`）がMUSTですが、実運用のトークンはこのプロファイルに準拠しないことも多いため、Phase 2以降に“厳格モード”として提供するのが現実的です。citeturn22view0turn13search8  
- `azp`: 既定は**未強制**。ただし **`aud`が複数要素**の場合、OIDC ID Tokenでは`azp`確認がSHOULDとされるため（混同回避）、`id_token_like_validation=true` のようなモードで強化する余地があります。citeturn21view0turn18search13  

**Algorithm/key-selection rules（RFC 8725観点の既定）**  
- `alg` allow-list（例: `RS256`のみから開始）。“受信トークンが提示した`alg`を無条件に受け入れる”のはBCPが想定する攻撃（アルゴリズム混同等）につながるため、**サーバー設定が正**でトークンはそれに従属する形にします。citeturn13search8turn13search2  
- `none`は常に拒否。JWT BCPは“`none`は他の手段で暗号学的に保護される場合のみ”とし、OAuth2 JWT access token profile（RFC 9068）でも`none`はMUST NOTです。citeturn13search8turn22view0  
- 鍵選択は「issuerでスコープされたJWKS」内のみ。Discovery/metadata経由の場合は`issuer`完全一致を前提に“そのissuerが指す`jwks_uri`”のみを使う（mix-up/なりすまし防止）。citeturn26view0turn25view0  
- `kid`は最優先の探索キーだが、`kid`不在やローテーション時を考え、(a) 少数鍵なら全試行、(b) 多数鍵なら`kid`必須化、のように上限を設ける。Microsoft Entra発行トークンでも`kid`が検証鍵を示すヘッダーとして説明されています。citeturn15search0turn13search1  

**Principal-to-ACL-user mapping rules（推奨モデル）**  
- 既定（Phase 1）: **“一致必須”**  
  - `requested_username`（`AUTH username ...`のusername）と、JWTから抽出した`principal`（既定は`sub`）が**完全一致**しない限り拒否。  
  - 理由: JWTを持つ主体が任意のACLユーザー名を指定して権限昇格するのを防ぐため。JWT BCPが強調する“JWTの混同/誤用”のクラスを、運用設定で最小化するアプローチです。citeturn13search8turn7search30  
- 拡張（Phase 2〜）: **テンプレート/マッピング表**  
  - 例: `principal = sub`（`system:serviceaccount:<ns>:<sa>`）→ `acl_user = k8s/<ns>/<sa>` のような正規化。  
  - さらに、明示的な`principal → acl_user`マッピング表（exact/prefix/regex）を導入し、`AUTH username`が一致しない場合でも“そのusernameへなりすます権限がprincipalに付与されている”時だけ許容。  
- 不一致時の扱い: 既定は **必ず拒否**（ログに「principalとrequested usernameの不一致」を理由として記録、トークン値は記録しない）。citeturn13search8turn19search11  

**Workload-identity provider nuance（最初から押さえるべき差分）**  
- entity["organization","OpenID Foundation","openid standards body"] / OIDC Discovery: issuerの厳格一致とTLS検証が“なりすまし対策”として明記されているため、Discovery採用時は必須チェックにする。citeturn26view0turn25view0  
- Entra（Azure/Entra）: OIDCメタデータ文書から`jwks_uri=https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys`が得られる例が公式に提示されている。citeturn16view0  
- EntraのWorkload Identity Federation/AKS: “AKSクラスタがissuerとなるKubernetes SAトークンを、EntraがOIDCで公開鍵発見して検証してからトークン交換する”流れが公式に説明されており、Kubernetes SAトークン（＝cluster issuer/JWKS）を扱う価値が高い。citeturn15search1turn18search3turn19search11  
- Google Cloud: メタデータサーバーがaudience指定付きのIDトークンを発行し、検証側が後でaudienceを確認する流れが公式に示されているため、`aud`検証が相互運用上の要。citeturn17search0turn17search7  
- Kubernetes SAトークン: `sub`が `system:serviceaccount:NAMESPACE:KSA_NAME` という形式であることが、Google CloudのKubernetes連携ドキュメントに明示されている（実装上のprincipal抽出/テンプレートの根拠）。citeturn19search25  
- AWS（IRSA等）: IAM OIDCプロバイダ作成時にaudience（client IDs）を設定する・`azp`がある場合はそれを`aud`として扱う・EKSのProjectedServiceAccountToken検証にはJWKSを取得する、など “aud/azp/JWKS” 周りの挙動が公式に書かれているため、Phase 3でプロバイダ別プリセット化すると運用が楽になる。citeturn18search13turn18search3turn18search2  

## Configuration / JWKS Strategy

**first-slice recommendation（Phase 1で最も堅い最小実装）**  
- **JWKS source: ローカルファイル**（例: `auth.jwt.jwks_file=/etc/redis-compat/jwks.json`）  
  - 理由: HTTP/Discovery面を追加せず、SSRF・ネットワーク障害・更新の難しさを初期から抱えないため（運用が単純）。citeturn13search8turn8view0  
  - JWKSはRFC 7517で規定されるJWK Set（`{"keys":[...]}`）として読み込む。citeturn13search1  
- **Issuerは設定で固定**（例: `auth.jwt.issuer=https://login.microsoftonline.com/<tenant>/v2.0` など）。JWTの`iss`完全一致を必須にする。citeturn21view0turn7search30  
- **アルゴリズムは`RS256`のみから開始**（後で拡張）  
  - RFC 9068ではJWT access tokenの署名必須・`none`禁止・`RS256`サポート必須等が書かれており、相互運用上もまずRS256が“最小公倍数”になりやすい。citeturn22view0  
- **Operational kill-switch**  
  - `auth.jwt.enabled`（bool）: falseが既定、trueでJWT認証経路を有効化  
  - `auth.jwt.fallback_to_password`（bool）: 既定true（段階導入向け）、のちにfalse推奨（JWT-onlyユーザーを導入する段階で）  
  - Redis互換としては、`AUTH`/`HELLO AUTH`は接続認証の入口であり、複数の認証経路を試行する設計はRedis Modules APIの“auth callbacks are attempted”の記述とも整合します。citeturn14search2turn14search3turn14search26  

**long-term recommendation（目標状態）**  
- **Issuer固定 + OIDC Discovery（`/.well-known/openid-configuration`）から`jwks_uri`を導出**  
  - OIDC Discoveryはissuerからwell-knownで設定を取得する仕組みを規定しており、設定文書の`issuer`がリクエストに使ったIssuer URLと完全一致し、かつID Tokenの`iss`とも一致させることを要求しています。citeturn26view0turn21view0  
  - OAuth AS metadata（RFC 8414）でも同様に、取得したメタデータの`issuer`がリクエストに使ったissuerと同一でない場合は“使ってはいけない”と明記されています。citeturn25view0  
- **Direct JWKS URLは“例外ルート”として残す**  
  - OIDC準拠でないIdP/社内JWKS配布などに必要なことがあるため。ただしRFC 8414は`jwks_uri`自体にhttps必須なども規定しているため、同等の制約（https限定、リダイレクト禁止、ホスト制限）をデフォルトにする。citeturn25view4turn8view0  
- **Hybrid/bootstrap**  
  - “起動直後はローカルJWKSで即応”しつつ、バックグラウンドまたはオンデマンドでDiscovery→JWKS更新へ移行する方式は、GoogleのOIDCガイドがDiscovery文書取得とキャッシュルール適用を推奨している点とも相性が良いです。citeturn14search19turn26view0  

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["OIDC discovery jwks_uri flow diagram","JSON Web Key Set JWKS key rotation diagram","JWT signature verification flow diagram"],"num_per_query":1}

**refresh / failure model（JWKS更新と失敗時挙動の推奨）**  
- Startup behavior  
  - ローカルJWKS: 起動時にロードし、パース失敗ならJWT認証を自動的に無効化（fail-safe）し、運用上はログ/メトリクスで検知。citeturn13search1turn13search8  
  - Discovery/JWKS URL: 起動時に“同期で1回”取得してキャッシュへ。Issuer/metadata整合性チェックを必須（不一致なら使用禁止）。citeturn25view0turn26view0  
- Refresh trigger  
  - **オンデマンド更新（推奨）**: `kid`が見つからない、または署名検証に必要な鍵が無い場合に限り、レート制限付きでJWKSを再取得し、1回だけリトライ。トークンヘッダーの`kid`が検証鍵を示すことはEntraドキュメントでも説明されており、ローテーション時の自然なトリガになります。citeturn15search0turn18search3  
  - **最小周期の定期更新（任意）**: DiscoveryやJWKS取得レスポンスのHTTPキャッシュポリシー（max-age等）を尊重しつつ、最低でも数時間〜1日で更新する設定を提供。GoogleはDiscovery文書取得時に“キャッシュルールを適用する”と述べています。citeturn14search19  
- Failure/staleness behavior  
  - **Last-known-good（LKG）キャッシュ**を保持し、取得失敗時は“直近成功したJWKS”で検証を継続（可用性）。ただしBCP上、間違った鍵受容は重大なので、`max_stale`（例: 24h）を超えたらfail-closedに倒す設定を用意。citeturn13search8turn25view4  
- Provider outage + kid rotation  
  - `kid`未解決 → まずLKGで探索 → 不在ならオンデマンド更新 → なお失敗なら「一時的に拒否（認証失敗）」が基本。EKSのドキュメントも“検証にはJWKS取得が必要”という前提を置いています。citeturn18search3turn13search1  
- SSRF/URL安全性（Discovery/JWKS URLを導入するなら必須）  
  - https限定、リダイレクト禁止（SSRF対策として`openidconnect`でも明示的に注意喚起）。citeturn8view0turn25view4  
  - issuer/metadataの完全一致検証（RFC 8414 / OIDC Discovery）。citeturn25view0turn26view0  
  - DNS/IP制限（リンクローカル/ループバック/プライベートIPのブロック等）は“実装推奨”だが、少なくとも管理者が設定するissuerを固定し、CONFIG権限を最小にする（運用統制）ことが重要。citeturn8view0turn14search13  

## Phased Delivery Plan

**Phase 1: minimal viable server-side token auth（HTTPなし、最小面積）**  
- 機能  
  - `AUTH username <jwt>` / `HELLO ... AUTH username <jwt>` でJWTを受理（特定設定時のみ）。Redisのコマンド構文（`AUTH <username> <password>` / `HELLO ... AUTH <username> <password>`）互換のまま“password文字列の代わりにjwtを入れる”形にする。citeturn14search2turn14search3  
  - `auth.jwt.enabled` のキルスイッチと、`auth.jwt.fallback_to_password` による段階導入（既存静的ACLに影響しない）。citeturn14search26turn14search13  
- セキュリティ既定値  
  - 署名必須、`none`拒否、`iss`/`aud`/`exp`/`nbf`検証（`iat`は緩く）。citeturn13search8turn7search30turn22view0  
  - アルゴリズムは`RS256`のみから開始（allow-list）。citeturn22view0  
- JWKS  
  - ローカルJWKSファイルのみ（起動時ロード＋明示リロード）。JWK Set形式はRFC 7517準拠。citeturn13search1  
- マッピング  
  - principal claimは`sub`固定、`AUTH username` と完全一致のみ許可（最小＆安全）。citeturn21view0turn13search8  
- テスト  
  - JWTの不正系（署名不一致、exp切れ、iss/aud不一致、巨大トークン）をgoldenテスト化。JWTクレーム定義（`exp`/`nbf`等）はRFC 7519に基づく。citeturn7search30turn13search8  

**Phase 2: hardening + rotation（Discovery/JWKS URLを導入、堅牢に）**  
- HTTP面の導入（ただし最小）  
  - `auth.jwt.issuer`（必須） → OIDC Discoveryの `/.well-known/openid-configuration` から `jwks_uri` を取得するモード追加。citeturn26view0turn16view0  
  - issuer整合性（取得URLと文書内issuer一致）を必須化。citeturn26view0turn25view0  
  - リダイレクト禁止（SSRF）。citeturn8view0  
- JWKSキャッシュ/更新  
  - LKGキャッシュ＋オンデマンド更新（`kid`ミス時に限定）。`kid`が検証鍵の識別子として扱われることはEntraでも説明がある。citeturn15search0turn25view4  
- マッピング拡張  
  - principal claimを設定可能に（例: `sub`/`email`/`preferred_username`など）。ただし“存在しない/形式が変わる可能性”は常にあるため、既定は`sub`を維持。citeturn21view0turn19search25  
- 接続中の期限切れ処理  
  - `exp`を超えたら接続の認証状態を失効させる（次コマンドから拒否）。citeturn7search30turn13search8  

**Phase 3: provider-grade workload-identity support（相互運用プリセット＋運用可観測性）**  
- マルチissuer & プロファイル  
  - 例: `auth.jwt.issuers=[{issuer, audiences, algs, principal_claim, jwks_source, mapping_rules}, ...]` のような“issuer単位のポリシー”。OIDC Discovery/AS metadataがissuer単位のメタデータ取得を想定していることに整合。citeturn26view0turn25view0  
- プロバイダ別Nuanceを“設定プリセット”に落とす  
  - Entra: 公式に示されるauthorityとwell-known、`jwks_uri`、`kid`を踏まえた設定テンプレートを用意。citeturn16view0turn15search0  
  - Google Cloud: メタデータサーバーIDトークンはaudience指定が前提であるため、audience運用（設定/テンプレ）を整備。citeturn17search0turn17search7  
  - Kubernetes SAトークン: `sub`形式（`system:serviceaccount:...`）に合わせたテンプレートマッピング（`namespace`/`serviceaccount`抽出）を提供。citeturn19search25turn19search11  
  - AWS（IRSA等）: audienceや`azp`取り扱いの注意、EKSのJWKS取得手順などを踏まえたガイドを付属。citeturn18search13turn18search3turn18search25  
- 監査性/運用性  
  - `CONFIG GET`で「現在のissuerポリシー概要」「JWKS最終成功時刻」「鍵数」「更新失敗回数」などを確認可能にする（ログにJWT全文は残さない）。JWTは“機密クレデンシャル”として扱うべきで、漏洩は重大です。citeturn15search4turn13search8  

## Risk Register

- SSRF（Discovery/JWKS URLの悪用）  
  - リスク: `CONFIG SET`等で不正なURLを設定されると、内部ネットワークへHTTP到達しうる。  
  - 緩和: https限定、リダイレクト禁止（SSRF注意は`openidconnect`公式ドキュメントでも明記）、issuer/metadata完全一致検証（RFC 8414 / OIDC Discovery）、さらに可能ならプライベートIP/リンクローカル遮断。citeturn8view0turn25view0turn26view0  
- Discovery/メタデータ偽装（issuer mix-up / impersonation）  
  - リスク: 攻撃者が“同じissuerを名乗るメタデータ”を返し、自分の`jwks_uri`へ誘導しうる。  
  - 緩和: 取得URLに使ったissuerと、文書内`issuer`の完全一致を必須（不一致なら“使ってはいけない”と明記）。citeturn25view0turn26view0  
- 鍵ローテーション/`kid`更新に伴う認証失敗  
  - リスク: 新`kid`のトークンが来た瞬間にJWKSが古いと失敗。  
  - 緩和: LKGキャッシュ＋`kid`未解決時オンデマンド更新（レート制限）＋定期更新（キャッシュ指示に準拠）。`kid`が検証鍵を示すことはEntraでも説明される。citeturn15search0turn14search19turn25view4  
- Provider障害時の可用性 vs セキュリティのトレードオフ  
  - リスク: JWKS取得不能で新規認証が止まる（fail-closed）か、古い鍵で継続（fail-open寄り）か。  
  - 緩和: `max_stale`設定（期限内はLKGで検証継続、超過したらfail-closed）。BCP上、誤検証は重大なので“デフォルトは短めのstale許容”が安全。citeturn13search8turn25view4  
- Algorithm confusion / `none`受理 / allow-list漏れ  
  - リスク: 不適切な`alg`受理で署名未検証や混同が起こる。  
  - 緩和: `alg` allow-list固定、`none`常時拒否（RFC 9068はaccess tokenで`none`禁止、JWT BCPも注意喚起）。citeturn22view0turn13search8  
- トークン混同（ID TokenをAccess Token/認証トークンとして誤用）  
  - リスク: 本来用途が異なるトークンを受理してしまう。  
  - 緩和: `iss`/`aud`厳格、（任意で）`typ`チェック。RFC 9068では`typ=at+jwt`をMUSTとして混同回避を強調。citeturn22view0turn21view0turn13search8  
- 入力サイズ/形式によるDoS（巨大JWT/JWKS、Base64爆弾、JSON深さ）  
  - リスク: パース/検証コストでスレッド占有。  
  - 緩和: トークン最大長/JWKS最大サイズ/鍵数上限/JSON深さ制限、パース前ガード、`kid`無い場合の総当たり上限。JWTはコンパクト形式（3セグメント）であることが前提。citeturn7search30turn13search8turn13search1  
- ログ/メトリクスによる機密漏洩  
  - リスク: JWT（クレデンシャル）をログに出すと流出が致命的。Microsoftもアクセストークンを“センシティブな資格情報”と表現している。citeturn15search4  
  - 緩和: JWT本文はログ不可（ハッシュ/`kid`/issuer/失敗理由だけ）。監査用途は“最小情報”に限定。citeturn13search8turn15search4  
- 接続が長寿命な場合の期限切れ取り扱い  
  - リスク: `AUTH`時点で有効でも、長時間後に`exp`が過ぎたトークンで認証状態が残り続ける。  
  - 緩和: 接続ごとに`exp`を保持し、コマンド実行時に期限切れなら拒否（署名再検証不要）。`exp`はJWT標準クレーム。citeturn7search30turn13search8  

## References

- Redisコマンド仕様（`AUTH`, `HELLO`）: citeturn14search2turn14search3  
- Redis Modules認証コールバックの説明（AUTH/HELLO時に追加コールバックを試行）: citeturn14search26  
- `jsonwebtoken`（docs.rs, GitHub）: citeturn8view4turn2search0turn6search36turn5search0  
- `openidconnect`（docs.rs, GitHub releases、SSRF注意）: citeturn9view0turn8view0turn6search1  
- `josekit`（docs.rs, GitHub、OpenSSL依存）: citeturn6search6turn6search34turn10search6turn5search2turn2search3  
- `jwt-simple`（GitHub, docs.rs, license）: citeturn6search7turn6search3turn23search7turn7search13  
- `jwt-compact`（docs.rs, 設計方針）: citeturn9view3turn8view3turn12search0turn23search17  
- JWT/JWS/JWK標準: RFC 7519（JWT）citeturn7search30、RFC 7515（JWS）citeturn13search2、RFC 7517（JWK/JWKS）citeturn13search1  
- JWTセキュリティBCP: RFC 8725 citeturn13search8  
- OAuth/OIDCメタデータ: RFC 8414（issuer整合性、jwks_uri要件）citeturn25view0turn25view4、OpenID Connect Discovery 1.0（issuer整合性、TLS要件）citeturn26view0  
- OIDC ID Token検証規則（`iss`/`aud`/`azp`/署名/`exp`等）: OpenID Connect Core 1.0 citeturn21view0  
- OAuth2 JWTアクセストークンプロファイル（`typ=at+jwt`、署名必須、`none`禁止など）: RFC 9068 citeturn22view0  
- Microsoft identity platform OIDCメタデータと`jwks_uri`例: citeturn16view0  
- Microsoft Entraトークン（`alg`/`kid`説明）: citeturn15search0  
- AKS Workload Identity概要（Kubernetes SAトークンをOIDCで検証して交換）: citeturn15search1  
- Google Cloud: メタデータサーバーIDトークン取得/検証（audience指定など）citeturn17search0turn17search7  
- Google Cloud: Kubernetes SAトークンの`sub`形式（WIF with Kubernetes）: citeturn19search25  
- Kubernetes: 認証（JWT bearer tokenとしてのSAトークン）: citeturn19search11  
- AWS: EKS IRSAでのJWKS取得（ProjectedServiceAccountToken検証）: citeturn18search3turn18search31  
- AWS: IAM OIDCプロバイダ作成/`aud`/`azp`の注意: citeturn18search2turn18search13turn18search25