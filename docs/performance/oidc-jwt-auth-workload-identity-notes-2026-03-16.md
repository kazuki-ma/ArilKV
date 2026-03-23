# OIDC JWT `AUTH` / Workload Identity DeepResearch Notes (2026-03-16)

Source report:

- `docs/performance/oidc-jwt-auth-workload-identity-deepresearch-2026-03-16.md`

## What The Report Validates

- `jsonwebtoken` remains the best default crate choice for inbound JWT verification in this server shape.
- A local/file-backed JWKS bootstrap is the right first production slice.
- Security defaults should stay strict:
  - algorithm allow-list only
  - `none` rejected
  - `iss` / `aud` / `exp` / `nbf` enforced
- Remote discovery/JWKS refresh should be treated as a separate hardening phase, not bundled into the first slice.

## Follow-Up Work The Report Suggests

- Add remote issuer/discovery mode with strict issuer equality checks and SSRF hardening.
- Add direct JWKS URL mode only as an explicit fallback path, not the default.
- Add last-known-good JWKS cache plus:
  - on-demand refresh on unknown `kid`
  - optional periodic refresh using HTTP cache hints
- Add per-connection token expiry enforcement after successful `AUTH`.
- Add size/resource guards for JWT and JWKS inputs.
- Add provider-oriented guidance/presets for:
  - Microsoft Entra
  - Google Cloud / GKE / metadata-server ID tokens
  - AWS IRSA / EKS
  - Kubernetes projected service-account tokens

## Deliberate Divergence From The Report

- The report recommends a default mapping where requested `AUTH username` must equal the JWT principal.
- The current server intentionally uses the explicit sentinel path `AUTH __OIDC__ <jwt>` / `HELLO ... AUTH __OIDC__ <jwt>`.
- We should keep that sentinel design unless we later decide the operational benefit of implicit username-based token mode outweighs the ambiguity and misuse risk.

Reason:

- it keeps password auth and token auth unambiguous
- it avoids hostile or accidental interpretation of ordinary ACL usernames as a token-mode selector
- it lets the validated JWT claim choose the real ACL principal cleanly

## Recommended Next OIDC Backlog

1. Discovery/JWKS URL hardening design and implementation.
2. JWKS cache/refresh model (`kid` miss refresh + LKG cache).
3. Connection-lifetime `exp` enforcement.
4. Input-size / telemetry hardening.
5. Provider-specific operator docs and config examples.
