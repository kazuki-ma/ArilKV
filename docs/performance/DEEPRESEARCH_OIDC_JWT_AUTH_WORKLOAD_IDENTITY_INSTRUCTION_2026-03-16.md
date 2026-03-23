# DeepResearch Instruction (No Repo Access): OIDC JWT `AUTH` / Workload Identity for a Redis-Compatible Server

## Important Constraints

- Assume you **cannot access this repository**.
- Build the answer from public primary/official sources only.
- Prioritize production security, operability, and maintenance posture over shortest implementation path.

## Project Context (Provided Manually)

We are building a Redis-compatible server in Rust.

Current authentication state:

- `AUTH` and `HELLO ... AUTH` currently support static ACL username/password credentials.
- ACL users, selectors, key/channel/database permissions, ACL file/runtime reload behavior, and command authorization are already first-class.
- We now want to add OIDC / workload-identity JWT authentication as an **additional** `AUTH` credential source.

Target shape:

- `AUTH username <jwt>` and `HELLO ... AUTH username <jwt>` should accept a bearer JWT instead of a static password when OIDC/JWKS validation is configured.
- The validated JWT principal should map to a local ACL user and refresh the connection's authenticated user state.
- We prefer an incremental rollout with an explicit kill-switch and clear fallback to existing static ACL auth.

Architecture constraints:

- This is a server-side inbound token verification feature, not a browser/client login flow.
- The server has an owner-thread/request-processor execution model; config is exposed through Redis-style `CONFIG SET/GET`.
- We want to keep the first implementation operationally simple if possible.
- A local/file-backed JWKS bootstrap is acceptable for the first slice if that is the best engineering tradeoff, but we want the report to recommend the best end-state for remote discovery / JWKS refresh as well.

Non-functional requirements:

- Correctness and security matter more than convenience.
- Avoid unnecessary surface area if the server only needs JWT/JWKS verification.
- Minimize long-term ops burden and brittle background-refresh behavior.
- The final design must be testable with exact compatibility-style regressions.

## Research Goals

1. Select the best Rust library strategy for server-side JWT/JWKS verification in this use case.
2. Define the recommended validation/security model for bearer-token `AUTH`.
3. Produce a phased plan from initial viable rollout to production-grade workload-identity support.

## Questions To Answer

1. Which Rust libraries/approaches are the most credible **today** for this exact use case?
   - Evaluate at least: `jsonwebtoken`, `openidconnect`, `josekit`, `jwt-simple`, and any other credible current option you believe beats them.
2. Which option is the best fit for a Redis-compatible server that only needs inbound JWT verification?
   - We do **not** need browser redirects, auth-code flows, PKCE, client registration, or general OIDC relying-party UX.
3. What should the trust/configuration model be?
   - Local JWKS file
   - Direct JWKS URL
   - OIDC discovery document + derived JWKS URL
   - Hybrid/bootstrap approach
4. What is the recommended JWKS refresh and key-rotation strategy?
   - Startup behavior
   - Background refresh or on-demand refresh
   - Failure/staleness behavior
   - Handling `kid` rotation and temporary provider outages
5. What claim-validation rules should be enforced by default?
   - `iss`, `aud`, `exp`, `nbf`
   - configurable username/principal claim
   - `sub` vs provider-specific claims
   - `alg` allow-listing and other RFC 8725 best practices
   - whether `typ`, `azp`, or other OIDC-specific fields should matter here
6. What ACL mapping model is recommended?
   - exact principal-to-ACL-user equality
   - configurable claim-to-username mapping
   - explicit mapping table
   - behavior when JWT principal and requested `AUTH username` disagree
7. What workload-identity provider nuances matter for first-class interoperability?
   - Azure AD / Microsoft Entra workload identity
   - GCP workload identity federation / service account identity tokens
   - AWS IAM / OIDC federated identities where relevant
   - Kubernetes projected service-account tokens / SPIFFE-adjacent patterns if applicable
8. What are the main failure modes and mitigations?
   - SSRF and untrusted discovery/JWKS URLs
   - stale keys
   - algorithm confusion / header trust mistakes
   - oversized or malformed tokens/JWKS payloads
   - provider outage during auth attempts

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary` (10 bullets max)
2. `Candidate Matrix` table:
   - Candidate
   - Maintenance status
   - License
   - Scope fit for server-side JWT verification
   - Dependency / ops burden
   - JWKS / discovery support
   - Security posture / notable caveats
   - Adopt now / pilot / avoid
3. `Recommended Default`
   - One primary choice
   - One fallback choice
   - Why
4. `Validation Blueprint`
   - JWT verification flow
   - Claim-validation defaults
   - Algorithm/key-selection rules
   - Principal-to-ACL-user mapping rules
5. `Configuration / JWKS Strategy`
   - first-slice recommendation
   - long-term recommendation
   - refresh / failure model
6. `Phased Delivery Plan`
   - Phase 1: minimal viable server-side token auth
   - Phase 2: hardening + rotation
   - Phase 3: provider-grade workload-identity support
7. `Risk Register`
   - top risks and explicit mitigation for each
8. `References`
   - primary sources only (official crate docs/repos, OIDC/OAuth/JWT RFCs/specs, official cloud workload-identity docs)

## Decision Criteria We Will Apply

- Best-fit for inbound server-side JWT verification beats generality.
- Security defaults must be defensible under RFC 8725-style guidance.
- Extra HTTP/discovery surface must be justified, not assumed.
- Any dependency choice must be actively maintained and operationally reasonable.
- We need a clean incremental rollout with a rollback / disable path.
