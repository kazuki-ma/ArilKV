# Infinite-Time Refactor Ideas (2026-03-17)

This note captures refactors that are not required for immediate correctness, but would make the codebase materially cleaner or easier to extend.

## Highest-Leverage Structural Refactors

- Split `request_lifecycle/server_commands.rs` by domain instead of continuing to grow one mega-file.
  Candidate seams:
  - `config_commands.rs`
  - `persistence_commands.rs`
  - `admin_introspection_commands.rs`
  - `acl_admin_commands.rs`
  - `replication_admin_commands.rs`

- Replace ad-hoc `CONFIG SET` branching with a declarative config registry.
  Desired properties:
  - name
  - parse/validate function
  - mutability (`mutable`, `immutable`, `protected`, `deny-loading`)
  - apply hook
  - startup-only vs runtime-allowed flag
  This would make future `dir` / `dbfilename` / `aclfile` / protected-config parity much less error-prone.

- Separate startup config application from runtime config mutation.
  Today both surfaces eventually converge on the same underlying config map, but they are conceptually different:
  - startup bootstrap
  - runtime `CONFIG SET`
  - internal test/bootstrap seeding
  Those should be explicit code paths with a shared typed config model underneath them.

- Move persistence path derivation into a dedicated module.
  `dir` + `dbfilename` + `appendfilename` + `aclfile` + OIDC JWKS file resolution is currently spread across unrelated code paths.
  One place should own:
  - normalization
  - relative-path resolution
  - startup/runtime validation
  - path collision rules

- Introduce a typed `ServerBootstrap` object instead of smuggling more behavior through `ServerConfig`.
  Ideal split:
  - listener/network config
  - startup config overrides
  - runtime feature toggles
  - cluster topology bootstrap

## Replication / Persistence Refactors

- Split replication into explicit layers:
  - rewrite/classification
  - offset/backlog capture
  - full-sync session state
  - downstream transport
  Current code still mixes these boundaries more than it should.

- Isolate AOF rewrite / live AOF runtime / startup replay into one persistence subsystem API.
  The behavior is correct enough now, but the mental model still spans several files and different entry points.

- Make snapshot export/import family-specific.
  RDB encode/decode is still too centralized, which will make future format/version changes harder than necessary.

## ACL / Security Surface

- Separate ACL file/bootstrap state from generic config plumbing.
  `aclfile`, `ACL LOAD`, `ACL SAVE`, startup ACL load, and runtime user mutations are intertwined in a way that makes Redis-parity decisions harder to reason about.

- Add an explicit trusted-local / protected-config policy abstraction.
  If we want Redis/Valkey-like protected config semantics, the decision logic should not be open-coded inside individual commands.

## Testing / Tooling

- Add a reusable standalone-server startup harness that accepts a `ServerConfig` and waits for readiness.
  Several tests can then stop re-implementing small boot/shutdown wrappers.

- Convert repeated interop server choreography into a scenario DSL.
  Patterns repeated today:
  - Garnet primary + Garnet replica
  - Garnet + local `redis-server`
  - startup / shutdown / restart / reload
  - temp dir + config seeding + wait-for-ready

## Suspected Performance-Oriented Cleanups

- Reduce boundary churn in hot paths.
  The main suspicion remains that owner-thread handoff, small allocations, and repeated serialization boundaries matter more than individual data-structure operations in common workloads.

- Make metadata side paths more explicit.
  TTL, hash-field TTL, object encoding side-state, and persistence metadata still cross-cut many command paths.

- Add a dedicated replication/full-sync benchmark harness.
  Current hot-path smoke benches are useful, but they do not answer:
  - full-sync cost
  - rewrite cost
  - slow replica impact
  - HFE-heavy workload behavior
