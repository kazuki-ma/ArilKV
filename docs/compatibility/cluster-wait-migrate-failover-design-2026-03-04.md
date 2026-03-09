# Cluster/Replication Compatibility Design (CLUSTER / WAIT / WAITAOF / MIGRATE / FAILOVER)

Date: 2026-03-04

## 1. Goal

Close high-cost compatibility gaps in:

- `CLUSTER` control surface (especially `KEYSLOT`, `NODES`, `SLOTS`, `SHARDS`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `READONLY`, `READWRITE`)
- `WAIT`
- `WAITAOF`
- `MIGRATE`
- `FAILOVER`

This document answers two questions first:

1. What is impossible with the current model?
2. What should be built before implementation starts (design/tests/harness)?

## 2. Current Reality (Code Facts)

- `WAIT` is currently a minimal stub and always returns `:0` after numeric validation.
  - `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs`
- `WAITAOF` is currently a minimal stub.
  - Returns `[0,0]` for `numlocal=0`
  - Returns `ERR WAITAOF cannot be used ... appendonly is disabled` for `numlocal>0`
- `MIGRATE` is currently short-circuited in the connection path to `+NOKEY`
  (before request-lifecycle command execution), so practical behavior is not
  the documented disabled-surface error.
- `FAILOVER` returns `ERR This instance has cluster support disabled`.
- `READONLY`/`READWRITE` also return `ERR This instance has cluster support disabled`.
- `CLUSTER` has partial read-surface behavior, but some subcommands are static/stubbed (`INFO/NODES/SLOTS/COUNTKEYSINSLOT/GETKEYSINSLOT/SHARDS`).
- Cluster redirection path (`MOVED`/`ASK` + one-shot `ASKING`) already exists in connection routing.
- Replication layer is explicitly "minimal":
  - broadcasts write frames
  - tracks `master_repl_offset`
  - ignores downstream `REPLCONF ACK` chatter
  - does not track per-replica acknowledged offsets
- Server has AOF replay utilities, but no live append-only write pipeline integrated into request execution.

## 3. Feasibility Matrix

### 3.1 Feasible With Current Data Model (No Core Store Redesign)

- `CLUSTER KEYSLOT`: already real; keep as-is.
- `CLUSTER COUNTKEYSINSLOT/GETKEYSINSLOT`: can be made real from existing key registries (`string_key_registries` / `object_key_registries`) and slot hash.
- `READONLY`/`READWRITE`: per-connection flags can be added in `connection_handler` state without changing store layout.
- `WAIT` (basic correctness level): possible if we add per-replica ACK tracking in replication coordinator.

### 3.2 Feasible But Requires Subsystem Extensions (Major Work)

- `WAIT` full behavior:
  - needs target offset capture per command
  - per-replica ACK state
  - timeout wait primitive and wakeup path
- `MIGRATE` (DB 0 subset):
  - needs network transfer protocol path (`DUMP/PTTL/RESTORE` equivalent)
  - atomic-ish source delete behavior once destination restore succeeds
  - retry/error mapping and option support (`COPY/REPLACE/AUTH/AUTH2/KEYS`)
- `FAILOVER` command path:
  - needs command-level orchestration over existing cluster/failover primitives
  - needs replica identity and liveness/offset visibility in server command layer

### 3.3 Not Achievable As "Real Compatibility" Without New Durability Model

- `WAITAOF numlocal>0` real semantics are not possible with current runtime architecture.
  - reason: no live AOF append+fsync pipeline tied to executed write offsets
- `WAITAOF numreplicas>0` real semantics are also blocked until we can map replica ACKed durable offsets.
- Full Redis-grade `FAILOVER TO <host> <port>` is not realistic until replica identity and replication progress are first-class command-visible state.
- `MIGRATE` parity for `db != 0` conflicts with current single-db mode (`SELECT` supports only DB 0).

## 4. Required Design Changes (High Level)

### 4.1 Replication Ack Ledger (for WAIT)

Add a coordinator-managed replica state table:

- replica connection id / endpoint
- `last_ack_offset`
- `last_ack_unix_millis`
- `link_up`

Add command path plumbing:

- capture `wait_target_offset` at command completion
- `WAIT N timeout` waits for `>=N` replicas with `ack_offset >= wait_target_offset`

### 4.2 Live AOF Durability Pipeline (for WAITAOF)

Add runtime AOF writer integration:

- append every mutating frame in command order
- expose `last_appended_offset` and `last_fsynced_offset`
- support `appendfsync` policies

Then implement:

- `WAITAOF numlocal numreplicas timeout`
  - `numlocal` check against local fsynced offset
  - `numreplicas` check against durable-ack replica offsets

### 4.3 Cluster Command View Provider

Current routing has `ClusterConfigStore` in connection layer, but command handlers are mostly static.

Add command-visible cluster view:

- wire read-only access to `ClusterConfigStore` into command handler path
- produce `CLUSTER INFO/NODES/SLOTS/SHARDS` from live snapshot
- make `COUNTKEYSINSLOT/GETKEYSINSLOT` query real key registries

### 4.4 MIGRATE Execution Path

Phase as DB0-first:

- validate full syntax/options
- for each key:
  - export serialized payload (+ TTL metadata)
  - send restore to target
  - delete source only after success (unless `COPY`)
- support `REPLACE`
- surface Redis-compatible error mapping

## 5. Phased Delivery Plan

1. `P0` Design/Telemetry prep
- finalize this design + interop probe harness
- establish baseline matrix for current gaps

2. `P1` `WAIT` correctness
- implement replication ACK ledger + blocking wait path
- target: Redis external cases for WAIT pass

3. `P2` `CLUSTER` read/control surface
- real `CLUSTER INFO/NODES/SLOTS/SHARDS/COUNTKEYSINSLOT/GETKEYSINSLOT`
- enable `READONLY/READWRITE` behavior in cluster mode

4. `P3` `MIGRATE` DB0 subset
- network move path with `COPY/REPLACE/KEYS`
- explicit `DB!=0` compatibility error policy

5. `P4` `FAILOVER` command integration
- bind command to existing failover controller path
- add target-selection/timeout behavior incrementally

6. `P5` `WAITAOF`
- only after live AOF writer + fsync offset tracking lands

## 6. TLA+ Plan (Concurrency-Sensitive Areas)

Add/extend models before finalizing each concurrency-heavy phase:

- `WaitAckProgress.tla`
  - safety: ack offset monotonicity
  - liveness: `WAIT` returns by quorum or timeout
- `MigrateAtomicMove.tla`
  - safety: no key lost/duplicated under timeout/retry interleavings
- `ClusterReadonlyAskingRouting.tla`
  - safety: `ASKING` one-shot consumption + `READONLY/READWRITE` mode transitions

Code annotation rule:

- add `// TLA+ : <ActionName>` on modeled critical sections
- keep action labels synchronized with `formal/tla/specs/*.tla`

## 7. Test Strategy

### 7.1 Unit Tests

- command-level parser/arity/options for all target commands
- WAIT/WAITAOF timeout/edge validations
- MIGRATE option parser and error mapping

### 7.2 Integration Tests (`garnet-server`)

- local primary/replica WAIT quorum behavior
- cluster multi-port READONLY/READWRITE + ASKING behavior
- MIGRATE success/failure/copy/replace scenarios

### 7.3 Interop/External Tests

- Redis external runtest subset for target areas
- dedicated interop probe script (added in this iteration)
- docker compose baseline environment for Redis reference behavior

## 8. Decision

Proceed with `P0` now (design + probe harness), then implement in order:

- `WAIT` -> `CLUSTER` surface -> `MIGRATE` -> `FAILOVER` -> `WAITAOF`

Rationale:

- `WAITAOF` depends on durability model that does not exist yet
- `WAIT` and `CLUSTER` can start with current model + bounded extensions
- `MIGRATE`/`FAILOVER` are large but parallelizable once state visibility is added
