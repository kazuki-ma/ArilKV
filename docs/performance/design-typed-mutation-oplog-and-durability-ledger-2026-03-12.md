# Design Doc: Typed Mutation Op-Log And Durability Ledger

Date: 2026-03-12
Status: Proposed
Scope: greenfield replication/AOF/durability model for `WAIT` and `WAITAOF`
Related:
- `TODO_AND_STATUS.md` `11.491`
- `docs/compatibility/cluster-wait-migrate-failover-design-2026-03-04.md`
- `docs/performance/design-unified-typed-kv-runtime-2026-03-12.md`

## 1. Executive Summary

The current runtime already has the beginning of a `WAIT` ledger:

- downstream replicas are assigned ids
- ACK offsets are tracked in `redis_replication.rs`
- `connection_handler.rs` special-cases `WAIT` and waits on that ledger

At the same time:

- `request_lifecycle/server_commands.rs` still contains a stub `WAIT`
- `WAITAOF` is still a stub because no live local durability pipeline exists
- replication is driven by broadcast command frames and command rewrite, not by typed mutation events

If `garnet-rs` were designed from scratch, replication and durability would not be modeled as:

- command rewrite first
- ACK bookkeeping later
- AOF as a separate utility

The target model is:

1. owner-thread mutation produces a typed operation record
2. that record is appended to one ordered op-log
3. one shared ledger advances:
   - `applied`
   - `replicated`
   - `replica_acked`
   - `locally_fsynced`
   - `replica_durable`
4. `WAIT` and `WAITAOF` become pure queries over that ledger

## 2. Current Baseline

### 2.1 Replication today

Current facts:

- mutating frames are broadcast downstream
- `master_repl_offset` is advanced by frame byte length
- downstream `REPLCONF ACK` is tracked per replica id
- `wait_for_replicas(...)` exists in `redis_replication.rs`
- `connection_handler.rs` intercepts `WAIT` and uses that ledger directly

This is enough for a bounded `WAIT` implementation, but it is not a clean architecture because:

- command execution and durability state are split across layers
- the unit of replication is still "serialized command frame"
- script/function rewrite logic is mixed into propagation concerns

### 2.2 AOF today

Current facts:

- `tsavorite::AofWriter` exists as a low-level append/flush/sync primitive
- `aof_replay.rs` exists for replay and crash-recovery tests
- runtime config exposes `appendonly` / `appendfsync` surface values
- there is no live append-only pipeline tied to executed write offsets

This is why `WAITAOF` is not real today:

- no authoritative "this write reached local append offset X"
- no authoritative "offset X has been fsynced locally"
- no authoritative "replica Y has durably persisted offset X"

## 3. Problem Statement

The current model treats three related concerns as separate afterthoughts:

1. mutation semantics
2. replication propagation
3. durability accounting

That creates three classes of problems.

### 3.1 Semantic duplication

- command handlers decide how a write behaves
- replication code decides how to serialize/rewrite that write
- future durability code would need a third view of the same write

### 3.2 Incomplete observability

The runtime can answer:

- "what is current master replication offset?"
- "which replica last ACKed what byte offset?"

But it cannot answer uniformly:

- "which logical mutation does this offset correspond to?"
- "was that mutation only applied, or also appended, or also fsynced?"
- "did a replica apply it, ACK it, or durably persist it?"

### 3.3 Architectural leakage

`WAIT` currently lives in `connection_handler`, not in one command-semantic layer, because the underlying state is not represented as part of the command execution result.

That is a code smell:

- the ledger exists
- the command layer does not own it
- the transport layer compensates by special-casing command semantics

## 4. Design Goals

The target op-log model should:

1. give every mutating operation a stable logical identity
2. order all propagated mutations in one monotonic stream
3. let local apply, local append, local fsync, replica apply, replica ack, and replica durable states be observed against the same identity
4. make `WAIT` and `WAITAOF` simple ledger waits, not command-specific side channels
5. reduce command-rewrite dependence by propagating typed mutations

## 5. Proposed Model

### 5.1 One logical mutation record

Each successful mutating command produces one or more `MutationRecord`s:

```rust
struct MutationRecord {
    op_id: OpId,
    db: DbName,
    kind: MutationKind,
    effects: Vec<KeyEffect>,
    replication: ReplicationForm,
    durability: DurabilityClass,
}
```

Interpretation:

- `op_id` is the authoritative ordered identity
- `db` is explicit
- `kind` says what logical mutation happened
- `effects` summarize the touched keys/TTL/type changes
- `replication` tells downstream serialization/adaptation how to emit it
- `durability` tells the ledger whether it must participate in local/remote durability accounting

### 5.2 Mutation kinds

The exact enum can evolve, but the ledger must be built around logical records such as:

- `StringSet`
- `StringDelete`
- `HashFieldUpdate`
- `SetMutation`
- `ZsetMutation`
- `StreamAppend`
- `ExpireSet`
- `ExpireDelete`
- `MultiExecBatch`
- `FunctionMutation`
- `ScriptMutation`
- `SelectDbForReplication`

The critical point is:

- downstream propagation should adapt from typed mutation kinds
- not from arbitrary post-hoc command rewrite paths

### 5.3 Ordered stages in the ledger

For each `op_id`, the runtime tracks progress through a shared ledger:

```rust
struct OpLedgerState {
    applied_local: bool,
    appended_local: Option<AofOffset>,
    fsynced_local: Option<AofOffset>,
    replica_applied_quorum: u64,
    replica_acked_quorum: u64,
    replica_durable_quorum: u64,
}
```

Not all fields must be materialized exactly like this. The design point is:

- all waitable states are projections of one ordered mutation stream

## 6. Offset Semantics

The ledger needs distinct monotonic frontiers.

### 6.1 `applied_op_id`

- advanced when the owner-thread mutation is committed to the in-memory runtime
- this is the earliest point at which a command reply may be emitted

### 6.2 `replication_offset`

- advanced when the mutation has been serialized into the downstream replication stream
- replaces the current "frame byte length only" notion with a typed-to-wire mapping

### 6.3 `local_append_offset`

- advanced when the mutation is appended to the local AOF stream

### 6.4 `local_fsync_offset`

- advanced when the local AOF writer confirms `sync_all` or policy-equivalent durability

### 6.5 `replica_ack_offset`

- advanced per replica when a downstream node reports it has processed/ACKed the mutation stream up to that offset

### 6.6 `replica_durable_offset`

- advanced per replica only when the replica can honestly claim local durability for that offset

This last state is the missing piece for real `WAITAOF numreplicas`.

## 7. How `WAIT` And `WAITAOF` Fall Out Of The Model

### 7.1 `WAIT numreplicas timeout`

At command completion:

- capture the current mutation's replication frontier target

Then:

- wait until at least `numreplicas` replicas have `replica_ack_offset >= target`

No command-specific transport special case is needed.

### 7.2 `WAITAOF numlocal numreplicas timeout`

At command completion:

- capture the local append/fsync target for the mutation
- capture the replication target for the mutation

Then:

- `numlocal` is satisfied when `local_fsync_offset >= local_target`
- `numreplicas` is satisfied when enough replicas have `replica_durable_offset >= replica_target`

Again, no stub logic or config-surface guesswork is needed once the ledger exists.

## 8. Replication Form Boundary

The typed mutation log does not require downstream peers to consume an internal binary protocol immediately.

The boundary can be staged:

1. command execution emits `MutationRecord`
2. a propagation adapter turns it into:
   - Redis-compatible RESP frames today
   - another wire/storage form later if desired

This preserves compatibility while cleaning the internal model.

The key is that rewrite becomes:

- an adapter from typed mutation to wire form

instead of:

- the primary source of truth for what happened

## 9. Local Durability Pipeline

The local writer side should be modeled as:

1. receive ordered `MutationRecord`
2. append serialized durability form to AOF
3. mark `appended_local`
4. flush/sync according to policy
5. mark `fsynced_local`

Supported policy examples:

- `appendfsync always`
- `appendfsync everysec`
- `appendfsync no`

The ledger must expose which frontier corresponds to each policy so `WAITAOF` remains exact.

## 10. Replica Durable ACK

A real `WAITAOF numreplicas` requires a stronger downstream contract than current `REPLCONF ACK`.

Greenfield design:

- keep one ACK channel for "applied/processed"
- add one durable-ACK channel for "fsynced/persisted"

That could be:

- protocol extension in Garnet-to-Garnet mode
- downgraded capability when the downstream peer is plain Redis/Valkey and cannot provide durable ACKs

This is important because:

- `WAIT` and `WAITAOF` are not the same query
- one asks "did replicas process this?"
- the other asks "did replicas durably persist this?"

## 11. Migration From Current Code

### Stage 1: make the existing ACK ledger first-class

- move `WAIT` semantics out of connection-only special handling into a command-visible replication ledger API
- make command completion return/capture the relevant target offset

### Stage 2: introduce `MutationRecord`

- keep emitting RESP frames downstream
- derive those frames from typed mutation records instead of ad-hoc rewrite sites

### Stage 3: add live local append pipeline

- wire `MutationRecord` into an append-only writer task
- start tracking `local_append_offset` and `local_fsync_offset`

### Stage 4: split replica processed ACK vs durable ACK

- extend downstream state to support both concepts explicitly

### Stage 5: make `WAIT` / `WAITAOF` command-semantic ledger queries

- at that point, transport no longer owns correctness

## 12. Expected Wins

### 12.1 Correctness

- one source of truth for write progress
- `WAIT` and `WAITAOF` become exact projections of the same ordered stream
- script/function propagation is grounded in typed effects, not in transport-only rewrite

### 12.2 Performance

- less duplicate serialization logic
- cleaner batching opportunities at the op-log boundary
- clearer separation between mutation cost and wire/persistence cost

### 12.3 Code cleanliness

- command semantics no longer leak into `connection_handler`
- replication, AOF, and durability consume the same record type
- the system becomes easier to reason about because "what happened?" and "how far has it progressed?" share the same identity

## 13. Risks

1. introducing `MutationRecord` too early without a stable internal value model can create churn
2. ledger bookkeeping must stay monotonic and lightweight on the hot path
3. multi-op commands and script/function writes need careful batching semantics so one logical user command can still expose the right wait target
4. mixed peer capability (`Redis` replica vs `Garnet` replica) must be modeled explicitly for durable ACK support

## 14. Decision

The right greenfield target is:

- typed mutation production at owner-thread commit time
- one ordered op-log
- one durability/replication ledger with distinct monotonic frontiers

Future replication/AOF work should be evaluated against that target instead of extending the current split model of command rewrite + side-channel ACK bookkeeping.
