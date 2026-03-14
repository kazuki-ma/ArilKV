# Design Doc: Live AOF Durability Tracking

Date: 2026-03-14
Status: Proposed
Scope: `11.488` prerequisite architecture for real `WAITAOF`
Related:
- `TODO_AND_STATUS.md` `11.488`
- `TODO_AND_STATUS.md` `11.489`
- `docs/performance/design-typed-mutation-oplog-and-durability-ledger-2026-03-12.md`
- `docs/performance/design-live-control-plane-topology-snapshot-2026-03-12.md`

## 1. Executive Summary

`WAIT` is now backed by the live downstream ACK ledger, but `WAITAOF` is still a stub.

That gap is no longer a parser problem. It is an execution-model problem:

- there is no live append-only writer in the server runtime
- there is no authoritative local append frontier
- there is no authoritative local fsync frontier
- there is no replica durable-offset visibility
- there is no way to tie a successful command reply to a durable target

The `tsavorite` crate already has low-level AOF primitives, but they are offline utilities today.
The next step is not "teach `WAITAOF` to guess better". The next step is to make durability a
first-class live runtime with monotonic frontiers.

This document defines that runtime in the current codebase, not in a hypothetical rewrite.
The design deliberately fits the existing frame-based replication path and stages toward a cleaner
shared ledger later.

## 2. Current Baseline

### 2.1 What exists today

In the current repository:

- `request_lifecycle/server_commands.rs` implements `WAIT`
- `request_lifecycle/server_commands.rs` still stubs `WAITAOF`
- `redis_replication.rs` maintains:
  - `master_repl_offset`
  - per-replica `downstream_ack_offsets`
  - `wait_for_replicas(...)`
- `connection_handler.rs` materializes `WAIT` after command execution
- `tsavorite::AofWriter` provides:
  - `append_operation(...)`
  - `flush()`
  - `sync_all()`
  - `current_offset()`
- `aof_replay.rs` uses that writer only in replay/recovery tests

### 2.2 What does not exist

There is still no live durability runtime that can answer:

- which write has been appended locally
- which appended write has been fsynced locally
- which replica has durably persisted which offset
- which durability target belongs to the current command reply

`appendonly` and `appendfsync` are currently config-surface values, not a live durability system.

### 2.3 Why `WAITAOF` is still fake

Today `WAITAOF` can only do two things:

- reject `numlocal > 0` when `appendonly` is disabled
- return `[0, 0]`

That behavior is structurally unavoidable until the runtime can publish:

- `local_append_offset`
- `local_fsync_offset`
- per-replica `durable_offset`

## 3. Problem Statement

The missing architecture is not just "an AOF writer". It is the absence of a live durability ledger.

Three failures follow from that.

### 3.1 No local durability target

After a write succeeds, the runtime cannot say:

- "this reply corresponds to local AOF offset X"

That means `WAITAOF numlocal ...` has nothing authoritative to wait on.

### 3.2 No replica durability target

The current replication ACK only proves "replica applied / observed stream offset".
It does not prove:

- "replica durably persisted that offset locally"

So `WAITAOF ... numreplicas ...` cannot be exact.

### 3.3 No shared ordering between reply, replication, and durability

The runtime currently has three loosely related notions of progress:

- command execution completed
- replication frame published
- AOF utility exists somewhere else

They need one monotonic ordering model, even if the first implementation still uses RESP frames as
the propagation unit.

## 4. Design Goals

The live durability model must:

1. work with the current frame-based replication/runtime architecture
2. provide authoritative monotonic local append and local fsync frontiers
3. let top-level command execution capture a per-command durability target
4. expose per-replica durable frontiers when replicas support them
5. preserve `WAIT` semantics while enabling real `WAITAOF`
6. keep the hot path simple enough that the first implementation is measurable and debuggable

## 5. Non-Goals

This document does not attempt to solve:

- full Redis AOF file-format parity
- `BGREWRITEAOF`
- checkpoint compaction policy
- greenfield typed mutation replication replacing RESP frame propagation

Those are future-compatible concerns, but they are not prerequisites for `11.488`.

## 6. Core Model

### 6.1 Authoritative frontiers

The runtime must track these monotonic frontiers:

```rust
struct DurabilityFrontiers {
    local_append_offset: AofOffset,
    local_fsync_offset: AofOffset,
    replica_durable_offsets: HashMap<u64, AofOffset>,
}
```

Required invariants:

- `local_fsync_offset <= local_append_offset`
- all frontiers are monotonic
- each recorded per-command target is captured from these frontiers after command publication order
  is fixed

### 6.2 Per-command target capture

Every top-level mutating command must be able to capture:

```rust
struct DurabilityWaitTarget {
    replication_target: u64,
    local_append_target: Option<AofOffset>,
    local_fsync_target: Option<AofOffset>,
}
```

Interpretation:

- `replication_target` remains the current `WAIT` target
- `local_append_target` is optional because `appendonly=no` may disable live AOF publication
- `local_fsync_target` is optional because some policies only guarantee append, not synchronous fsync

`WAITAOF` should query these captured targets, not whatever the current frontier happens to be when
the command later runs.

## 7. Proposed Runtime

### 7.1 New runtime object

Add a live runtime owned by the server process:

```rust
struct LiveAofDurabilityRuntime {
    writer_tx: mpsc::Sender<DurabilityRecord>,
    state: Arc<DurabilityLedger>,
}

struct DurabilityLedger {
    local_append_offset: AtomicU64,
    local_fsync_offset: AtomicU64,
    replica_durable_offsets: Mutex<HashMap<u64, u64>>,
    notify: Notify,
}
```

The first implementation does not need to expose every field as an object graph. The design point is:

- one writer task owns file IO
- one shared ledger publishes monotonic frontiers
- readers wait on that ledger

### 7.2 Record unit for the first slice

The first slice should intentionally keep the existing propagation unit:

```rust
struct DurabilityRecord {
    replication_offset: u64,
    frame: Arc<[u8]>,
    require_local_durability: bool,
}
```

This is not the final greenfield ideal, but it is the right first step because:

- replication is already frame-based
- script/function rewrite already produces final published frames
- we need exact runtime behavior before a deeper typed op-log refactor

The typed op-log doc remains the long-term target. This document defines the bridge from today's
runtime into that target.

## 8. Publication Path

### 8.1 Current path

Today the command path roughly does:

1. execute mutation
2. build propagation frame(s)
3. publish frame(s) to downstream replicas
4. maybe wait for ACKs

### 8.2 New path

After `11.488`, the publication path for top-level writes should be:

1. execute mutation
2. build final propagation frame(s)
3. allocate / advance `replication_target`
4. enqueue durability record to live AOF runtime when appendonly is enabled
5. publish frame(s) to downstream replicas
6. store captured `DurabilityWaitTarget` on the connection/request context

The ordering requirement is:

- the captured durability target must correspond to the exact write that was replied to

### 8.3 API boundary

The current `publish_write_frame(...)` style API is too small for this. Replace it with something like:

```rust
struct PublishedWriteTicket {
    replication_target: u64,
    local_append_target: Option<AofOffset>,
    local_fsync_target: Option<AofOffset>,
}

fn publish_committed_write(
    &self,
    frame: Arc<[u8]>,
    append_policy: AppendPolicy,
) -> PublishedWriteTicket
```

This lets:

- `WAIT` keep using `replication_target`
- `WAITAOF` use `local_fsync_target`
- future typed mutation work swap the internal record shape without changing the command boundary

## 9. Local AOF Writer Semantics

### 9.1 Writer task

The live writer should run as a dedicated async task created in `server_runtime.rs`.

Responsibilities:

- receive `DurabilityRecord`
- append the RESP frame to the configured AOF file
- advance `local_append_offset`
- flush / fsync according to policy
- advance `local_fsync_offset`
- notify waiters

### 9.2 Append policies

The first implementation should support the current config surface semantically:

- `appendonly no`
  - do not enqueue AOF records
  - `local_*_target` is `None`
- `appendfsync always`
  - append, flush, fsync per published record batch
- `appendfsync everysec`
  - append immediately
  - fsync on timer
  - `local_append_offset` advances before `local_fsync_offset`
- `appendfsync no`
  - append and flush
  - do not promise periodic fsync beyond process/file semantics

`WAITAOF numlocal ...` must be defined against `local_fsync_offset`, not merely "flushed to userspace".

## 10. Replica Durable-Offset Visibility

### 10.1 Why current ACK is insufficient

Current downstream `REPLCONF ACK <offset>` only proves replica stream application visibility.
It does not prove the replica has durably persisted that offset to its own AOF.

### 10.2 Required extension

For Garnet-to-Garnet replicas, add a separate durable-ACK control frame, for example:

- `REPLCONF AOFACK <offset>`

Meaning:

- replica has advanced `local_fsync_offset >= offset`

Primary tracking must keep this distinct from ordinary ACK:

```rust
struct ReplicaDurabilityState {
    applied_or_acked_offset: u64,
    durable_offset: u64,
}
```

Required invariant:

- `durable_offset <= applied_or_acked_offset`

### 10.3 Compatibility note

Exact `WAITAOF numreplicas` requires replicas that can surface a durable frontier.

For non-Garnet replicas that only support ordinary `REPLCONF ACK`:

- `WAIT` can still be exact
- `WAITAOF numreplicas` cannot honestly claim durable parity

The runtime should keep that distinction explicit instead of faking durable ACK out of ordinary ACK.

## 11. Command Semantics

### 11.1 `WAIT`

`WAIT` remains a wait on `replication_target`.

This task should not regress or redefine `WAIT`.

### 11.2 `WAITAOF numlocal numreplicas timeout`

The runtime semantics after `11.488` / `11.489` should be:

- capture the connection's latest `DurabilityWaitTarget`
- `numlocal` is satisfied when:
  - `numlocal == 0`, or
  - `local_fsync_offset >= local_fsync_target`
- `numreplicas` is satisfied when enough replicas have:
  - `replica_durable_offset >= replication_target`

The returned pair remains:

- `[numlocal_synced, numreplicas_synced]`

## 12. Concurrency And Ordering Invariants

This path is concurrency-sensitive and should be modeled explicitly before the first code slice that
mixes reply emission with live fsync frontiers.

Required invariants:

1. publication order is monotonic
2. `local_append_offset` never regresses
3. `local_fsync_offset` never exceeds the appended frontier
4. captured wait target for reply N never points behind reply N's published write
5. replica durable frontier never exceeds what that replica has actually appended/fsynced

When the first live durability slice lands, add or update a focused TLA+ model for:

- publication
- append
- fsync
- wait target capture
- local/replica durable waits

## 13. Staged Implementation Plan

### Stage 1: introduce live local durability runtime

Code targets:

- `server_runtime.rs`
- new runtime module, e.g. `aof_runtime.rs`
- `tsavorite::AofWriter` integration

Deliverables:

- background writer task
- `local_append_offset`
- `local_fsync_offset`
- appendonly/appendfsync live semantics

Validation:

- Rust tests for monotonic append/fsync frontier behavior
- no `WAITAOF` semantic change yet

### Stage 2: publish write tickets from the command path

Code targets:

- `connection_handler.rs`
- `redis_replication.rs`
- request connection effects / per-connection target storage

Deliverables:

- `publish_committed_write(...) -> PublishedWriteTicket`
- per-connection captured durability targets
- no separate "guess current AOF offset" logic

Validation:

- exact Rust regressions that top-level write captures a nonzero local durability target under appendonly
- `WAIT` still green

### Stage 3: add durable ACK from replicas

Code targets:

- `redis_replication.rs`
- replica upstream loop
- primary control-frame parsing

Deliverables:

- `REPLCONF AOFACK <offset>` or equivalent internal contract
- per-replica durable frontier

Validation:

- primary + replica TCP integration test proving durable frontier trails ACK frontier until fsync

### Stage 4: implement real `WAITAOF`

Code targets:

- `request_lifecycle/server_commands.rs`
- `connection_handler.rs` if post-execution materialization is needed

Deliverables:

- real local/replica durable waits
- correct timeout behavior
- exact result pair

Validation:

- Rust regressions for:
  - appendonly disabled error
  - local-only satisfaction
  - replica durable satisfaction
  - timeout partial results
  - `MULTI/EXEC` interaction
  - scripting interaction

## 14. Test Plan

### 14.1 Unit / integration

Add exact Rust coverage for:

- local append/fsync frontier monotonicity
- appendfsync policy behavior
- captured durability targets for top-level writes
- standalone primary + replica durable-ACK flow
- `WAITAOF` return tuple under success and timeout

### 14.2 Compatibility

After the first real `WAITAOF` slice:

- targeted external validation for the touched `WAITAOF` scenarios first
- then full command-coverage rerun at the milestone boundary

### 14.3 Performance

Any hot-path publication change must be measured with:

- fixed before/after parameters
- throughput + p99
- especially for write-heavy paths where append queueing is added

## 15. Recommended First Commit After This Doc

The first code slice after this design should be deliberately small:

1. add live `AofDurabilityRuntime`
2. wire it into `server_runtime.rs`
3. expose monotonic `local_append_offset` / `local_fsync_offset`
4. do not change `WAITAOF` semantics yet

That yields a concrete local durability ledger with minimal semantic risk and gives the next slice a
real substrate for captured durability tickets.
