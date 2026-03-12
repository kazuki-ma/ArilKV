# Design Doc: Owner-Thread Mutable Runtime Objects

Date: 2026-03-12
Status: Proposed
Scope: greenfield runtime-object model for set / zset / stream families in `garnet-rs`
Related:
- `TODO_AND_STATUS.md` `11.497`
- `docs/performance/design-owner-thread-actor-only-2026-02-21.md`
- `docs/performance/design-unified-typed-kv-runtime-2026-03-12.md`
- `docs/performance/design-typed-mutation-oplog-and-durability-ledger-2026-03-12.md`

## 1. Executive Summary

Today most complex object mutations still follow this shape:

1. read encoded object payload
2. deserialize whole payload into a Rust structure
3. mutate it
4. serialize the whole payload again
5. write it back

That is structurally wrong for an owner-thread execution model.

The clearest evidence is sets:

- the code already has `set_object_hot_state`
- dirty hot entries are later materialized back to object payload bytes

That means the system has already discovered the right direction:

- owner-thread-local mutable object state is valuable

Greenfield rule:

- set, zset, and stream values should live as canonical owner-thread mutable runtime objects
- persistence and replication should serialize from those runtime objects at the boundary
- command execution should not round-trip through serialized payloads on every mutation

## 2. Current Baseline

### 2.1 Current object-store model

Current object-store APIs still largely expose:

- `load_set_object(...)`
- `save_set_object(...)`
- `load_zset_object(...)`
- `save_zset_object(...)`
- `load_stream_object(...)`
- `save_stream_object(...)`

and the codecs still deserialize full payloads:

- `deserialize_set_object_payload(...)`
- `deserialize_zset_object_payload(...)`
- `deserialize_stream_object_payload(...)`

So even after owner-thread routing improvements, the value model is still blob-oriented.

### 2.2 Existing proof point: set hot state

`object_store.rs` already contains:

- `set_object_hot_state`
- `with_set_hot_entry(...)`
- `materialize_set_hot_entries_for_db(...)`

This is effectively a partial runtime-object design hidden behind a cache name.

The problem is that the current taxonomy is still split:

- canonical payload bytes live in the object store
- mutable set state may temporarily live in hot state

That is useful as an optimization, but it is not the clean final ownership model.

### 2.3 Current cost profile

For `zset` and `stream` especially, frequent mutations still pay for:

- full payload decode
- temporary allocations
- whole-structure reserialization
- repeated key/value container reconstruction

Under owner-thread execution, that work is avoidable.

## 3. Problem Statement

The current blob-first design creates both performance and design debt.

### 3.1 Performance debt

- repeated full-object serialization churn
- high allocation pressure
- poor cache locality across repeated mutations of the same hot object
- unnecessary copy work on owner-thread hot paths

### 3.2 Design debt

- canonical state and hot state are split for sets
- object semantics are encoded in codec flow rather than runtime structure
- background persistence/materialization is an afterthought instead of a first-class boundary
- `SWAPDB` and runtime ownership have to reason about caches separately from canonical data

## 4. Goals

The new design should:

1. make runtime object state canonical for `set`, `zset`, and `stream`
2. preserve owner-thread-only mutation as the concurrency model
3. make persistence and replication explicit adapter boundaries
4. allow snapshotting/checkpointing without forcing blob round-trips on every command
5. delete the conceptual distinction between "hot cache" and "real object state" for these families

## 5. Non-Goals

This document does not require:

- immediate conversion of every object family
- lock-free cross-thread object mutation
- exposing raw runtime-object pointers to unrelated threads

The first target families are:

- `set`
- `zset`
- `stream`

## 6. Design Principles

### 6.1 Runtime object is canonical

If an object family benefits from in-place mutable state, that state must be the authoritative in-memory representation.

It must not be treated as a cache layered on top of canonical payload bytes.

### 6.2 Serialization is a boundary adapter

Persistence and replication are allowed to require bytes.

Command mutation paths are not.

### 6.3 Owner thread owns mutation

Only the owner thread mutates runtime objects.

Cross-thread access is:

- routed to the owner thread, or
- snapshot/freeze based

### 6.4 Stable object identity

Entries should point at stable runtime object identities, not inline serialized blobs.

That allows:

- repeated mutation without full reallocation
- dirty tracking
- background flush/snapshot scheduling

## 7. Proposed Object Model

### 7.1 Entry-to-object indirection

Greenfield target:

```rust
#[repr(transparent)]
struct RuntimeObjectId(u64);

enum EntryValue {
    String(StringValue),
    RuntimeObject(RuntimeObjectRef),
}

struct RuntimeObjectRef {
    id: RuntimeObjectId,
    kind: RuntimeObjectKind,
}

enum RuntimeObjectKind {
    Set,
    Zset,
    Stream,
}
```

The entry tells us:

- this key owns a runtime object
- which object family it is

The actual mutable state lives in an owner-thread object arena.

### 7.2 Per-owner object arena

```rust
struct DbShardRuntime {
    entries: TypedKvStore<OwnedKey, EntryRecord>,
    runtime_objects: RuntimeObjectArena,
}

struct RuntimeObjectArena {
    set_objects: HashMap<RuntimeObjectId, SetRuntimeObject>,
    zset_objects: HashMap<RuntimeObjectId, ZsetRuntimeObject>,
    stream_objects: HashMap<RuntimeObjectId, StreamRuntimeObject>,
}
```

Interpretation:

- object mutation and allocation are shard/owner local
- the arena naturally follows the same owner-thread execution model as the entry store

### 7.3 Object header

Each runtime object should carry:

```rust
struct RuntimeObjectHeader {
    owner: ShardIndex,
    db_runtime: DbRuntimeId,
    dirty_generation: u64,
    persisted_generation: u64,
    created_at: u64,
    last_mutated_at: u64,
}
```

This is enough to support:

- dirty queues
- snapshot progress
- flush scheduling
- debugging and metrics

## 8. Family-Specific Shapes

### 8.1 Set

Set should move first because the code already has a partial runtime-object precursor.

Target:

```rust
struct SetRuntimeObject {
    header: RuntimeObjectHeader,
    payload: SetState,
}

enum SetState {
    Members(BTreeSet<OwnedKey>),
    ContiguousI64Range(ContiguousI64RangeSet),
}
```

Current `set_object_hot_state` should disappear and be replaced by this canonical object.

### 8.2 Zset

Target:

```rust
struct ZsetRuntimeObject {
    header: RuntimeObjectHeader,
    by_member: BTreeMap<OwnedKey, f64>,
    by_score: BTreeMap<OrderedF64, BTreeSet<OwnedKey>>,
}
```

This avoids rebuilding the whole zset map for:

- `ZADD`
- `ZREM`
- `ZINCRBY`
- pop/rank/range operations

### 8.3 Stream

Stream has the highest semantic complexity and also one of the largest potential wins.

Target:

```rust
struct StreamRuntimeObject {
    header: RuntimeObjectHeader,
    entries: BTreeMap<StreamId, StreamEntry>,
    groups: BTreeMap<OwnedKey, StreamGroupState>,
    node_sizes: VecDeque<usize>,
    idmp: StreamIdmpState,
}
```

This lets:

- `XADD`
- `XTRIM`
- consumer-group updates
- pending-entry changes
- `XAUTOCLAIM` / `XCLAIM`

mutate one runtime object directly instead of repeatedly decoding the full stream payload.

## 9. Persistence Boundary

### 9.1 Snapshotting/checkpoint

Checkpoint logic should serialize runtime objects from the owner thread or from a frozen object snapshot.

Two acceptable models:

- owner-thread serialization callback
- frozen immutable snapshot object handed to the persistence worker

The key design rule is:

- command mutation does not need full serialization just because persistence exists

### 9.2 AOF / replication

With a typed mutation op-log, replication and AOF can consume:

- operation records such as `SetAdd`, `SetRemove`
- `ZsetAdd`, `ZsetRemove`, `StreamAppend`, `StreamAck`, `StreamTrim`

Fallback:

- object snapshot records remain allowed for recovery/bootstrap paths

But the steady-state goal is:

- operation log first
- whole-object blob only when necessary

## 10. Background Hooks

### 10.1 Dirty object queues

Each owner shard should keep a dirty queue or generation index of runtime objects.

Uses:

- background persistence flush
- memory pressure compaction
- checkpoint enumeration

### 10.2 Materialization is no longer special-cased cache flush

Today set hot-state has an explicit materialization step.

Under the new design:

- object serialization is a normal background/persistence hook
- not an exceptional "flush the cache back into truth" event

### 10.3 Eviction / cold-object policy

If the project later wants a cold-object policy, it can be explicit:

- serialize runtime object
- retire runtime object arena slot
- keep entry pointing to cold persisted form until next touch

That is a legitimate later optimization, but only after canonical runtime ownership exists.

## 11. Interaction With MultiDB And `SWAPDB`

Runtime objects belong to `DbRuntime`.

Therefore:

- `SWAPDB` moves them automatically by moving the owning runtime binding
- there is no separate hot-object cache to remap

This is one of the main design wins over the current `set_object_hot_state` arrangement.

## 12. Family Priority

### 12.1 Set first

Reasons:

- existing precursor already exists
- smallest semantic jump
- immediate cleanup of hot-state split

### 12.2 Zset second

Reasons:

- large win for mutation-heavy score updates
- simpler than stream
- clear serializer/deserializer costs today

### 12.3 Stream third

Reasons:

- highest semantic complexity
- strongest need for owner-thread mutable state
- should be built after the persistence/dirty-object boundary is proven on set/zset

## 13. Migration Plan

### Stage 1: turn set hot-state into canonical set runtime object

- replace `set_object_hot_state` terminology and semantics
- entry points at runtime object id
- serialization becomes boundary-only

### Stage 2: add dirty-object infrastructure

- generation counters
- per-shard dirty queues
- serialization hooks

### Stage 3: convert zset to canonical runtime object

- remove full deserialize/reserialize mutation path
- use typed mutation events where possible

### Stage 4: convert stream to canonical runtime object

- keep group/pending semantics inside one owner-thread object
- revalidate blocking and claim paths against existing TLA+ assumptions

### Stage 5: unify with typed KV runtime

- integrate runtime objects into the broader typed `EntryValue` model

## 14. Expected Benefits

### 14.1 CPU and allocation reduction

- no full payload decode/encode for every hot mutation
- fewer temporary containers
- fewer repeated copies of members and field/value pairs

### 14.2 Better cache locality

- hot objects stay in owner-thread-local mutable form
- repeated updates hit stable in-memory structures

### 14.3 Cleaner code

- delete the conceptual split between canonical payload and hot cache
- persistence/replication boundaries become explicit adapters
- `set_object_hot_state` stops being a special exception

## 15. Decision

The project should move `set`, `zset`, and `stream` to canonical owner-thread mutable runtime objects.

Serialized object payloads remain important at the persistence/replication boundary, but they should no longer be the primary in-memory mutation representation.
