# Design Doc: First-Class DbRuntime

Date: 2026-03-12
Status: Proposed
Scope: greenfield logical DB ownership model for `garnet-rs`
Related:
- `TODO_AND_STATUS.md` `11.494`
- `docs/compatibility/multidb-db-model-and-command-boundary-redesign-2026-03-11.md`
- `docs/performance/design-command-execution-scopes-2026-03-12.md`
- `docs/performance/design-unified-typed-kv-runtime-2026-03-12.md`

## 1. Executive Summary

The current MultiDB model is still structurally split:

- DB0 uses `MainDbRuntime`
- `db > 0` uses `AuxiliaryDbState`
- `DbCatalog` holds side-state separately
- `SWAPDB` swaps logical bindings plus a growing list of side buckets

That is an improvement over snapshot/flush/reimport, but it is still not the correct end state.

Greenfield rule:

- every logical DB is backed by the same `DbRuntime` type
- DB0 is not structurally special
- `SWAPDB` swaps logical DB bindings to runtime objects, not entries

The core redesign is:

1. separate logical DB names from physical DB runtimes
2. make `DbRuntime` the owner of all data-bearing and data-derived state that must move with the data
3. keep only truly logical-name-scoped coordination outside the runtime

## 2. Current Baseline

Today `request_lifecycle.rs` still has two different storage shapes:

```rust
struct AuxiliaryDbState {
    entries: HashMap<RedisKey, AuxiliaryDbValue>,
}

struct MainDbRuntime {
    string_stores: ...,
    object_stores: ...,
    string_expirations: ...,
    hash_field_expirations: ...,
    string_key_registries: ...,
    object_key_registries: ...,
}

struct DbCatalog {
    main_db_runtime: MainDbRuntime,
    auxiliary_databases: AuxiliaryDbStorageMap,
    logical_bindings: RwLock<DbCatalogBindings>,
    side_state: DbCatalogSideState,
}
```

This means:

- DB0 and DBn do not share one structural contract
- DB-owned state is still spread across several top-level catalog fields
- `SWAPDB` needs a long list of "also swap this side map" maintenance

## 3. Problem Statement

The current architecture still has two different problems.

### 3.1 DB0 remains special

Even with logical bindings, DB0 still has:

- different physical storage
- different metadata/index ownership
- different background/runtime behavior

That blocks a clean mental model:

- "a DB" is not one object

### 3.2 `SWAPDB` is still maintenance-heavy

Current `swap_logical_databases` no longer migrates entries, but it still manually swaps:

- hot-set state
- forced encodings
- debug state
- LRU/LFU metadata
- logical bindings

That approach does not scale.

The correct target is:

- data-bearing state moves because the `DbRuntime` object moved
- only logical-name-scoped ledgers need explicit fixup

## 4. Design Principles

### 4.1 One DB, one runtime object

Every logical DB must be backed by the same runtime shape.

There is no `main` versus `auxiliary` storage class in the final design.

### 4.2 Stable runtime identity, swappable logical binding

`DbName` is a logical name.

`DbRuntimeId` is a stable physical owner identity.

`SWAPDB` swaps `DbName -> DbRuntimeId` bindings.

### 4.3 Data-owned state lives with the runtime

Anything that must follow the data across `SWAPDB` belongs in `DbRuntime`.

Examples:

- key/value data
- expirations
- per-key metadata that survives swap
- key registries and expiry indexes
- hot mutable object state
- data-adjacent stats/cursors

### 4.4 Logical-name state stays outside the runtime

Anything whose meaning is defined by the logical DB name rather than by data ownership stays outside `DbRuntime`.

Examples:

- client-selected DB bindings
- `WATCH` invalidation rules tied to logical DB names
- blocked-client wake fixups that are defined in terms of logical DB names
- topology/control-plane projections that render by logical DB

## 5. Proposed Object Model

### 5.1 Catalog shape

Greenfield target:

```rust
#[repr(transparent)]
struct DbRuntimeId(u32);

struct DbCatalog {
    bindings: RwLock<Vec<DbRuntimeId>>,
    runtimes: RuntimeRegistry<DbRuntimeId, Arc<DbRuntime>>,
    logical_state: Vec<LogicalDbState>,
}
```

Interpretation:

- `bindings[db]` answers which runtime currently backs that logical DB
- `runtimes[id]` owns the actual data/runtime object
- `logical_state[db]` owns only state that is defined by DB name

### 5.2 Runtime shape

```rust
struct DbRuntime {
    data: DbDataPlane,
    indexes: DbDerivedIndexes,
    coordination: DbRuntimeCoordination,
    background: DbBackgroundState,
    identity: DbRuntimeIdentity,
}
```

The exact fields can evolve, but the ownership rule is fixed:

- if the state must move with the database contents, it belongs here

### 5.3 Logical state shape

```rust
struct LogicalDbState {
    name: DbName,
    sessions: DbSessionBindingState,
    watched: WatchedKeyLedger,
    blocked: BlockingKeyLedger,
    tracking: TrackingDbLedger,
}
```

This is intentionally lightweight.

It exists so `SWAPDB` can keep logical-name semantics correct without making data movement linear in entry count.

## 6. What Belongs In `DbRuntime`

### 6.1 Data plane

The data plane includes:

- string/object stores in the current staged architecture
- or one typed entry store in the later unified runtime

Greenfield assumption:

- the outer ownership object is the same either way

### 6.2 Derived indexes

Derived indexes belong in the runtime because they are derived from that runtime's data:

- key registries
- expiry indexes
- hash-field expiry secondary indexes
- key counts / per-shard derived counters

If the data moves, these move with it.

### 6.3 Runtime-owned coordination

Some coordination is still data-owned rather than logical-name-owned:

- owner-thread-local mutable object state
- materialization queues
- runtime-local caches
- expiration cursors

These belong in `DbRuntime`, not in top-level catalog side maps.

### 6.4 Background state

Background workers need per-runtime state such as:

- active-expire cursors
- pending materialization work
- runtime-local sweep epochs
- runtime statistics snapshots

These also move with the runtime.

## 7. What Stays Outside `DbRuntime`

### 7.1 Session-selected DB identity

A client selecting DB `3` means:

- the client is attached to logical DB name `3`

It does not mean the client should follow a particular physical runtime object forever.

Therefore selected DB state is logical-name state.

### 7.2 `WATCH`

`WATCH` is semantically defined by:

- logical DB name
- key name

When `SWAPDB 0 1` happens, Redis-visible behavior is about the logical DB identities being swapped.

So the watch ledger stays logical, and `SWAPDB` performs watch invalidation/fingerprint fixup against that logical ledger.

### 7.3 Blocking and ready ledgers

Blocking ledgers are similarly tied to:

- client intent against a logical DB/key

They are not data-bearing storage.

So they stay outside `DbRuntime`, with explicit fixup rules during binding swaps.

## 8. `SWAPDB` Semantics

### 8.1 Target algorithm

`SWAPDB a b` should do:

1. invalidate/fence logical-name-sensitive ledgers
2. swap `bindings[a]` and `bindings[b]`
3. repair or wake logical-name-scoped wait/watch/tracking state as required
4. return

The runtime objects themselves are not rewritten.

### 8.2 Complexity target

Target complexity:

- data movement: `O(1)`
- data-derived index movement: `O(1)`
- control-plane fixup: linear only in watchers/blockers/tracking/session bookkeeping that is logically tied to the DB names

Prohibited complexity:

- entry-count linear remap
- payload-size linear copy
- DB snapshot + reimport

### 8.3 Why this works

It works because all data-bearing state is runtime-owned.

After the binding swap:

- logical DB `a` now points at runtime `Rb`
- logical DB `b` now points at runtime `Ra`

No data structure inside `Ra` or `Rb` needs to be rewritten just because the logical names changed.

## 9. Background Tasks

### 9.1 Active expire

Active expire should run against `DbRuntimeId`, not hard-coded DB0 structures and not an auxiliary-db special path.

A worker should either:

- iterate runtime registry directly, or
- resolve `DbName -> DbRuntimeId` at task scheduling time and then operate on the runtime

Prefer the first when the work is truly data-owned.

### 9.2 Hot object materialization

Set/zset/stream hot runtime state belongs to the runtime.

Materialization queues and flush hooks should be owned by `DbRuntime`.

### 9.3 Metrics

Metrics should distinguish:

- runtime-owned raw counters
- logical-name rendered views

This avoids smuggling logical-name dependence into the runtime object.

## 10. `FLUSHDB`, `FLUSHALL`, `MOVE`, and `MIGRATE`

### 10.1 `FLUSHDB`

`FLUSHDB` is `DbLocal`.

Two valid implementations exist:

- clear the currently bound runtime in place
- install a fresh empty runtime for that logical DB and retire the old one safely

The second is attractive because it matches binding-based ownership cleanly.

### 10.2 `FLUSHALL`

`FLUSHALL` is `Global`.

It may:

- replace all logical bindings with fresh empty runtimes
- retire the old runtimes after in-flight readers finish

This is cleaner than coordinating N different storage shapes.

### 10.3 `MOVE`

`MOVE srcdb dstdb key` becomes a coordinated cross-runtime transfer:

- source runtime lookup
- destination runtime existence check
- export/import or direct typed move
- logical ledgers updated at the DB boundary

This is a `Global` or cross-`DbLocal` coordinator operation, not an ambient DB trick.

### 10.4 `MIGRATE`

`MIGRATE` remains a command-layer/network feature, but it should read from a `DbRuntime` and serialize from the runtime-owned entry representation.

Its DB interaction becomes simpler once every DB shares the same runtime shape.

## 11. Memory Layout And Lifetime

### 11.1 Stable runtime lifetime

`DbRuntime` should be reference-counted or otherwise safely retained while:

- owner-thread work is in flight
- background tasks are in flight
- snapshot/replication reads are in flight

This is why the binding table should reference stable runtime identities/handles rather than inline runtime structs that move during swap.

### 11.2 Runtime retirement

If operations like `FLUSHDB` or `FLUSHALL` install new runtimes, the old runtime must be retired only after:

- outstanding readers finish
- background work is drained or transferred

This can reuse the same epoch/hazard style already present elsewhere in the project.

## 12. Migration Plan From Current Architecture

### Stage 1: introduce explicit runtime identity

- add `DbRuntimeId`
- make logical bindings map `DbName -> DbRuntimeId`

### Stage 2: wrap DB0 state as one runtime instance

- move current `MainDbRuntime` and current side-state lanes behind one `DbRuntime`

### Stage 3: replace auxiliary storage shape

- stop using `AuxiliaryDbState` as a separate DB class
- represent nonzero DBs with the same `DbRuntime` type

### Stage 4: move runtime-owned side maps into the runtime

- forced encodings
- hot object state
- object access metadata
- DB-derived indexes and cursors

Only truly logical ledgers remain outside.

### Stage 5: convert `SWAPDB` to pure binding swap plus logical fixup

- no side-map special cases
- no data movement

## 13. Expected Benefits

### 13.1 Correctness

- one structural definition of "a DB"
- no DB0-only correctness path
- `SWAPDB` stops depending on a long manual side-map list

### 13.2 Code cleanliness

- command handlers can talk to `DbHandle` / `DbRuntime` without knowing special DB classes
- background tasks get one ownership model
- tests no longer need special-case expectations for DB0 versus DBn

### 13.3 Performance

- `SWAPDB` data movement stays constant-time
- `FLUSHDB` / `FLUSHALL` can later exploit runtime replacement
- background work and caches become runtime-local, improving locality

## 14. Decision

The project should converge on:

- one first-class `DbRuntime` type for every logical DB
- a binding table from `DbName` to `DbRuntimeId`
- a small separate logical-name ledger for watch/block/session semantics

This is the clean ownership model needed for:

- real MultiDB
- non-special DB0
- durable `SWAPDB` semantics
