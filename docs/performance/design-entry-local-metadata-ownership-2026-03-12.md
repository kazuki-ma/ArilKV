# Design Doc: Entry-Local Metadata Ownership

Date: 2026-03-12
Status: Proposed
Scope: greenfield metadata layout for `garnet-rs` entries, DB indexes, and runtime caches
Related:
- `TODO_AND_STATUS.md` `11.492`
- `docs/performance/design-unified-typed-kv-runtime-2026-03-12.md`
- `docs/compatibility/multidb-db-model-and-command-boundary-redesign-2026-03-11.md`

## 1. Executive Summary

The current codebase has multiple kinds of "metadata" stored in different places:

- canonical expiration state
- hash-field expiration state
- LFU/LRU access data
- forced encoding flags
- set hot-state
- debug state
- watch versions
- derived key registries and counters

The main problem is not only that these are split. The bigger problem is that they are split **without a taxonomy**.

Greenfield rule:

1. canonical key semantics belong to the entry
2. lookup/scheduling structures belong to the DB runtime as derived indexes
3. performance-only structures belong to rebuildable caches
4. coordination structures belong to DB-local ledgers, not to ad-hoc side maps

This document defines that taxonomy and maps current metadata into it.

## 2. Current Baseline

### 2.1 Current side-state bucket

Today `DbCatalogSideState` contains:

- `forced_list_quicklist_keys`
- `forced_raw_string_keys`
- `forced_set_encoding_floors`
- `set_object_hot_state`
- `set_debug_ht_state`
- `key_lru_access_millis`
- `key_lfu_frequency`

Separately, the runtime also carries:

- `string_expirations`
- `hash_field_expirations`
- `string_key_registries`
- `object_key_registries`
- `string_expiration_counts`
- hashed `watch_versions`

These structures do not all have the same semantic status, but today they are implemented with similar "side map" mechanics.

### 2.2 Why this is a problem

Without a taxonomy:

- some metadata is treated as if it were canonical when it is really a cache
- some metadata is treated as if it were rebuildable when it is actually externally visible
- `SWAPDB`, overwrite, persistence, and replication need per-feature exceptions

That is exactly how structural drift accumulates.

## 3. Metadata Taxonomy

### 3.1 Canonical metadata

Canonical metadata is part of the logical state of a key.

If the key is reconstructed from snapshot/replication/persistence, this metadata must be reconstructed too.

Examples:

- key expiration deadline
- hash-field expiration state
- externally visible access state if the project chooses exact persistence for it
- representation constraints that materially affect visible semantics

### 3.2 Derived DB-local index

An index is not the source of truth. It exists to schedule or locate canonical state.

Examples:

- expiry priority/index by deadline
- key registry for enumeration
- per-shard counters

If lost, it can be rebuilt from canonical entry data.

### 3.3 Runtime cache

A cache improves mutation/read cost but is not authoritative.

Examples:

- decoded hot object cache
- precomputed encodings
- debug-only statistics snapshots

If lost, it is either rebuilt lazily or disappears without semantic damage.

### 3.4 Coordination ledger

A coordination ledger is runtime state used for transactions, invalidation, blocking, or concurrency visibility.

Examples:

- watch versions
- client tracking invalidation epochs
- blocker/ready state

It should not be mixed into value serialization, but it also should not be an untyped side-map accident.

## 4. Classification Of Current Metadata

### 4.1 Key expiration

Current state:

- canonical expiration metadata exists both in auxiliary entries and in `string_expirations`
- lookup/scheduling is coupled to the same storage shape

Target:

- canonical expiration belongs in the entry header
- DB runtime holds a derived expiry index keyed by stable entry identity

Survival:

- `SWAPDB`: yes
- replication: yes
- persistence: yes

### 4.2 Hash-field expiration

Current state:

- stored in auxiliary entry payload for non-main DBs
- stored in `hash_field_expirations` side maps for main runtime DBs

Target:

- canonical field-expiration state belongs with the owning hash entry value
- DB runtime may keep a secondary index for efficient expiry scans

Survival:

- `SWAPDB`: yes
- replication: yes
- persistence: yes

### 4.3 LFU/LRU access metadata

Current state:

- `key_lru_access_millis`
- `key_lfu_frequency`

These are logically key-local and externally visible through `OBJECT IDLETIME` / `OBJECT FREQ`.

Target:

- store them in the entry header or a tightly coupled entry-local sidecar
- do not keep them as free-floating global maps

Survival:

- `SWAPDB`: yes
- replication: project choice
- persistence: project choice

Greenfield recommendation:

- they should survive logical DB moves
- whether they must survive persistence/replication exactly should be an explicit policy, not an accident

### 4.4 Forced encoding hints

Current state:

- `forced_list_quicklist_keys`
- `forced_raw_string_keys`
- `forced_set_encoding_floors`

These exist because runtime representation and visible `OBJECT ENCODING` behavior are only partially aligned with the stored payload model.

Target:

- if a hint is required for visible semantics, it belongs with the entry
- if a hint only exists to steer a transient runtime representation, it belongs in a cache

Greenfield recommendation:

- reduce these hints by making value representation itself more honest
- only keep explicit entry-local representation hints when externally visible behavior actually depends on them

Survival:

- `SWAPDB`: yes when semantically visible
- replication: only if remote semantics depend on it
- persistence: only if restart semantics depend on it

### 4.5 Set hot-state

Current state:

- `set_object_hot_state` currently carries value-adjacent mutable state outside the canonical object payload

This is the clearest sign that the taxonomy is broken:

- if it contains the authoritative in-memory state, it is not a cache
- if it is only a cache, canonical correctness must not depend on it

Target:

- eliminate this category as separate metadata
- the authoritative set state should live in the entry value/runtime object
- any additional hot cache must be rebuildable from that canonical state

Survival:

- `SWAPDB`: canonical state yes, cache no
- replication: canonical state yes, cache no
- persistence: canonical state yes, cache no

### 4.6 Debug HT state

Current state:

- `set_debug_ht_state` supports debug/inspection behavior

Target:

- treat it as runtime-local observability metadata, not canonical key state

Survival:

- `SWAPDB`: only if the debug surface promises logical-key continuity
- replication: no
- persistence: no

### 4.7 Key registries and expiration counters

Current state:

- `string_key_registries`
- `object_key_registries`
- `string_expiration_counts`

Target:

- all of these are derived DB indexes/counters
- they should be rebuildable from entry ownership plus canonical metadata

Survival:

- `SWAPDB`: rebuilt or swapped as derived DB indexes
- replication: no
- persistence: no

### 4.8 Watch versions

Current state:

- watch invalidation uses hashed `watch_versions`

These are not entry metadata in the durable sense, but they are logically tied to key identity.

Target:

- keep watch state in a DB-local coordination ledger keyed by stable key/entry identity
- do not bury it in unrelated side-state buckets

Greenfield recommendation:

- transaction invalidation state should be explicit coordination metadata, not value metadata

Survival:

- `SWAPDB`: handled by DB-local watcher logic
- replication: no
- persistence: no

## 5. Target Layout

### 5.1 Entry header

```rust
struct EntryHeader {
    expiration: Option<ExpirationMetadata>,
    access: AccessMetadata,
    representation: RepresentationHint,
}
```

This is where metadata goes when it is:

- key-local
- semantically attached to the entry
- not just an index/counter/cache

### 5.2 Entry value

Some metadata belongs inside the value family itself:

- hash-field expirations belong with the hash value
- stream consumer-group state belongs with the stream value

Rule:

- if the metadata only makes sense when you have already typed the value, store it with the value

### 5.3 DB indexes

```rust
struct DbRuntimeIndexes {
    expiry_index: ExpiryIndex,
    key_registry: KeyRegistry,
}
```

Indexes should contain just enough to:

- schedule expiry
- enumerate keys
- answer summary queries efficiently

They should not become shadow sources of truth.

### 5.4 DB coordination ledgers

```rust
struct DbRuntimeCoordination {
    watch_ledger: WatchLedger,
    tracking_ledger: TrackingLedger,
    blocking_state: BlockingLedger,
}
```

These structures are:

- runtime-local
- semantic
- not durable value payload

### 5.5 Rebuildable caches

```rust
struct DbRuntimeCaches {
    decoded_object_cache: ObjectCache,
    debug_state: DebugState,
}
```

Rule:

- caches must be safe to drop without corrupting the DB

## 6. Survival Matrix

| Metadata kind | Canonical? | Survive `SWAPDB` | Survive replication | Survive persistence |
|---|---|---:|---:|---:|
| Key expiration | Yes | Yes | Yes | Yes |
| Hash-field expiration | Yes | Yes | Yes | Yes |
| LFU/LRU access | Usually yes at runtime | Yes | Policy | Policy |
| Encoding hint | Depends | If visible | If needed | If needed |
| Set hot-state | No as separate metadata | No | No | No |
| Debug HT state | No | Optional | No | No |
| Key registries | No | Rebuild/swap index | No | No |
| Expiration counters | No | Recompute | No | No |
| Watch versions | Coordination only | Ledger fixup | No | No |

## 7. Migration Guidance

### Stage 1

- label each current side-state lane as canonical, index, cache, or coordination
- stop adding new unclassified side maps

### Stage 2

- move canonical metadata to entry/value ownership
- keep compatibility adapters for existing command surfaces

### Stage 3

- convert indexes/counters into explicitly rebuildable DB-local structures

### Stage 4

- convert caches into disposable runtime helpers
- remove any correctness dependence on them

## 8. Decision

The greenfield rule set is:

- canonical metadata belongs to entries or typed values
- indexes belong to DB runtime indexes
- caches are rebuildable and non-authoritative
- coordination state belongs to explicit DB-local ledgers

Future large redesign work should classify metadata by this taxonomy before adding new storage structures. If a new field cannot be classified, the design is not ready.
