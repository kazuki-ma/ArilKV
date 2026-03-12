# Design Doc: Unified Typed KV Runtime

Date: 2026-03-12
Status: Proposed
Scope: `garnet-rs` greenfield runtime shape for one-lookup typed key/value storage
Related:
- `TODO_AND_STATUS.md` `11.490`
- `docs/compatibility/multidb-db-model-and-command-boundary-redesign-2026-03-11.md`
- `docs/performance/design-owner-thread-actor-only-2026-02-21.md`

## 1. Executive Summary

If `garnet-rs` were implemented again from scratch, the current split between:

- `string_store`
- `object_store`
- separate per-key side metadata maps

would not be kept.

The greenfield target is:

- one typed KV runtime per DB
- one shard lookup to reach the owning entry
- one overwrite path regardless of value kind
- metadata that travels with the owning entry/DB runtime instead of being coordinated across multiple stores

This design does not propose an immediate rewrite of the current codebase. It defines the target architecture that later iterations can move toward.

## 2. Current Baseline

Today the main runtime has two separate Tsavorite-backed data stores per shard:

- `MainDbRuntime.string_stores`
- `MainDbRuntime.object_stores`

and additional side structures:

- `string_expirations`
- `hash_field_expirations`
- `string_key_registries`
- `object_key_registries`
- forced-encoding maps
- object access metadata
- set hot-state / debug state

Consequences:

1. key existence and overwrite semantics often need cross-store coordination
2. string-vs-object replacement requires explicit delete/cleanup paths
3. expiry/watch/access metadata is not naturally co-owned with the value entry
4. page sizing and persistence behavior are configured separately for strings and objects
5. the runtime has to answer "where does this key live?" before it can answer "what is this key?"

Representative examples in the current code:

- string reads go through `read_string_value(DbKeyRef)`
- object reads go through `object_read(DbKeyRef)`
- overwrite helpers such as `key_type_snapshot_for_setkey_overwrite(...)` and `delete_string_value_for_object_overwrite(...)` exist because type ownership is physically split

This model is workable, but it is not the clean or fast endpoint.

## 3. Problem Statement

The split string/object model creates both performance cost and engineering debt.

### 3.1 Performance costs

- some operations must probe or coordinate two stores to establish final type/value state
- object families are encoded as monolithic payload blobs, so mutation commonly implies deserialize -> mutate -> reserialize
- metadata locality is poor because entry state is distributed across separate maps
- overwrite flows pay extra branching and cleanup cost to keep the two stores coherent

### 3.2 Code-quality costs

- type semantics are expressed by control flow instead of by the storage model
- commands that should conceptually "replace one entry" instead perform multiple physical operations
- DB-state ownership is harder to reason about because value data and metadata are not one object graph
- new commands tend to re-encode the same "string/object/expiry/watch cleanup" choreography

## 4. Goals

The greenfield `Unified Typed KV Runtime` should:

1. make one entry the source of truth for key type and value ownership
2. make one shard lookup sufficient to find that entry
3. make overwrite semantics uniform across all value families
4. allow metadata to be attached to the owning entry or DB runtime intentionally
5. preserve owner-thread execution as the mutation model
6. leave room for later typed runtime objects for set/zset/stream families

## 5. Non-Goals

This document does not itself define:

- the final `WAIT` / `WAITAOF` durability ledger
- the final cluster control-plane architecture
- the exact command-scope API (`KeyLocal` / `DbLocal` / `Global`)
- the full typed runtime-object model for complex values

Those are handled by `11.491`, `11.493`, `11.496`, and `11.497`.

## 6. Proposed Runtime Shape

### 6.1 One DB runtime, one typed entry space

Each logical DB owns a `DbRuntime`.

Each `DbRuntime` owns per-shard typed KV maps:

```rust
struct DbRuntime {
    shards: PerShard<DbShardRuntime>,
    background: DbBackgroundState,
}

struct DbShardRuntime {
    entries: OrderedMutex<TypedKvStore<OwnedKey, EntryRecord>>,
    expiry_index: OrderedMutex<ExpiryIndex>,
    key_registry: OrderedMutex<KeyRegistry>,
}
```

The important part is not the exact Rust type. The important part is:

- one entry space per shard
- one physical owner for each key
- no split between "string path" and "object path"

### 6.2 Entry shape

```rust
struct EntryRecord {
    meta: EntryMeta,
    value: EntryValue,
}

struct EntryMeta {
    expiration: Option<ExpirationMetadata>,
    access: AccessMetadata,
    watch_version: WatchVersion,
    encoding_hints: EncodingHints,
}

enum EntryValue {
    String(StringValue),
    List(ListValue),
    Hash(HashValue),
    Set(SetValue),
    Zset(ZsetValue),
    Stream(StreamValue),
}
```

Interpretation:

- entry metadata is attached to the key entry rather than scattered across separate global maps
- the value kind is explicit at the storage boundary
- type checks are branch-on-enum, not probe-two-stores-and-clean-up

### 6.3 Overwrite semantics

A command that overwrites a key performs:

1. one shard lookup
2. one read/modify/write against `EntryRecord`
3. one metadata update path

Examples:

- `SET` over former `Stream`: replace `EntryValue::Stream(...)` with `EntryValue::String(...)`
- `ZUNIONSTORE` over former string: replace `EntryValue::String(...)` with `EntryValue::Zset(...)`
- `DEL`: remove one entry record and its attached metadata together

This removes the current need for helpers that explicitly coordinate:

- string delete
- object delete
- metadata cleanup
- overwrite event synthesis

## 7. Metadata Ownership

The current codebase proves that some metadata is logically key-local but physically side-owned:

- expiration
- LFU/LRU access state
- forced encoding floors
- watch invalidation state
- set hot-state

The greenfield rule is:

- if metadata exists because a key exists, it belongs either in the entry or in a DB-local structure whose ownership is indexed by that entry

Concretely:

- expiration deadline should be reachable from the entry record directly, with a DB-local expiry index for scheduling
- access metadata should be entry-local or stored in a DB-local cache keyed by stable entry identity
- watch version should be owned by the entry record or by a DB-local watch ledger keyed by the entry

The main goal is to stop treating metadata as "extra global maps we hope to remember during every overwrite path".

## 8. Value Representation Strategy

This design deliberately separates two layers:

1. `EntryValue` tells us the logical value kind
2. the internal representation of that value kind may still evolve

For a first greenfield implementation:

- `StringValue` can stay compact and byte-oriented
- `ListValue` / `HashValue` / `SetValue` / `ZsetValue` / `StreamValue` may start with serialized or semi-serialized backing

For the later endpoint:

- complex values should become owner-thread mutable runtime objects
- persistence/replication serialization becomes an adapter boundary, not the primary mutation representation

That second step is intentionally left to `11.497`.

## 9. Persistence And Replication Boundary

The typed runtime does not mean persistence must store raw Rust objects.

The intended boundary is:

- command execution mutates `EntryRecord` / `EntryValue`
- persistence and replication consume typed mutation events or snapshot adapters

That means:

- the in-memory runtime can stay optimized for mutation/locality
- the persistence format can stay stable/versioned
- replication can be driven by typed events instead of by command rewrite

This design therefore pairs naturally with `11.491`.

## 10. MultiDB Implications

This design assumes the long-term `DbRuntime` direction from the MultiDB redesign:

- all logical DBs are peers
- DB0 is not structurally special
- `SWAPDB` swaps DB ownership/bindings, not key payloads

The unified typed KV runtime helps because:

- each DB owns one coherent entry graph
- metadata no longer has to be moved across parallel string/object worlds
- there is no dual-store skew to fix during swap/import/flush

## 11. Migration Plan From Current Architecture

This is a target architecture, not an immediate rewrite. A realistic migration would be staged.

### Stage 1: API unification

- keep current physical stores
- define a unified internal API around `DbHandle` + typed key/value ownership
- push overwrite/replace semantics behind one storage boundary

Outcome:

- commands stop caring whether bytes came from "string store" or "object store"

### Stage 2: value-model unification

- introduce a typed `EntryValue` abstraction over current string/object payloads
- route type checks and overwrite decisions through that abstraction

Outcome:

- logical entry semantics become uniform before physical storage changes

### Stage 3: physical store collapse

- replace split `string_stores` + `object_stores` with one typed per-shard store
- replace separate key registries/metadata ownership with unified shard-owned structures

Outcome:

- one lookup / one overwrite path becomes physically true, not just logically wrapped

### Stage 4: runtime-object optimization

- move complex value families to owner-thread mutable runtime objects
- persistence/replication becomes adapter-driven

Outcome:

- the highest-cost deserialize/reserialize paths disappear from hot mutation loops

## 12. Expected Wins

### 12.1 Performance

- fewer lookups and fewer cross-store conditionals on overwrite paths
- better cache locality for entry + metadata access
- cleaner path to owner-thread mutable objects for sets/zsets/streams
- less duplicated work in type checks and store coordination

### 12.2 Code cleanliness

- one clear answer to "where does a key live?"
- one clear answer to "what type is this key?"
- overwrite semantics become local and uniform
- fewer helper families dedicated only to string/object split cleanup

## 13. Risks

1. one unified store record may increase average entry size if metadata is packed naively
2. a direct rewrite would be too disruptive without staged adapters
3. persistence format and recovery logic become a central dependency and must be versioned carefully
4. some metadata is operationally DB-local, not strictly entry-local, and should not be forced into the record if that harms hot-path size

## 14. Decision

For greenfield `garnet-rs`, the right storage target is:

- `DbRuntime`-owned
- typed
- one-lookup
- metadata-coherent

The current repository should not jump there immediately, but future large redesign work should be evaluated against this target rather than against the existing split string/object model.
