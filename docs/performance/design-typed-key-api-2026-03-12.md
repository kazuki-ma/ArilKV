# Design Doc: Typed Key API

Date: 2026-03-12
Status: Proposed
Scope: internal key-type boundary for `garnet-rs`
Related:
- `TODO_AND_STATUS.md` `11.495`
- `docs/performance/design-command-execution-scopes-2026-03-12.md`
- `docs/performance/design-first-class-db-runtime-2026-03-12.md`

## 1. Executive Summary

The codebase has improved from implicit DB0 access to `DbKeyRef`, but internal key handling is still inconsistent.

Current reality from a fresh grep:

- `current_request_selected_db()` call sites: `722`
- `DbKeyRef::new(...)` call sites: `642`
- `RedisKey::from(...)` call sites: `182`

This means the project is in an in-between state:

- typed DB-scoped keys exist
- but command bodies still repeatedly build them from ambient DB state and ad-hoc owned-key conversions

Greenfield rule:

- raw `&[u8]` key bytes are only allowed at parse and external-I/O boundaries
- all internal DB-memory/state APIs use typed key forms

The target type set is:

- `KeyRef<'a>`
- `OwnedKey`
- `DbKeyRef<'a>`
- `DbScopedKey`

## 2. Current Baseline

### 2.1 Types that already exist

Today the code already has:

- `KeyBytes = [u8]`
- `KeyRef<'a>`
- `DbKeyRef<'a>`
- `DbScopedKey`

But the owned half is still inconsistent:

- many maps store `RedisKey`
- command bodies create `RedisKey::from(args[i])`
- `DbScopedKey` internally still stores `RedisKey`

### 2.2 Typical current pattern

Many command bodies still look like:

```rust
let key = RedisKey::from(args[1]);
let db_key = DbKeyRef::new(current_request_selected_db(), &key);
```

That is better than passing raw bytes everywhere, but it is still structurally wrong because:

- the owned/borrrowed distinction is implicit
- the DB choice is ambient
- command code performs the same conversions repeatedly

## 3. Problem Statement

The remaining issues are:

### 3.1 Raw bytes cross too far inward

Several DB-state helpers still take `(db, key: &[u8])` or `key: &[u8]` even though they touch memory state.

Representative examples:

- `watch_key_version_in_db(&self, db, key: &[u8])`
- `expire_key_if_needed(&self, key: &[u8])`
- `string_store_shard_index_for_key(&self, key: &[u8])`
- `track_string_key_in_shard(&self, key: &[u8], ...)`
- `bump_watch_version_in_db(&self, db, key: &[u8])`

These are not parse-boundary APIs. They are storage/runtime APIs and should use typed keys.

### 3.2 Owned keys are not first-class

The code has a borrowed key wrapper, but not a proper owned-key domain type.

That is why:

- `RedisKey` keeps leaking as the de-facto internal owned key
- `DbScopedKey` is half-typed rather than fully typed
- sets/maps that conceptually store keys do not advertise that fact in the type system

### 3.3 Borrowed versus owned lifetime is hidden

Without a clear distinction:

- some helpers clone keys too early
- some command bodies allocate just to pass through one function
- some state stores owned keys but the API surface does not say so

## 4. Goals

The typed-key API should:

1. make borrowed versus owned key lifetime explicit
2. make DB-scoped versus DB-free key usage explicit
3. prevent DB-memory APIs from accepting raw `&[u8]`
4. reduce repeated `RedisKey::from(...)` + `DbKeyRef::new(...)` choreography in handlers
5. leave non-key byte namespaces alone unless they are separately typed on purpose

## 5. Non-Goals

This document does not type every byte-string domain in the codebase.

Examples intentionally out of scope for this iteration:

- pubsub channel names
- ACL user names
- config option names
- SHA1 digests
- RESP payload bytes

Those are separate namespaces and should not be conflated with key typing.

## 6. Proposed Type Set

### 6.1 Borrowed key

```rust
#[repr(transparent)]
struct KeyRef<'a>(&'a KeyBytes);
```

Use `KeyRef<'a>` when:

- the key is borrowed from the current request frame
- the key is borrowed from a temporary decoded object
- the callee does not need ownership beyond the current operation

### 6.2 Owned key

```rust
#[repr(transparent)]
struct OwnedKey(RedisKey);
```

Use `OwnedKey` when:

- the key is stored in a map/set/vector
- the key crosses async/task/request boundaries
- the key must outlive the current request frame
- the key participates in durable or replayable metadata

Greenfield rule:

- `RedisKey` should become an implementation detail of `OwnedKey`, not the everyday internal key type

### 6.3 Borrowed DB-scoped key

```rust
struct DbKeyRef<'a> {
    db: DbName,
    key: KeyRef<'a>,
}
```

Use `DbKeyRef<'a>` when:

- accessing or mutating DB memory state
- querying DB-owned metadata
- routing an operation to a DB-local storage helper

### 6.4 Owned DB-scoped key

```rust
struct DbScopedKey {
    db: DbName,
    key: OwnedKey,
}
```

Use `DbScopedKey` when:

- storing DB-scoped metadata in a map/set
- tracking pending invalidations or watch fingerprints by DB+key
- moving a DB-scoped key across task boundaries

## 7. Boundary Rules

### 7.1 Allowed raw-byte boundaries

Raw `&[u8]` is allowed only for:

- RESP parse and serialization
- network protocol helpers
- storage codec/persistence payload decode/encode
- hash/CRC/digest helpers
- non-key string namespaces such as ACL names, pubsub channels, SHA1, config names

### 7.2 Disallowed raw-byte interiors

Raw `&[u8]` is not allowed for:

- DB-memory lookups
- expiry/watch/tracking ledgers keyed by DB keys
- key registries
- key-existence helpers
- overwrite/delete/materialize paths
- per-key stats or access metadata

These must use `KeyRef`, `OwnedKey`, `DbKeyRef`, or `DbScopedKey`.

### 7.3 Construction rule

Within a command handler:

1. parse args as raw bytes
2. immediately form `KeyRef` for key arguments
3. if the handler needs DB state, bind that into `DbKeyRef`
4. convert to `OwnedKey` only when ownership is actually required

The handler should not repeatedly bounce between:

- `&[u8]`
- `RedisKey`
- `DbKeyRef`

## 8. Inventory Of Remaining Internal Raw-Key Paths

The remaining migration surface falls into four buckets.

### 8.1 Command-body construction churn

Large command families still do:

- `RedisKey::from(args[i])`
- `DbKeyRef::new(current_request_selected_db(), &key)`

Representative files:

- `list_commands.rs`
- `set_commands.rs`
- `hash_commands.rs`
- `zset_commands.rs`
- `server_commands.rs`

Target replacement:

- scope-bound `DbHandle` or `KeyHandle` provides `DbKeyRef` directly
- command bodies do not allocate `OwnedKey` unless they store keys

### 8.2 Storage/runtime helpers using raw key bytes

Representative current helpers:

- `string_store_shard_index_for_key(&self, key: &[u8])`
- `object_store_shard_index_for_key(&self, key: &[u8])`
- `track_string_key_in_shard(&self, key: &[u8], ...)`
- `track_object_key_in_shard(&self, key: &[u8], ...)`
- `watch_key_version_in_db(&self, db, key: &[u8])`
- `bump_watch_version_in_db(&self, db, key: &[u8])`

Target replacement:

- `KeyRef` for DB-free routing/hash helpers
- `DbKeyRef` for DB-memory helpers

### 8.3 Owned-key maps storing `RedisKey`

Representative current structures:

- `DbKeySetState`
- `DbKeyMapState<T>`
- tracking stacks and client tracked-key sets
- auxiliary entry maps keyed by `RedisKey`
- registry sets in string/object stores

Target replacement:

- maps/sets store `OwnedKey`
- DB-scoped maps/sets store `DbScopedKey` when DB identity is part of the key

### 8.4 Mixed logical ledgers

Some logical ledgers still accept raw key bytes and then allocate internally:

- tracking invalidation collection
- watched-key fingerprint paths
- ready/blocking helper state

Target replacement:

- `DbScopedKey` when the ledger persists DB+key state
- `OwnedKey` when DB-free key identity is sufficient

## 9. API Guidelines

### 9.1 Function arguments

Use these rules:

- `KeyRef<'_>`: pure key inspection, hash-slot routing, temporary comparisons
- `DbKeyRef<'_>`: any DB-memory or DB-metadata access
- `OwnedKey`: storing a key
- `DbScopedKey`: storing a DB-scoped key

Avoid:

- `(db: DbName, key: &[u8])`
- raw `RedisKey` in public internal APIs

### 9.2 Function return values

Use:

- `OwnedKey` for returned owned keys
- `Vec<OwnedKey>` for owned key collections
- `DbScopedKey` / `Vec<DbScopedKey>` for DB-aware collections

Avoid returning raw `Vec<u8>` for values that are semantically keys.

### 9.3 Internal storage

Use:

- `HashSet<OwnedKey>`
- `HashMap<OwnedKey, T>`
- `HashMap<DbScopedKey, T>`

not:

- `HashSet<RedisKey>`
- `HashMap<RedisKey, T>` for general internal APIs

## 10. Relationship To Command Scopes

This document intentionally pairs with `11.493`.

Once command handlers receive:

- `KeyHandle`
- `DbHandle`
- `GlobalHandle`

the key typing rule becomes cleaner:

- `KeyLocal` handlers mainly consume `DbKeyRef`
- `DbLocal` handlers consume typed key collections from their DB handle
- `Global` handlers use `DbScopedKey` only when persisting DB-aware ledgers

That is how the project stops writing:

```rust
DbKeyRef::new(current_request_selected_db(), ...)
```

throughout command bodies.

## 11. Migration Plan

### Stage 1: introduce `OwnedKey`

- add `OwnedKey` as the canonical internal owned key type
- make `DbScopedKey` store `OwnedKey`

### Stage 2: convert key-bearing internal maps

- `DbKeySetState`
- `DbKeyMapState<T>`
- registry sets
- tracking and watch ledgers

### Stage 3: tighten helper signatures

- replace `(db, &[u8])` with `DbKeyRef`
- replace plain `&[u8]` with `KeyRef` for routing/hash-only helpers

### Stage 4: shrink command-body conversions

- command-scope APIs provide typed key handles directly
- command handlers stop constructing owned keys except when necessary

### Stage 5: add grep gates

Maintain explicit review gates for:

- raw `RedisKey::from(args[...]` inside command bodies
- new `(db: DbName, key: &[u8])` helper signatures
- new DB-memory APIs that accept raw bytes

## 12. Expected Benefits

### 12.1 Code cleanliness

- clearer ownership and lifetime
- fewer repeated conversions
- better separation between parse-boundary and runtime-boundary code

### 12.2 Correctness

- less chance of mixing DB-aware and DB-unaware key identity
- fewer silent allocations or accidental key copies
- easier reasoning about stored ledgers and side maps

### 12.3 Performance

- fewer unnecessary owned-key allocations in command bodies
- more predictable borrowing on hot paths
- cleaner later migration to scope-bound handlers and runtime-owned indexes

## 13. Decision

The project should standardize internal key handling on:

- `KeyRef<'a>`
- `OwnedKey`
- `DbKeyRef<'a>`
- `DbScopedKey`

and treat raw `&[u8]` keys as boundary-only types.

That is the missing half of the current MultiDB cleanup.
