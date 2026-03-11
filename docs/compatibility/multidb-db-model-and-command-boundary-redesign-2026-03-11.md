# MultiDB DB Model And Command Boundary Redesign

Date: 2026-03-11
Status: Proposed
Scope: `garnet-rs` MultiDB ownership model, `SWAPDB`, and command execution boundaries
Supersedes: `docs/compatibility/multidb-db-explicit-interface-design-2026-03-11.md` as the architectural target. The older document remains useful as a mechanical-slice log.

## 1. Executive Summary

The current `garnet-rs` MultiDB implementation is functionally improving, but its data model is still wrong for a production-quality `SWAPDB` and for a clean command boundary.

Today:

- command execution selects a DB by writing `selected_db` into request-local ambient state
- many commands still build `DbKeyRef::new(current_request_selected_db(), ...)` at the command body
- DB0 uses sharded Tsavorite stores while `db>0` uses an auxiliary `HashMap` model
- `SWAPDB` is implemented as snapshot + flush + reimport

This means:

- `SWAPDB` is linear in entry count and payload size
- storage-owned side state is at risk of drifting or being forgotten during swap
- the codebase still conflates logical DB identity with physical storage placement
- the command boundary is not explicit enough to support a clean DB-first model

This document proposes a redesign with three central decisions:

1. Treat logical DB identity and physical storage ownership as separate concepts.
2. Replace ambient selected-DB lookup in command internals with explicit command contexts.
3. Make `SWAPDB` a logical-to-physical mapping swap plus control-plane fixup, not an entry migration.

The design target is:

- no `SWAPDB` path that is linear in key count
- no storage helper that defaults to DB0
- no internal helper that rediscovers DB from thread-local ambient state
- a command model split into `KeyScoped`, `DbScoped`, and `Global` operations

## 2. Problem Statement

### 2.1 Current `garnet-rs` model

The current `RequestProcessor` stores DB-related state in two different shapes:

- DB0 data lives in top-level sharded Tsavorite stores and side tables
- nonzero DB data lives in `auxiliary_databases: Mutex<HashMap<DbName, AuxiliaryDbState>>`

Relevant current fields include:

- `string_stores`
- `object_stores`
- `string_expirations`
- `hash_field_expirations`
- `string_key_registries`
- `object_key_registries`
- `forced_list_quicklist_keys`
- `forced_raw_string_keys`
- `forced_set_encoding_floors`
- `set_object_hot_state`
- `set_debug_ht_state`
- `auxiliary_databases`

The selected DB is held in thread-local request context:

- `RequestExecutionContext.selected_db`
- accessed via `current_request_selected_db()`
- temporarily overridden via `with_selected_db(...)`

This means that many command implementations are still structurally of the form:

```rust
let key = DbKeyRef::new(current_request_selected_db(), raw_key);
self.read_string_value(key)?;
```

That is better than hidden helper fallback to DB0, but it is still not the clean end state.

### 2.2 Why the current model is structurally wrong for `SWAPDB`

The current `SWAPDB` implementation is in `server_commands.rs` and performs:

1. `snapshot_current_db_entries()` for DB1
2. `snapshot_current_db_entries()` for DB2
3. `flush_current_db_keys()` for DB1
4. `flush_current_db_keys()` for DB2
5. `import_migration_entry(...)` of DB2 snapshot into DB1
6. `import_migration_entry(...)` of DB1 snapshot into DB2

That makes the current `SWAPDB`:

- `O(n1 + n2)` in entry count
- `O(size1 + size2)` in payload bytes
- `O(size1 + size2)` in temporary memory

This is not just an implementation detail.

It follows from the current data model:

- DB state is not represented as a first-class swappable ownership unit
- storage-owned metadata is spread across top-level processor fields
- logical DB identity is encoded into keys (`DbScopedKey`) instead of being represented by a DB object boundary

### 2.3 Why `DbScopedKey` alone does not solve this

`DbScopedKey` is a correctness improvement for some classes of bugs:

- it prevents same-name keys in different DBs from aliasing accidentally
- it removes some ambient selected-DB peeking from helpers

But it does not solve the `SWAPDB` data-model problem.

If storage-owned state is keyed by logical `DbName`, then `SWAPDB` must either:

- rewrite all those keys on swap, which is linear in entry count, or
- leave them untouched, which is incorrect

So the key point is:

- `DbScopedKey` is suitable for logical-namespace control-plane state
- it is not the right long-term representation for storage-owned DB internals that must move with swapped data

## 3. `SWAPDB` Requirements

The redesign is driven by explicit `SWAPDB` requirements.

### 3.1 Functional requirements

`SWAPDB a b` must preserve Redis/Valkey-visible semantics for:

- key/value contents
- TTL / expiration state
- object metadata that is part of the data
- `OBJECT ENCODING`-relevant state
- hash-field expiration state
- set hot-state materialization semantics
- `WATCH` invalidation behavior
- blocked-list / blocked-zset / blocked-stream wake behavior
- keyspace visibility for `KEYS`, `SCAN`, `DBSIZE`, `INFO keyspace`, digest/reload/snapshot paths
- replication / AOF / snapshot semantics that refer to logical DB identity

### 3.2 Complexity requirements

The target is not strict mathematical `O(1)` for the full command.

Valkey itself documents `SWAPDB` as:

- `O(N) where N is the count of clients watching or blocking on keys from both databases`

That is a reasonable target for `garnet-rs` too.

The important prohibition is:

- `SWAPDB` must not be `O(entry_count)`
- `SWAPDB` must not be `O(total_payload_bytes)`
- `SWAPDB` must not allocate temporary copies of both databases' contents

So the practical target is:

- data movement: constant-time mapping swap
- control-plane fixup: linear only in watcher / blocker / session bookkeeping that is logically tied to DB identity

### 3.3 Engineering-quality requirements

- No helper may default to DB0 implicitly.
- No helper may rediscover DB from request-local ambient state once it is inside DB-owned logic.
- Command bodies should operate against explicit boundary objects.
- The post-merge code should be cleaner than the current code even if the patch is large.

## 4. Reference Designs

## 4.1 Valkey

Valkey is DB-first.

Key facts from source:

- clients hold a DB pointer: `client->db`
- `selectDb(client *c, int id)` updates `c->db`
- commands are shaped as `void someCommand(client *c)`
- command bodies operate directly on `c->db`

Examples:

- `selectDb(client *c, int id)` in `src/db.c`
- `setGenericCommand(client *c, ...)` in `src/t_string.c` reads and writes through `c->db`
- `serverDb` is a first-class DB object in `src/server.h`

`serverDb` contains, among other things:

- `keys`
- `expires`
- `keys_with_volatile_items`
- `blocking_keys`
- `blocking_keys_unblock_on_nokey`
- `ready_keys`
- `watched_keys`
- expiry cursors / stats

`SWAPDB` is implemented in `dbSwapDatabases(int id1, int id2)`.

It does not migrate entries. It swaps the data-bearing parts of the DB structs:

- `keys`
- `expires`
- `keys_with_volatile_items`
- expiry metadata

It intentionally does not swap:

- `blocking_keys`
- `ready_keys`
- `watched_keys`

Instead it performs logical-DB control-plane fixup:

- `touchAllWatchedKeysInDb(...)`
- `scanDatabaseForDeletedKeys(...)`
- `scanDatabaseForReadyKeys(...)`

Interpretation:

- Valkey keeps data ownership and logical DB control-plane separate enough to swap data without migrating entries
- command execution boundary is session-bound: the client already points at a DB before command execution begins

## 4.2 Original .NET Garnet

Original `.NET Garnet` is also DB-first, but in a session-centric way.

Key facts from source:

- `RespServerSession` holds `activeDbId`
- `RespServerSession` also holds `storageSession`, `basicGarnetApi`, `lockableGarnetApi`, `txnManager`
- switching DB swaps the active database session bound to the RESP session
- command dispatch methods pass `ref storageApi` into command handlers

Representative code:

- `RespServerSession` fields: `activeDbId`, `storageSession`, `basicGarnetApi`, `lockableGarnetApi`
- `TrySwitchActiveDatabaseSession(int dbId)` resolves or creates a `GarnetDatabaseSession` and makes it active
- `SwitchActiveDatabaseSession(...)` replaces the session's active storage/API objects
- `ProcessBasicCommands<TGarnetApi>(..., ref storageApi)` dispatches commands with a DB-bound API already in hand

`.NET Garnet` also has first-class DB objects:

- `GarnetDatabase`
- contains `MainStore`, `ObjectStore`, `AppendOnlyFile`, `VersionMap`, `VectorManager`, and related per-DB state

`TrySwapDatabases(int dbId1, int dbId2)` in `MultiDatabaseManager` swaps database objects in the database map instead of migrating entries.

Interpretation:

- the command boundary is cleaner than current `garnet-rs`
- commands execute against an already-bound DB session / storage API
- DB identity is represented by a session + database object, not by ambient selected-DB lookup inside command helpers

Limitation noted from current source:

- the current `.NET Garnet` swap path has explicit session constraints and active-session rewiring logic
- that limitation does not change the architectural lesson: it still solves `SWAPDB` via DB-object swap, not entry migration

## 4.3 Lessons for `garnet-rs`

From both reference implementations:

- the important thing is not the exact syntax of the command function signature
- the important thing is that command bodies run against an explicit DB binding
- `SWAPDB` is solved by swapping DB ownership or DB mapping, not by copying entries

## 5. Current `garnet-rs` Boundary And Why It Is Insufficient

Current boundary shape:

1. session code tracks `selected_db`
2. `RequestProcessor::execute_in_db(...)` writes it into `RequestExecutionContext`
3. command bodies rediscover it via `current_request_selected_db()`
4. helpers often branch on `db == DbName::default()` to decide whether to touch Tsavorite or auxiliary state

This is insufficient because:

- session boundary resolves only a logical DB number, not a DB runtime object
- command bodies still depend on ambient state instead of explicit DB binding
- DB0 versus nonzero DB is still a pervasive special case
- storage ownership is not represented by a swappable DB runtime

The core defect is not just the helper shape.

It is that the codebase still lacks a stable ownership boundary equivalent to:

- Valkey `serverDb`
- `.NET Garnet` `GarnetDatabase` / `GarnetDatabaseSession`

## 6. Proposed Redesign

## 6.1 Central design decision

Separate:

- logical DB identity
- physical storage ownership

These are currently conflated.

They must become separate first-class concepts.

## 6.2 Proposed runtime objects

```rust
struct DbCatalog {
    logical_to_storage: Vec<StorageSlotId>,
    storages: Vec<DbRuntime>,
}

struct DbRuntime {
    backend: DbBackend,
    storage_state: DbStorageState,
}

enum DbBackend {
    PrimarySharded(PrimaryStoreRuntime),
    AuxiliaryMap(AuxiliaryStoreRuntime),
}

struct DbHandle<'a> {
    logical_db: DbName,
    storage_slot: StorageSlotId,
    runtime: &'a DbRuntime,
}
```

This does not require all DBs to use identical physical backends on day one.

It does require all command and storage code to stop assuming:

- logical DB0 always means the Tsavorite-backed store
- logical DB nonzero always means auxiliary `HashMap`

After this redesign:

- logical DB0 may resolve to an auxiliary-backed `DbRuntime`
- logical DB9 may resolve to the current primary Tsavorite-backed `DbRuntime`
- `SWAPDB` only swaps `logical_to_storage[a]` and `logical_to_storage[b]`

That is the essential structural change.

## 6.3 What belongs inside `DbRuntime`

Storage-owned state must move with the data.

That means it must be inside `DbRuntime` or owned by the backend beneath it.

This includes at least:

- string values
- object values
- key TTL metadata
- hash-field expiration metadata
- key registries / key enumeration state
- set hot-state and writeback caches
- encoding floors / forced encodings that describe the stored object
- debug HT state that describes the stored object
- DB-local active-expire cursors and TTL stats
- per-DB LRU/LFU metadata if it is meant to move with the data

Important consequence:

- current top-level maps keyed by `DbScopedKey` are not the right final home for storage-owned side state

In the target design, once code is inside `&DbHandle`, DB identity is already fixed by the receiver.

So internal storage helpers should accept:

- `&DbHandle` plus raw key bytes, or
- methods on `DbHandle` itself

not another DB-rediscovering layer.

## 6.4 What remains logical-DB control-plane state

Some state should remain keyed by logical DB identity, not storage slot.

This includes:

- watch version namespace
- blocked wait registrations by logical DB
- client selected DB
- transaction selected DB state
- replication `SELECT` boundaries
- keyspace notification DB ids
- metrics labeled by logical DB number

This matches the Valkey approach where `watched_keys` and `blocking_keys` stay with the logical DB and are repaired after swap.

## 6.5 What remains global state

Some state is neither DB-local nor storage-local.

Examples:

- slowlog
- latency monitor
- pubsub server-wide registries
- global command accounting
- server config
- script cache
- replication coordinator

These belong in a `ServerContext` / `GlobalCommandContext` layer.

## 7. Command Execution Boundary Redesign

## 7.1 Command taxonomy

The command surface should be partitioned into three kinds.

### 7.1.1 `KeyScoped`

A command that operates on one key or on a small set of keys all resolved from the same DB handle.

Examples:

- `GET`
- `SET`
- `DEL`
- `HGET`
- `SADD`
- `XADD`

Boundary requirement:

- the command must run against an explicit `DbHandle`
- key arguments cross the boundary as key bytes or `DbKeyRef`
- no command body should call `current_request_selected_db()`

### 7.1.2 `DbScoped`

A command that needs full access to one DB's namespace.

Examples:

- `SCAN`
- `KEYS`
- `DBSIZE`
- `FLUSHDB`
- `RANDOMKEY`
- per-DB `INFO keyspace` rendering

Boundary requirement:

- the command receives an explicit `DbHandle`
- it may enumerate or mutate multiple keys inside that DB freely
- it must not depend on ambient selected-DB state

### 7.1.3 `Global`

A command that must coordinate across DBs or across server-wide state.

Examples:

- `SWAPDB`
- `MOVE`
- `COPY ... DB`
- `FLUSHALL`
- `SAVE`
- `BGSAVE`
- `DEBUG RELOAD`
- `CONFIG`
- `INFO`

Boundary requirement:

- the command receives a `DbCatalog` / `ServerContext`
- it explicitly resolves one or more `DbHandle`s as needed
- special cases like `SWAPDB` stay global instead of contaminating the common DB-local paths

## 7.2 Proposed execution contexts

```rust
struct CommandClientContext {
    client_id: Option<ClientId>,
    client_no_touch: bool,
    in_transaction: bool,
    resp_protocol: RespProtocolVersion,
}

struct DbCommandContext<'a> {
    logical_db: DbName,
    db: DbHandle<'a>,
    client: CommandClientContext,
    server: &'a ServerContext,
}

struct GlobalCommandContext<'a> {
    selected_db: DbName,
    catalog: &'a DbCatalog,
    client: CommandClientContext,
    server: &'a ServerContext,
}
```

The key point is:

- session/dispatch resolves the selected logical DB once
- command code receives a context, not ambient DB state

## 7.3 What remains acceptable at the outer boundary

Ambient request-local context can still exist at the true external edge for:

- protocol/session bookkeeping
- temporary compatibility during migration

But it should stop being part of the core storage command model.

A good rule is:

- ambient selected DB may exist in the session layer
- it may not be consulted inside DB-owned helpers

## 8. Proposed `SWAPDB` Algorithm

Given the redesign above, `SWAPDB(a, b)` becomes:

1. resolve the two logical DB names
2. capture the old storage-slot mapping for `a` and `b`
3. swap `logical_to_storage[a]` and `logical_to_storage[b]`
4. perform logical-DB control-plane fixup for `a` and `b`
5. return `OK`

No entry enumeration. No snapshot. No reimport.

### 8.1 Required control-plane fixup

At minimum the swap path must do the equivalent of Valkey's logical-DB repair:

- invalidate or bump watched keys in logical DB `a` and `b`
- reevaluate blocked waiters registered on logical DB `a` and `b`
- reevaluate stream/list/zset readiness for those logical DBs
- emit the correct logical DB notifications/dirty accounting

The complexity target is:

- proportional to watcher/blocker/session bookkeeping touched by the two logical DBs
- not proportional to entry count

### 8.2 Why this is compatible with current tests

The existing external compatibility suite is already exercising:

- `SWAPDB`
- blocked lists around `SWAPDB`
- `WATCH` / `EXEC`
- `INFO keyspace`
- reload/digest/snapshot paths

That means the repo already has strong correctness probes. The redesign is large, but testability is not the blocker.

## 9. Migration Strategy

A clean implementation path is incremental but architectural, not cosmetic.

## 9.1 Phase A: design-first scaffolding

- introduce `DbCatalog`
- introduce `StorageSlotId`
- introduce `DbHandle`
- document state partition: logical, storage-owned, global

No behavior change yet.

## 9.2 Phase B: move command boundary first

- change command invocation to resolve a `DbHandle` before command execution
- add `DbCommandContext` and `GlobalCommandContext`
- stop new code from calling `current_request_selected_db()` in command bodies

This is the highest-leverage structural cleanup because it removes ambient DB lookup from the execution model.

## 9.3 Phase C: move storage-owned side state under DB ownership

Move, one family at a time:

- TTL metadata
- hash-field TTL metadata
- key registries
- set hot-state
- forced encodings / debug state
- DB-local key visibility helpers

The important rule is:

- if a piece of state must move with `SWAPDB`, it belongs to `DbRuntime`, not to a processor-wide map keyed by logical DB name

## 9.4 Phase D: implement mapping-based `SWAPDB`

Once storage ownership is representable via `DbRuntime`, replace snapshot/flush/reimport with:

- mapping swap
- watch/block/session fixup

## 9.5 Phase E: delete residual ambient selected-DB internals

At this point, remaining `current_request_selected_db()` call sites should be confined to:

- session protocol boundary
- true global admin commands that resolve explicit DB handles as part of their work

## 10. Validation Plan

## 10.1 Exact Rust regressions to add before the redesign lands

Add exact regressions for:

- `SWAPDB` preserves `OBJECT ENCODING`-relevant state without entry migration
- `SWAPDB` preserves hash-field expiration state
- `SWAPDB` preserves set hot-state visibility
- `SWAPDB` preserves `WATCH` invalidation behavior
- `SWAPDB` wakes blocked clients based on post-swap visibility only
- `SWAPDB` keeps `INFO keyspace`, `KEYS`, `SCAN`, and digest/snapshot paths consistent

## 10.2 External targeted suites

Keep targeted no-singledb validation around:

- `unit/keyspace`
- `unit/multi`
- `unit/type/list`
- `unit/info-keysizes`
- `unit/other`

These have already exposed the most important MultiDB correctness gaps.

## 10.3 Full rerun gate

After the mapping-based `SWAPDB` lands:

- `make test-server`
- targeted external MultiDB suites
- full compatibility rerun

## 10.4 Benchmark plan

Because this redesign changes the command boundary and DB resolution path, benchmark only at meaningful milestones.

Required benchmark checkpoints:

1. after boundary-only refactor
2. after mapping-based `SWAPDB`

At each checkpoint:

- Garnet before/after with identical parameters
- Dragonfly comparison once per milestone, not per iteration

## 11. TLA+ Scope

This redesign includes concurrency-sensitive control-plane behavior and should be modeled.

Recommended specs:

### 11.1 `SwapDbWatchVisibility`

Model:

- two logical DBs
- watched keys on both DBs
- swap of logical-to-storage mapping
- post-swap transaction visibility

Invariant:

- if visible contents of a watched logical DB change, the watch version observed by `EXEC` must change

### 11.2 `SwapDbBlockingWake`

Model:

- blocked waiters on logical DB `a`
- storage swap between `a` and `b`
- ready / deleted key visibility after swap

Invariant:

- wake behavior depends on post-swap contents visible under the waiter's logical DB, not on pre-swap storage placement

### 11.3 `LogicalDbMapping`

Model:

- logical DB ids
- storage slots
- selected DB resolution
- key-local and db-local operations

Invariant:

- all DB-local operations observe the storage resolved from the current logical DB mapping
- no operation may silently fall back to storage slot 0

## 12. Recommendation

Proceed with the redesign.

This is not optional cleanup.

The current `SWAPDB` implementation is not merely ugly; it is a symptom of the wrong ownership model. The command boundary and DB data model should be corrected together.

The recommended target is:

- session chooses logical DB
- dispatch resolves `DbHandle`
- `KeyScoped` and `DbScoped` commands run against explicit DB contexts
- `Global` commands resolve multiple DBs explicitly
- storage-owned state lives under swappable DB runtimes
- `SWAPDB` swaps logical-to-storage mapping and performs only control-plane fixup

That is the design that aligns with both Valkey and original `.NET Garnet`, while still respecting `garnet-rs`'s current mixed backend reality during migration.
