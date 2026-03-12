# Design Doc: Command Execution Scopes

Date: 2026-03-12
Status: Proposed
Scope: greenfield command execution boundary for `garnet-rs`
Related:
- `TODO_AND_STATUS.md` `11.493`
- `docs/compatibility/multidb-db-model-and-command-boundary-redesign-2026-03-11.md`
- `docs/performance/design-unified-typed-kv-runtime-2026-03-12.md`

## 1. Executive Summary

The current command path still mixes three different concerns:

- command parsing and arity validation
- routing decisions
- storage/control-plane execution scope

Today `command_spec.rs` exposes:

- `KeyAccessPattern`
- `OwnerRoutingPolicy`

and the connection layer chooses an owner thread or shard before calling:

```rust
RequestProcessor::execute_with_client_context_in_db(..., selected_db)
```

After that, many command bodies still rediscover the logical DB through ambient request-local state such as:

```rust
DbKeyRef::new(current_request_selected_db(), raw_key)
```

That is the wrong boundary.

Greenfield rule:

1. routing metadata must not be the command execution API
2. every command handler must receive the scope it is allowed to touch
3. ambient selected-DB lookup must disappear from command internals

The target execution taxonomy is:

- `KeyLocal`
- `DbLocal`
- `Global`

## 2. Current Baseline

### 2.1 Current metadata split

`command_spec.rs` currently models:

- `KeyAccessPattern::{None, FirstKey, AllKeysFromArg1}`
- `OwnerRoutingPolicy::{Never, FirstKey, SingleKeyOnly}`

This is enough for:

- cluster slot checks
- first-key owner-thread routing
- some transaction/script key analysis

But it is not enough for execution-boundary correctness.

Example problem:

- `DEL k1 k2` is currently described largely by key-access and first-key routing
- but its real execution scope is "selected DB, potentially multiple keys/shards"
- that means it is not a clean `KeyLocal` command even if current routing envelopes it by first key

### 2.2 Current execution entrypoint

The current owner-routing path calls:

```rust
processor.execute_with_client_context_in_db(
    args,
    &mut response,
    client_no_touch,
    client_id,
    false,
    selected_db,
)
```

inside both inline and owner-thread execution.

That is already better than hidden DB0 fallback. But it still leaves the handler body free to reach back into:

- `current_request_selected_db()`
- `with_selected_db(...)`

This is why the command boundary is still structurally dirty.

### 2.3 Current routing reality

`connection_routing.rs` currently routes:

- `KeyAccessPattern::FirstKey` by `args[1]`
- `KeyAccessPattern::AllKeysFromArg1` also by `args[1]`
- `KeyAccessPattern::None` to shard `0`

This is a performance policy, not a semantic classification.

That distinction matters because:

- some multi-key commands are not truly single-owner operations
- some keyless commands are still DB-local rather than global
- some commands are control-plane global even when they mention one key

## 3. Problem Statement

The current boundary creates three classes of debt.

### 3.1 Ambient DB dependency

Handlers frequently reconstruct `DbKeyRef` by reading ambient state instead of being given a DB-scoped handle.

Consequences:

- command code is harder to reason about locally
- `SELECT`/`SWAPDB`/nested execution paths remain fragile
- tests and replay paths need request-context scaffolding instead of explicit inputs

### 3.2 Routing and execution are conflated

`FirstKey` routing is treated as if it implies `KeyLocal` execution.

That is not generally true.

Examples:

- `DEL k1 k2`
- `MSET k1 v1 k2 v2`
- `WATCH k1 k2`
- `RENAMENX src dst`
- `BLMOVE src dst`

Some of these are:

- DB-local commands touching several keys
- coordinated commands that need ordering/barrier logic
- commands that are only routable by first key as a temporary optimization

### 3.3 Global state is not modeled as a first-class execution scope

Commands such as:

- `SWAPDB`
- `FAILOVER`
- `CLUSTER NODES`
- `INFO`
- `REPLICAOF`

do not fit the same shape as ordinary key mutations.

Treating them as ordinary request handlers with optional ambient DB access makes the internal API blur data-plane and control-plane responsibilities.

## 4. Goals

The new boundary should:

1. make handler scope explicit in the type system
2. keep routing metadata separate from execution metadata
3. let key-local commands run on one owner thread without ambient DB lookup
4. let DB-local commands operate across several keys in one logical DB cleanly
5. let global commands operate on catalog/control-plane state without pretending to be key commands
6. define how transactions, scripts, replay, and background tasks enter the same boundary

## 5. Non-Goals

This document does not itself define:

- the full `DbRuntime` storage layout
- the full control-plane state object
- the final persistence or replication record format

Those are covered by:

- `11.494`
- `11.496`
- `11.491`

## 6. Scope Taxonomy

### 6.1 `KeyLocal`

`KeyLocal` means:

- execution is anchored to one logical DB
- one primary key determines the owning shard/thread
- the command may touch only data reachable through that key-local owner context

Typical examples:

- `GET`
- `SET`
- `INCR`
- `HSET`
- `LPUSH`
- `SADD`
- `ZADD`
- `XADD`

Also included:

- commands whose additional work is still subordinate to the primary key owner
- blocking wake/ready logic for the same owned key family

`KeyLocal` is the fast path.

### 6.2 `DbLocal`

`DbLocal` means:

- execution is anchored to one logical DB
- the command may touch multiple keys, multiple shards, or DB-wide indexes
- the handler is still forbidden from touching other DBs or global control-plane mutation unless explicitly delegated

Typical examples:

- `DEL k1 k2 ...`
- `MSET`
- `WATCH`
- `SCAN`
- `KEYS`
- `DBSIZE`
- `FLUSHDB`
- `MOVE`
- `RENAME` / `RENAMENX`

`DbLocal` is not a "slow fallback". It is the correct semantic scope for commands whose unit of consistency is the logical DB rather than a single key owner.

### 6.3 `Global`

`Global` means:

- execution may coordinate across DBs, connections, topology state, replication state, or process-wide metrics
- the command is allowed to touch `DbCatalog` bindings and control-plane state

Typical examples:

- `SELECT`
- `SWAPDB`
- `FLUSHALL`
- `INFO`
- `CLUSTER *`
- `READONLY`
- `READWRITE`
- `FAILOVER`
- `REPLICAOF`
- `CONFIG` / `LATENCY` / server metrics surfaces

`Global` is the only scope allowed to mutate logical DB bindings or topology.

## 7. Proposed Handler API

### 7.1 Boundary objects

Greenfield target:

```rust
struct RequestEnvelope<'a> {
    client: ClientHandle,
    args: &'a [ArgSlice],
    command: CommandId,
}

struct DbHandle<'a> {
    db: DbName,
    runtime: &'a DbRuntime,
}

struct KeyHandle<'a> {
    db: DbHandle<'a>,
    shard: ShardIndex,
    key: DbKeyRef<'a>,
}

struct GlobalHandle<'a> {
    catalog: &'a DbCatalog,
    control_plane: &'a ControlPlaneSnapshot,
}
```

Important property:

- none of these APIs require `current_request_selected_db()`

### 7.2 Handler traits

```rust
trait KeyLocalCommand {
    fn execute(
        &self,
        request: &RequestEnvelope<'_>,
        key: KeyHandle<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError>;
}

trait DbLocalCommand {
    fn execute(
        &self,
        request: &RequestEnvelope<'_>,
        db: DbHandle<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError>;
}

trait GlobalCommand {
    fn execute(
        &self,
        request: &RequestEnvelope<'_>,
        global: GlobalHandle<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError>;
}
```

The exact Rust surface can vary. The design requirement is:

- handler type tells you what state it may touch

### 7.3 Capability separation

To keep boundaries tight, the handles themselves should expose only the allowed operations.

Examples:

- `KeyHandle` exposes entry-local read/write and key-adjacent wait/watch helpers
- `DbHandle` exposes key enumeration, multi-key coordination, per-DB derived indexes, and DB-wide expiry scans
- `GlobalHandle` exposes DB binding swap, topology snapshot access, replication state, and process metrics

This is better than passing `&RequestProcessor` everywhere.

## 8. Command Metadata Redesign

### 8.1 Add explicit execution scope

The command catalog should carry both routing and execution metadata.

```rust
enum ExecutionScope {
    KeyLocal(KeyScopePlan),
    DbLocal,
    Global,
}

enum KeyScopePlan {
    PrimaryArg(usize),
    SingleDeclaredKey,
}
```

Keep routing metadata separate:

```rust
enum RoutingPlan {
    Never,
    PrimaryKey,
    SingleKeyOnly,
    Coordinator,
}
```

Interpretation:

- execution scope answers "what state may this handler touch?"
- routing plan answers "where should this invocation run?"

The current code mostly has only the second answer.

### 8.2 Classification rule

Classification must follow the true semantic mutation boundary, not the cheapest current route.

That means:

- `GET` is `KeyLocal`
- `DEL many` is `DbLocal`
- `WATCH many` is `DbLocal`
- `SWAPDB` is `Global`
- `CLUSTER COUNTKEYSINSLOT` is `Global` even though it takes one slot argument

This deliberately resists the current "first key means local" shortcut.

## 9. Multi-Key Commands

Multi-key commands are where this design matters most.

### 9.1 Same-owner multi-key commands

Some multi-key commands can still be `KeyLocal` if the command semantics are owner-anchored and all touched keys are required to stay within the same owner boundary.

This should be rare and explicit.

Example candidate:

- a future owner-thread-only internal helper command

### 9.2 Ordinary user-facing multi-key commands

Most user-facing multi-key commands should be `DbLocal`.

Reasons:

- they touch multiple keys
- they may span multiple owner shards inside one DB
- they often need deterministic DB-wide ordering or conflict checks

Examples:

- `DEL`
- `MSET`
- `SUNIONSTORE`
- `ZUNIONSTORE`
- `BLMOVE`

### 9.3 Cross-slot and cluster behavior

Cluster slot validation remains a routing/control concern.

A `DbLocal` command may still be:

- rejected in cluster mode for cross-slot access
- executed under a coordinator if allowed by topology policy

The design rule is:

- cluster validation does not change the semantic execution scope

## 10. Transactions, Scripts, Replay, Background Tasks

### 10.1 Transactions

`MULTI` should queue parsed command plans, not opaque "re-run in ambient DB" frames.

Queued item target:

```rust
struct PlannedInvocation {
    db: DbName,
    command: CommandId,
    scope: ExecutionScope,
    owned_args: Vec<Vec<u8>>,
}
```

At `EXEC` time:

- watch checks run in `DbLocal`
- each queued item executes using its explicit planned scope and DB

### 10.2 Scripting

Scripts are not a fourth permanent scope kind.

Scripts should resolve their allowed key set up front and execute through the same scope planner.

Guideline:

- key-declared script calls become `KeyLocal` or `DbLocal` according to the invoked command
- topology/control-plane mutations from scripts remain `Global` and can be rejected by policy if needed

### 10.3 Replication and AOF replay

Replay paths should not emulate a connection-local ambient selected DB.

Instead they should build:

- `ReplayEnvelope { db, command, args }`

and feed the normal scope planner.

### 10.4 Background tasks

Expiry sweeps, unblock processing, tracking invalidation, and migration helpers are not RESP commands, but they should still use the same boundary objects:

- per-key work via `KeyHandle`
- per-DB scans via `DbHandle`
- topology/catalog work via `GlobalHandle`

That keeps internal invariants aligned.

## 11. Dispatch Flow

Greenfield flow:

1. parse args and identify command
2. validate arity and syntax
3. compute `ExecutionScope`
4. compute routing/topology decision
5. build `KeyHandle`, `DbHandle`, or `GlobalHandle`
6. invoke the typed handler

This differs from today in one critical way:

- scope is decided before execution and carried explicitly into the handler

## 12. Migration Plan From Current Code

### Stage 1: metadata expansion

- extend `CommandSpecEntry` with `execution_scope`
- keep `KeyAccessPattern` and routing metadata temporarily

### Stage 2: typed dispatch adapters

- add new internal dispatch methods for `KeyLocal`, `DbLocal`, and `Global`
- adapt a small set of commands first

### Stage 3: ban ambient DB lookup in migrated handlers

- migrated handlers may not call `current_request_selected_db()`
- keep compile-time or review-time grep gate for remaining call sites

### Stage 4: transaction/replay integration

- queue scope-planned invocations
- replay through typed dispatch

### Stage 5: remove `execute_with_client_context_in_db` as the universal handler entrypoint

- retain it only as a narrow connection boundary adapter, or delete it entirely once all commands are typed

## 13. Expected Benefits

### 13.1 Code cleanliness

- handler inputs describe allowed state directly
- `DbKeyRef::new(current_request_selected_db(), ...)` stops appearing in command bodies
- routing, topology validation, and state mutation become separate phases

### 13.2 Correctness

- `SELECT`, `SWAPDB`, transactions, replay, and nested command execution stop depending on ambient request-local DB state
- global commands cannot accidentally mutate per-key data through the wrong boundary

### 13.3 Performance

- `KeyLocal` remains an explicit fast path
- `DbLocal` commands can later get a coordinator implementation without changing command semantics again
- routing policy can evolve independently from handler scope

## 14. Decision

The project should move to explicit command execution scopes:

- `KeyLocal`
- `DbLocal`
- `Global`

and stop treating first-key routing metadata as the command execution API.

That is the clean boundary needed before:

- first-class `DbRuntime`
- typed key APIs
- live control-plane integration
