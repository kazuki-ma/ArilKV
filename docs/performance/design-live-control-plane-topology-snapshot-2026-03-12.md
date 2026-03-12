# Design Doc: Live Control-Plane Topology Snapshot

Date: 2026-03-12
Status: Proposed
Scope: cluster / replication / durability control-plane unification for `garnet-rs`
Related:
- `TODO_AND_STATUS.md` `11.496`
- `docs/compatibility/cluster-wait-migrate-failover-design-2026-03-04.md`
- `docs/performance/design-command-execution-scopes-2026-03-12.md`
- `docs/performance/design-typed-mutation-oplog-and-durability-ledger-2026-03-12.md`

## 1. Executive Summary

The current codebase has live control-plane facts, but they are split across unrelated layers:

- cluster routing reads `ClusterConfigStore` in the connection layer
- some `CLUSTER` subcommands are answered directly from a live snapshot in `connection_handler.rs`
- `server_commands.rs` still has static/stubbed `CLUSTER`, `READONLY`, `READWRITE`, and `FAILOVER`
- `WAIT` is implemented in the connection loop while the request handler still returns stubbed `:0`
- replication ACK state lives in `redis_replication.rs`
- local AOF durability has only offline primitives in `AofWriter`

That is not one control plane.

Greenfield rule:

- routing, `CLUSTER *`, `READONLY/READWRITE`, `FAILOVER`, `WAIT`, and `WAITAOF` must all read from one authoritative control-plane view

This does not require one giant lock. It requires one published snapshot source.

## 2. Current Baseline

### 2.1 Current split behavior

Today:

- `handle_wait` in `server_commands.rs` validates args and returns `:0`
- `handle_waitaof` is stubbed
- `handle_readonly`, `handle_readwrite`, and `handle_failover` return `ClusterSupportDisabled`
- `handle_cluster` still contains static text responses for `INFO`, `NODES`, `SLOTS`, and `SHARDS`
- `connection_handler.rs` intercepts `WAIT` and calls `replication.wait_for_replicas(...)`
- `connection_handler.rs` also intercepts some `CLUSTER` subcommands through `try_handle_live_cluster_snapshot_command(...)`
- `connection_routing.rs` accepts a `read_only_mode` parameter and currently ignores it

So command behavior depends on where in the stack the request happens to be handled.

### 2.2 Live state already exists in pieces

Pieces that already exist:

- topology snapshot source: `ClusterConfigStore`
- replication frontiers: `master_repl_offset`, per-replica `downstream_ack_offsets`
- write publication path: `publish_write_frame(...)`
- AOF primitive offsets and `sync_all()` in `AofWriter`

The problem is not lack of all primitives. The problem is lack of one shared read model.

## 3. Problem Statement

There are four structural problems.

### 3.1 Command layer and connection layer disagree

The same command surface is partly implemented:

- in request handlers
- in connection pre-dispatch logic

That causes:

- duplicated parsing and semantics
- stub command handlers that lie about actual behavior
- no single source of truth for tests or future changes

### 3.2 Topology, replication, and durability are not one view

`CLUSTER`, `WAIT`, `WAITAOF`, and `FAILOVER` all need different parts of the same operational truth:

- who owns which slots
- local node role
- replica link/ack state
- local fsync frontier
- failover eligibility/progress

Those facts should be published together as one coherent read model.

### 3.3 Per-connection mode is not integrated with topology state

`ASKING`, `READONLY`, and `READWRITE` are per-connection/session state, but their meaning depends on topology.

Example:

- `READONLY` is not a global switch
- it is a connection policy evaluated against the current cluster snapshot

Today there is no explicit boundary that combines:

- live topology snapshot
- connection-local routing mode

### 3.4 Derived text/status surfaces are treated as primary logic

Current static payloads for `CLUSTER INFO` / `NODES` / `SLOTS` / `SHARDS` are renderers pretending to be authoritative state.

The right order is:

1. authoritative snapshot
2. derived rendering

not the reverse.

## 4. Goals

The redesign should:

1. provide one authoritative control-plane read model
2. make routing and command handlers consume the same snapshot
3. integrate per-connection routing mode cleanly
4. define authoritative versus derived fields explicitly
5. support `WAIT`, `WAITAOF`, `READONLY/READWRITE`, `CLUSTER *`, and `FAILOVER`
6. remain compatible with staged implementation, not only a full rewrite

## 5. Non-Goals

This document does not define:

- final DB runtime layout
- final mutation op-log record encoding
- full `MIGRATE` command design

It only defines the shared control-plane ownership and API boundary.

## 6. Design Principles

### 6.1 One published snapshot, many writers

The project does not need one monolithic mutex.

It needs:

- a control-plane runtime that gathers authoritative state
- an immutable published snapshot used for reads

Writers may live in separate subsystems:

- cluster manager
- replication manager
- durability/AOF manager
- failover coordinator

### 6.2 Authoritative state and rendering are different

Examples:

- authoritative: `slot -> owner`, `local role`, `replica ack offset`
- derived: `CLUSTER NODES` text line, `MOVED` string, `INFO replication` payload

### 6.3 Connection-local policy overlays the snapshot

Per-connection state includes:

- `selected_db`
- `asking_once`
- `cluster_read_only_mode`

These are not global control-plane facts, but they are interpreted against the snapshot.

### 6.4 Global commands use the same view as routing

A `CLUSTER NODES` response and a `MOVED` decision must be computed from the same snapshot version.

That is the core consistency requirement.

## 7. Proposed Object Model

### 7.1 Runtime and snapshot

Greenfield target:

```rust
struct ControlPlaneRuntime {
    topology: TopologyRuntime,
    replication: ReplicationRuntime,
    durability: DurabilityRuntime,
    failover: FailoverRuntime,
    published: ArcSwap<ControlPlaneSnapshot>,
}

struct ControlPlaneSnapshot {
    version: u64,
    topology: TopologySnapshot,
    replication: ReplicationSnapshot,
    durability: DurabilitySnapshot,
    failover: FailoverSnapshot,
}
```

Interpretation:

- authoritative subsystem state is folded into one immutable read snapshot
- readers consume one versioned view

### 7.2 Connection-local overlay

```rust
struct ConnectionRoutingMode {
    selected_db: DbName,
    asking_once: bool,
    read_only: bool,
}

struct ControlPlaneView<'a> {
    snapshot: Arc<ControlPlaneSnapshot>,
    connection_mode: Option<&'a mut ConnectionRoutingMode>,
}
```

This lets the same command/routing code see:

- cluster topology
- replication/durability frontiers
- session routing mode

## 8. Authoritative Versus Derived Fields

### 8.1 Topology authoritative fields

Must be authoritative:

- local node id
- local role
- worker/node table
- slot ownership and migration state
- cluster epoch/config version

Derived from them:

- `CLUSTER INFO`
- `CLUSTER MYID`
- `CLUSTER NODES`
- `CLUSTER SLOTS`
- `CLUSTER SHARDS`
- `MOVED` / `ASK` error strings

### 8.2 Replication authoritative fields

Must be authoritative:

- applied write frontier
- published replication frontier
- per-replica ack frontier
- replica link status
- replica identity/endpoint

Derived from them:

- `WAIT`
- `ROLE`
- replication section of `INFO`
- failover eligibility ranking

### 8.3 Durability authoritative fields

Must be authoritative:

- locally appended frontier
- locally fsynced frontier
- per-replica durable frontier
- append-only mode/policy

Derived from them:

- `WAITAOF`
- AOF status in `INFO persistence`

### 8.4 DB/runtime-derived fields

Some `CLUSTER` subcommands also need DB keyspace state:

- `COUNTKEYSINSLOT`
- `GETKEYSINSLOT`

These are not pure control-plane facts.

So the design rule is:

- slot ownership comes from `ControlPlaneSnapshot`
- slot key enumeration/count comes from `DbHandle` / key registry for the selected DB

That keeps topology authoritative without pretending the control plane owns all data-plane indexes.

## 9. Request-Path API

### 9.1 Routing API

```rust
enum RouteDecision {
    Local,
    Ask { target: NodeRoute },
    Moved { target: NodeRoute },
    CrossSlot,
    ReadOnlyViolation,
    ClusterDown,
}

fn route_command(
    snapshot: &ControlPlaneSnapshot,
    mode: &ConnectionRoutingMode,
    command: CommandId,
    keys: &[KeyRef<'_>],
) -> RouteDecision
```

This replaces ad-hoc routing helpers that separately read cluster config and ignore read-only mode.

### 9.2 Global command API

```rust
trait ControlPlaneCommand {
    fn execute(
        &self,
        request: &RequestEnvelope<'_>,
        view: &mut ControlPlaneView<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError>;
}
```

Commands handled through this boundary:

- `CLUSTER *`
- `READONLY`
- `READWRITE`
- `FAILOVER`
- `ROLE`
- `WAIT`
- `WAITAOF`

## 10. Command Semantics Under The New Model

### 10.1 `READONLY` / `READWRITE`

These become connection-mode transitions validated against the topology snapshot.

Rules:

- only meaningful in cluster mode
- read-only mode affects routing/write admission for that connection
- state is stored on the connection, not globally

### 10.2 `CLUSTER *`

All read-surface subcommands render from `ControlPlaneSnapshot`.

No static text payloads remain in request handlers.

### 10.3 `WAIT`

`WAIT` should stop being a connection-loop special case.

Instead:

- the connection tracks its last write frontier
- `WAIT` reads replication frontiers from `ControlPlaneSnapshot` or waits on the replication runtime
- command semantics live behind one control-plane API

### 10.4 `WAITAOF`

`WAITAOF` uses the same model, but consults durability frontiers instead of only ACK frontiers.

This depends on integrating live AOF append/fsync frontiers into the control-plane runtime.

### 10.5 `FAILOVER`

`FAILOVER` is a global control-plane mutation command.

It should:

- inspect topology and replication snapshots
- compute or validate a failover plan
- mutate the runtime
- publish a new snapshot version

## 11. Snapshot Publication And Consistency

### 11.1 Publication rule

Any mutation that changes externally visible control-plane behavior must publish a new snapshot version.

Examples:

- slot ownership changes
- local role changes
- replica ack frontier changes that matter for `WAIT`
- fsync frontier changes that matter for `WAITAOF`
- connection-mode transitions do not publish globally, because they are session-local

### 11.2 Consistency rule

One request should make routing and command decisions against one snapshot version.

That avoids:

- routing based on one topology view
- `CLUSTER NODES` rendering from another

in the same command path.

## 12. Background Coordinators

Background coordinators should update the control-plane runtime, not hand-roll side channels.

Examples:

- gossip/cluster manager updates topology runtime
- replication manager updates replica liveness and ack frontiers
- durability manager updates append/fsync frontiers
- failover controller updates topology/role state

All of them then publish a unified snapshot.

## 13. Migration Plan From Current Architecture

### Stage 1: introduce `ControlPlaneSnapshot`

- build a snapshot type from current `ClusterConfigStore` plus replication ledger state
- keep existing writers, but publish one read object

### Stage 2: move `CLUSTER` live-read handling behind control-plane API

- remove `try_handle_live_cluster_snapshot_command(...)`
- remove static `CLUSTER INFO/NODES/SLOTS/SHARDS` payloads from `server_commands.rs`

### Stage 3: integrate `READONLY` / `READWRITE`

- add explicit connection-mode state
- make routing consult it
- stop ignoring `read_only_mode`

### Stage 4: move `WAIT` behind control-plane API

- remove connection-loop-only semantics
- keep per-connection last-write frontier but delegate waiting to control plane

### Stage 5: integrate durability frontiers

- add live AOF append/fsync frontiers
- implement `WAITAOF`

### Stage 6: move `FAILOVER` to the same boundary

- expose failover plan/execute through control-plane runtime and published snapshot

## 14. Expected Benefits

### 14.1 Correctness

- one source of truth for routing and `CLUSTER *`
- no stub command handlers that diverge from live behavior
- `READONLY` and `WAIT` semantics stop being connection-loop special cases

### 14.2 Code cleanliness

- connection code handles framing and session state
- command/control-plane code handles semantics
- rendering is clearly separated from authoritative state

### 14.3 Extensibility

- `FAILOVER`, `WAITAOF`, and future topology-aware commands have a natural home
- tests can target one control-plane API instead of scattered hooks

## 15. Decision

The project should introduce one versioned `ControlPlaneSnapshot` shared by:

- routing
- `CLUSTER *`
- `READONLY` / `READWRITE`
- `WAIT`
- `WAITAOF`
- `FAILOVER`

with per-connection routing mode layered on top.

That is the missing control-plane boundary for true Redis/Valkey compatibility.
