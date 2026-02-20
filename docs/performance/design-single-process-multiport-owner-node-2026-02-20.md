# Design Doc: Single-Process Multi-Port Owner-Node Architecture

Date: 2026-02-20  
Status: Draft (for implementation planning)  
Author: Codex + user discussion

## 1. Problem Statement

Current optimization work reduced lock contention, but hot-path command execution still pays:

- lock/unlock overhead even when wait is low
- `Arc` atomic refcount overhead on shared state paths
- residual shared-state coupling through one logical server instance

Target direction is stronger: make the architecture itself enforce that global locks are unnecessary on the data path.

## 2. Core Hypothesis

If we expose each owner shard as an independent Redis cluster node endpoint:

- `1 owner thread = 1 listen port = 1 logical node`
- clients see multiple cluster nodes (same host, different ports)
- single-key operations stay node-local
- cross-node operations rely on cluster semantics (`MOVED`/`CROSSSLOT`) instead of in-process atomicity

Then we can remove global lock requirements from request execution by construction.

## 3. High-Level Proposal

Preferred first implementation:

- **single process, multi port, multi node-in-process**
- each port has:
  - dedicated request processor instance
  - dedicated expiration/background loop
  - dedicated metrics object
  - dedicated cluster local-worker identity

Recommended strictness level:

- run **one Tokio current-thread runtime per port thread** (not one shared multi-thread runtime)
- this preserves a clear ownership model for “node-local mutable state”

This keeps operational simplicity (one OS process) while giving near process-level isolation for data path state.

## 4. Why This Can Remove Global Locks

Under this model, data-path state is per-node:

- key/value store
- expiration maps/counters
- key registries
- watch version map
- transaction queue state

Cross-node consistency is delegated to cluster protocol behavior, not shared-memory atomic sections.

Important framing:

- this does **not** guarantee multi-key cross-node atomicity (same as distributed cluster semantics)
- that is acceptable and explicit in Redis Cluster behavior

## 5. Concerns and Reality Check

The concern is not zero. It is manageable if explicitly designed.

### 5.1 Background Tasks

Current server spawns periodic expiration tasks.  
If each port/node gets its own processor and task loop, no global data lock is required for expiration.

Risk:

- accidental shared processor wiring across ports would reintroduce contention.

Mitigation:

- hard type boundary: each node context owns its processor/task handles.

### 5.2 Durability / AOF / Checkpoint

If all nodes append to one shared persistence target, a global serialization point appears.

Mitigation options:

- per-node AOF/checkpoint files (strong isolation, preferred first)
- later optional merge/compaction off data path

### 5.3 Metrics and Admin Plane

Global aggregate metrics can reintroduce synchronization hotspots.

Mitigation:

- keep per-node metrics first
- aggregate lazily (read-time sum), not write-time global lock

### 5.4 Cluster Metadata

If one global mutable cluster-config object is shared across all node contexts, update paths may contend.

Mitigation:

- per-node local config snapshot + message-based updates
- no data-path lock dependency on config mutation

## 6. Single-Process vs Multi-Process

### Single-Process Multi-Port (first target)

Pros:

- better operator acceptance
- simpler deployment
- lower inter-process operational complexity

Cons:

- process-level resources still shared (allocator, file descriptors, logging, scheduler pressure)
- isolation bugs are easier to accidentally introduce

### Multi-Process (control/validation path)

Pros:

- strongest isolation boundary
- easiest way to prove “no shared-memory global lock on data path”

Cons:

- more deployment surface
- orchestration complexity

Decision:

- implement single-process multi-port first
- keep multi-process experiment as control baseline

## 7. Formal Invariants (Target)

For single-key command execution path:

1. request accepted on port `P` is executed only against node context `N(P)`
2. command path does not lock mutable state outside `N(P)`
3. cross-node key access is redirected/protocol-rejected, never synchronized via global lock

For implementation reviews, this should be checked as a hard condition, not a best effort.

## 8. Phased Implementation Plan

### Phase 0: Control Benchmark (no code redesign)

Purpose:

- measure how much gain can come from deployment topology alone

Method:

- run current server as independent cluster nodes (multi-process control)
- compare against single endpoint baseline

Outcome:

- establishes upper/lower bound before structural rewrite

### Phase 1: Single-Process Multi-Port Node Contexts

Deliverables:

- multi-port config (`GARNET_BIND_ADDRS` style or equivalent)
- per-port `NodeServerContext`
- per-port listener + per-port runtime thread
- explicit slot ownership map per port

### Phase 2: Shared-State Elimination on Node Data Path

Deliverables:

- no cross-node shared mutex on GET/SET path
- no global mutable registries touched in hot path

### Phase 3: Optional Node-Local Type Narrowing

Only after invariants hold and benchmarks justify complexity:

- evaluate `Arc` -> `Rc`/owned references in node-local single-thread contexts
- evaluate lock-free/local-only structures replacing mutex wrappers where valid

## 9. Experiment Plan and Success Criteria

Primary workloads:

- GET-only and SET-only
- mixed read/write
- pipeline 1 and 32
- threads/connections matrix already used in repo benchmarks

Success criteria:

1. throughput improvement with stable or better p99
2. hotspot reduction in lock symbols (`__psynch_mutexwait` etc.)
3. no regression in cluster correctness tests (`MOVED`, `CROSSSLOT`, transaction behavior)

## 10. Risks

1. hidden shared state sneaks back through “utility singletons”
2. durability path becomes new bottleneck
3. scheduler imbalance across port threads
4. memory overhead from N independent node contexts

## 11. Testing Requirements

Must include:

1. deterministic tests for slot->port ownership enforcement
2. concurrency tests proving no cross-node mutable path access
3. compatibility tests for redis-cli and cluster redirection flows
4. benchmark integrity checks (already enforcing connection/server-error fail-fast)

## 12. Open Questions

1. Should initial phase support only cluster-aware clients, or include optional proxy mode?
2. Should node contexts share object-store features initially, or start with string-only fast path?
3. Which persistence mode is first: per-node file sets or disabled durability for pure perf validation?

## 13. Decision Summary

- The design is **feasible** and has clear performance upside.
- “No major concern” is too optimistic; there are real concerns, but they are **designable**.
- Background tasks are not a blocker if they are strictly node-localized.
- Proceed with single-process multi-port as first implementation target, with explicit invariants and staged validation.
