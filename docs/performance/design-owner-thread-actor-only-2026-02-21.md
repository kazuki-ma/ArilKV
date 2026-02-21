# Design Doc: Owner-Thread Actor-Only Execution

Date: 2026-02-21  
Status: Draft (active implementation)  
Author: Codex + user discussion

## 1. Objective

Make command execution semantics explicit and uniform:

- all data-path command execution must run on an owner-thread actor
- direct `RequestProcessor::execute` calls from async connection/background tasks are disallowed
- simplification and correctness clarity are prioritized over temporary resource efficiency

## 2. Motivation

Current code mixes two models:

- actor-routed execution for a subset of commands
- direct shared `RequestProcessor` execution for many other paths

This mixed model keeps lock behavior and ownership reasoning unclear.  
We want one execution rule first, then optimize internals under that rule.

## 3. Decision

Adopt **Actor-Only execution policy** for runtime data path:

1. connection command execution: always owner-thread
2. `MULTI/EXEC` replay: always owner-thread per queued frame
3. upstream replication apply: always owner-thread
4. expiration background work: scheduled via owner-thread

Routing policy:

- key-based commands: route by key shard ownership
- keyless commands: route to shard `0`
- multi-key commands: route by first key (current behavior envelope), with later tightening if needed

## 4. Non-Goals (This Step)

- removing `OrderedMutex` immediately
- replacing `Arc` with `Rc`/owned references
- lock-free object store redesign
- changing external Redis compatibility semantics

Those are follow-up optimization stages after actor-only invariants are enforced.

## 5. Invariants

After this step:

1. no live connection path executes command logic outside owner-thread pool
2. no replication apply path mutates store outside owner-thread pool
3. expiration mutations happen on owner-thread pool
4. any fallback direct execution path is treated as a bug

## 6. Risks

1. Throughput regression from extra queue hops
2. Hotspot concentration on shard `0` for keyless commands
3. Hidden bypass paths in utility/replay code

Mitigation:

- add tests that assert routing behavior and command-path parity
- keep benchmark reports per step and compare before/after
- isolate bypasses as explicit TODO items

## 7. Implementation Plan

1. Introduce universal owner-shard selection helper
2. Replace direct connection-path execute fallback with mandatory routing
3. Route `EXEC` queue replay through owner-thread
4. Route upstream replication command apply through owner-thread
5. Route periodic expiration work through owner-thread
6. Keep correctness tests green before lock-elimination work

## 8. Exit Criteria

This design phase is complete when:

- runtime code has no direct `processor.execute(..)` on connection/replication/expiration paths
- integration tests pass
- benchmark baseline is rerun and recorded
