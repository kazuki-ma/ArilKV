# DeepResearch Instruction (No Repo Access): Formal Methods and SOTA Literature for Replication Snapshot Cutover

## Important Constraints

- Assume you **cannot access this repository**.
- Build the answer from public sources only.
- We explicitly want a literature and formal-methods oriented answer.
- Product docs and source code are welcome, but also search public papers, arXiv, conference talks, and publicly accessible engineering writeups.
- Classify every source as:
  - `official product doc`
  - `official source code`
  - `vendor engineering content`
  - `research / paper`

## Project Context (Provided Manually)

We are building a Redis-compatible server in Rust with an owner-thread / actor execution model.

We need to implement correct full-sync replication:

- ingest an upstream RDB snapshot, then continue with the live command stream
- export a downstream RDB snapshot, then continue with the live command stream

The key correctness problem is the snapshot/live cutover boundary:

- no writes may be lost
- no writes may be duplicated semantically
- writes that represent one logical command must not be split inconsistently across snapshot and suffix replay
- cross-shard internal execution must still appear as one coherent external dataset stream when the server is exported as one logical Redis instance

We already use TLA+ in this repository for critical concurrency and command-path bugs, and we are open to adding a new model for replication cutover.

## Research Goals

1. Find the best current formalization patterns for snapshot/live cutover correctness.
2. Find relevant system-design literature and production prior art that can inform the model and implementation.
3. Produce a concrete TLA+ modeling recommendation we can implement locally.

## Questions To Answer

1. What are the most relevant production and research precedents for this problem?
   - Redis / Valkey full sync and backlog semantics
   - dual-channel replication ideas
   - snapshot barrier / watermark techniques from streaming systems
   - replicated-log snapshot installation patterns
   - incremental snapshot + change-log cutover techniques from CDC systems
   - any modern papers or public implementations that cleanly address "snapshot plus suffix with no gap / no duplicate"

2. What are the right **state-machine invariants** for this replication problem?
   - no gap
   - no duplicate
   - snapshot then suffix ordering
   - atomic command visibility
   - cross-shard command indivisibility
   - live-stream handoff correctness
   - reconnect / backlog exhaustion behavior

3. Is the following design statement correct or incorrect, and why?
   - "For one exported logical Redis instance, a single global logical cutover is required, but shards may materialize their local snapshot views at different wall-clock times if they are pinned to that same logical cutover."

4. Under what conditions are per-shard independent epochs safe?
   - only if each shard is exported as an independent replication partition?
   - ever safe for a single externally visible replication stream?

5. What is the minimal but sufficient TLA+ model shape for this problem?
   - state variables
   - actions
   - refinement boundary
   - useful liveness and safety properties
   - counterexample scenarios worth checking first

6. What specific bug patterns should the model be expected to catch?
   - snapshot built before backlog capture starts
   - backlog replay begins from wrong offset
   - cross-shard command split between snapshot and suffix
   - duplicate replay after reconnect
   - live switch occurs before backlog drain completes
   - stale cutover metadata reused

7. What validation complement should exist beyond TLA+?
   - exact integration tests
   - randomized schedule tests
   - crash/reconnect simulation
   - differential interop tests

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary`
   - no more than 10 bullets
2. `Prior Art Map`
   - mechanism / system / paper
   - why it is relevant
   - source classification
3. `Recommended Correctness Model`
   - the minimal conceptual model we should adopt
4. `Invariants`
   - explicit safety invariants
   - explicit liveness properties
5. `Shard/Epoch Judgment`
   - answer whether the shared-global-cutover / per-shard-materialization statement is sound
   - answer when independent per-shard epochs are safe or unsafe
6. `TLA+ Modeling Blueprint`
   - variables
   - actions
   - properties
   - likely counterexamples
7. `Test Strategy Complement`
   - what runtime tests should exist in addition to the model
8. `References`
   - URLs and source classification

## Specific Search Instructions

- Search public papers and arXiv for snapshot consistency, asynchronous snapshotting, online backup/cutover, incremental snapshot + change-log replay, and barrier/watermark synchronization.
- Search for public material on Valkey dual-channel replication and similar mechanisms.
- If a result is insightful but not directly about Redis, include it only if you explain the transfer clearly.
- Distinguish carefully between:
  - ideas proven in production systems
  - ideas from research literature
  - ideas that are plausible but not well-proven

## Decision Criteria We Will Apply

- We want the simplest model that still catches the real failure modes.
- Formal guidance must map cleanly to implementable Rust/TLA+ work, not just abstract theory.
- A source that clearly informs cutover correctness is more valuable than a loosely related distributed-systems survey.
