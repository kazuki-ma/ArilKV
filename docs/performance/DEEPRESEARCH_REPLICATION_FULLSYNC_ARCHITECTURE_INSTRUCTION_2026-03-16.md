# DeepResearch Instruction (No Repo Access): Full-Sync Replication Architecture for a Redis-Compatible Server

## Important Constraints

- Assume you **cannot access this repository**.
- Build the answer from public sources only.
- Prioritize official/product sources first.
- For mechanism ideas that are not documented by product docs, you may use source code, conference talks/slides, engineering blog posts, and public papers, but classify each source as:
  - `official product doc`
  - `official source code`
  - `vendor engineering content`
  - `research / paper`
- We care more about a design that is correct and production-defensible than about the smallest implementation.

## Project Context (Provided Manually)

We are building a Redis-compatible server in Rust.

Current replication shape:

- We already support Redis-style `REPLICAOF`, `REPLCONF`, `SYNC`, and `PSYNC` command surfaces.
- Mutation replication today is command-frame based, with a single global replication stream and a single global replication offset.
- The current implementation can accept upstream replication and apply live command frames after `FULLRESYNC`, but the initial RDB payload is not yet fully ingested into local state.
- For downstream Redis replicas, we recently made the wire handshake succeed by returning a valid empty Redis RDB payload for real replicas, then relying on subsequent command propagation.
- For `redis-cli --rdb`-style callers we still have a separate debug-snapshot path selected explicitly by `REPLCONF rdb-only 1`.

Current architectural constraints:

- The server uses an owner-thread / actor-style execution model.
- There are multiple internal shards / owner threads, but the server is exported as one logical Redis-compatible instance unless explicitly operating in cluster mode.
- The replication path today has one externally visible master replication stream / offset space.
- We are willing to add per-session backlog or cutover state if that is the best design.
- We do not want to rely blindly on Redis's exact process model if a better fit exists for an actor-based Rust server.

Compatibility/correctness goals:

1. `Redis or Valkey primary -> Garnet replica` must support true attach-time dataset sync, not just post-attach command replication.
2. `Garnet primary -> Redis or Valkey replica` must support true attach-time dataset sync with no missed writes across the snapshot/live cutover.
3. The design must preserve Redis-compatible ordering and fail defensibly under backpressure or reconnect.

## Core Design Problem

We need the best architecture for **full synchronization** in both directions:

1. Upstream ingest:
   - Receive `FULLRESYNC` from a Redis/Valkey primary.
   - Ingest the RDB payload into local Garnet state.
   - Then continue applying the post-snapshot command stream.

2. Downstream export:
   - Serve `FULLRESYNC` / `SYNC` to Redis/Valkey replicas.
   - Produce an RDB snapshot that represents a coherent point-in-time dataset.
   - Then deliver all writes after that cutover with no gap and no duplicate semantic effects.

The critical correctness question is the cutover boundary:

- Which writes belong in the snapshot?
- Which writes belong in the post-snapshot command stream?
- How should this work when the server has multiple internal owner threads / shards?

## Research Goals

1. Find the best production-grade architecture for full-sync correctness in an actor-based Redis-compatible server.
2. Determine whether a single global cutover epoch is required for one exported instance, or whether some per-shard epoch split is ever safe.
3. Compare proven designs from modern Redis-family and adjacent systems, not just Redis OSS 1:1 imitation.
4. Produce an actionable decision matrix and phased implementation plan.

## Questions To Answer

1. What are the most credible production designs today for snapshot + live-stream cutover?
   - Compare at least:
     - Redis OSS replication / PSYNC / backlog / diskless replication
     - Valkey replication and any recent dual-channel or parallel replication work
     - Redis Software / Redis Enterprise gradual sync or per-shard backlog approaches if relevant
     - DragonflyDB or other modern Redis-compatible engines if they expose relevant design ideas
     - RedisShake or adjacent migration/snapshot streaming tools if they offer useful cutover patterns

2. For a server exported as **one logical Redis instance**, is a **single global cutover** effectively required?
   - Can internal shards take their local snapshot materialization at different wall-clock times as long as they are pinned to one logical cutover?
   - When, if ever, is it safe to let shards have independent cutover epochs?
   - Does that only become safe when each shard is exported as an independently replicated partition?

3. What is the best design for downstream export in this architecture?
   - Global cutover + per-session backlog replay
   - Shared backlog ring indexed by replication offset
   - Dual-channel replication
   - Snapshot barrier / epoch pinning
   - Fork/CoW-like snapshotting
   - Copy-on-write or immutable snapshot views at the shard level
   - Any better current design you believe beats these

4. What is the best design for upstream ingest?
   - Apply a complete RDB into quiesced local state, then switch to command stream
   - Stream-apply RDB while buffering post-cutover commands
   - Stage into a temporary snapshot structure, then atomically publish
   - Any better current design you believe beats these

5. How should command ordering and atomicity be preserved?
   - Multi-key commands
   - `MULTI` / `EXEC`
   - Lua / function-generated replication effects
   - deferred replication rewrites
   - synthetic replication frames such as expiry-driven `DEL`
   - `SELECT` / multi-DB behavior

6. What backlog design is best for the export side?
   - per-replica buffer
   - shared replication ring
   - AOF-backed replay window
   - dual log / dual channel
   - memory/durability tradeoffs
   - explicit rollback / disconnect behavior when backlog is exhausted mid-sync

7. What are the main failure modes and mitigations?
   - snapshot built too early or too late
   - writes missed during snapshot generation
   - writes duplicated across snapshot and suffix stream
   - cross-shard command split between snapshot and suffix
   - replica disconnect during backlog drain
   - memory blow-up from per-session buffering
   - interaction with cluster mode / per-primary ownership

8. What should the validation strategy be?
   - exact correctness tests
   - targeted interoperability matrix
   - performance benchmarks
   - chaos/failure injection
   - formal invariants worth modeling

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary`
   - no more than 12 bullets
2. `System Comparison Matrix`
   - System / mechanism
   - snapshot strategy
   - cutover strategy
   - backlog strategy
   - sharding story
   - strengths
   - weaknesses
   - source classification
3. `Recommended Architecture`
   - one primary recommendation
   - one fallback recommendation
   - why
4. `Cutover Invariants`
   - explicit invariants we must preserve
5. `Shard/Epoch Analysis`
   - answer whether per-shard epochs are safe or unsafe, and under what exported topology
6. `Downstream Export Design`
   - data structures
   - sequencing
   - reconnect behavior
7. `Upstream Ingest Design`
   - sequencing
   - staging/publish model
   - buffering model
8. `Phased Delivery Plan`
   - Phase 1: minimum correct production slice
   - Phase 2: performance hardening
   - Phase 3: advanced optimizations
9. `Risk Register`
   - top risks and explicit mitigations
10. `Validation Plan`
   - correctness
   - interop
   - performance
   - formal methods
11. `References`
   - include URLs and source classification

## Specific Source Requests

Please explicitly inspect and use, if relevant:

- Redis replication / PSYNC documentation and source behavior
- Valkey replication material, including public 2025 dual-channel replication material if available
- Redis Software / Redis Enterprise docs that mention per-shard backlog or gradual sync behavior
- RedisShake architecture docs if snapshot + incremental catch-up patterns are useful
- Public papers or arXiv material on asynchronous snapshotting, online cutover, replicated-log snapshot installation, or related mechanisms

## Decision Criteria We Will Apply

- Correctness at the snapshot/live boundary is the primary criterion.
- Designs that fit an actor/owner-thread server are preferred over ones that only fit fork-based process architectures.
- One exported Redis-compatible instance should have one coherent replication story; avoid clever per-shard schemes unless they are rigorously justified.
- We want a recommendation we can actually implement and verify in a Rust codebase, not just a theoretical ideal.
