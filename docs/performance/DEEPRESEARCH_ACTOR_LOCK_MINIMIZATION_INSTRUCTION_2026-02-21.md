# DeepResearch Instruction (No Repo Access): Actor Model + Minimal Locking

Date: 2026-02-21  
Target system: Redis-compatible Rust server implementation (Garnet-RS server path)  
Constraint: **You cannot read repository code or local design docs.**  
You must work only from the system description below plus external sources.

## 1. Mission

Produce a practical recommendation report for a Redis-like server that is moving to an actor/owner-thread model, with the next objective:

- avoid coarse locks on hot path
- do non-blocking/optimistic validation first
- take lock only when strictly necessary and in minimal scope

Goal is implementable engineering guidance, not theory-only discussion.

## 2. System Description (Self-Contained)

Assume this architecture:

1. **Protocol/connection layer**
   - TCP server accepts many client connections.
   - RESP commands are parsed per connection.
   - Commands are dispatched to execution logic.

2. **Owner-thread actor routing**
   - There is a shard-owner thread pool.
   - Commands are routed to an owner thread (mostly key-based).
   - Keyless commands currently route to shard `0`.

3. **Core execution object**
   - A shared `RequestProcessor`-like component contains data structures and command handlers.
   - String path is sharded.
   - Object path exists as a single shared store.

4. **Current synchronization style**
   - Mutex wrappers (`OrderedMutex`-like) protect:
     - sharded string stores
     - object store
     - expiration metadata
     - key registries / metadata maps
   - Actor routing exists, but lock-based internal structures still remain.

5. **Background and side paths**
   - Periodic expiration task runs on timer (e.g., ~50ms interval).
   - Replication path receives upstream command stream and applies writes.
   - Transaction path (`MULTI/EXEC` style) replays queued frames.

6. **Current design direction**
   - Unify runtime mutation paths through owner-thread actors.
   - Keep Redis compatibility semantics.
   - Then reduce lock scope/frequency inside actor-driven execution.

## 3. What We Need From You

Given the architecture above, research and propose how to minimize locking overhead **without sacrificing correctness**.

Focus on:

- realistic production-proven patterns
- applicability to Redis-like command workloads
- migration safety and testability

## 4. Research Questions

1. What production-proven patterns exist for “optimistic/non-blocking fast path + fallback lock” in high-throughput KV/data servers?
2. Which techniques from modern JVM/Java concurrency ecosystem are relevant?
3. Which lessons from recent GC/concurrency work (HotSpot/ZGC/Shenandoah, safepoint/handshake/barrier ideas) are transferable to this Rust actor KV setting, and which are not?
4. How should we evaluate and combine:
   - versioned validation (seqlock-style/version counters)
   - optimistic read + retry
   - lock striping / key-range narrowing
   - RCU/epoch-like publication
   - actor-local state promotion
5. What operations should likely remain lock-protected for correctness (expiration, type transitions, replication ordering, migration/transaction boundaries)?

## 5. Required Comparisons

At minimum compare:

- coarse mutex vs shard mutex vs key-range lock vs optimistic versioned retry
- actor-only serialization vs actor + optimistic shared reads
- lock-free/RCU-like publication vs simple mutex paths
- Java `StampedLock` optimistic-read model vs Rust-equivalent designs
- GC/safepoint/barrier design lessons: transferable vs non-transferable

## 6. Deliverables

Return report in this exact order:

1. Executive summary (max 10 bullets)
2. Evidence table (claim -> source -> confidence)
3. Technique matrix (principle, correctness model, perf expectation, complexity, failure modes, fit score)
4. Concrete target architecture for this system
5. 3-phase migration plan:
   - Phase A (days, low risk)
   - Phase B (1-2 weeks, medium risk)
   - Phase C (advanced/optional)
6. Benchmark + flamegraph plan per phase
7. Risk register + rollback criteria
8. “Do first this week” checklist (max 12 items)

## 7. Quality Bar

- Must be actionable; avoid generic advice.
- Separate “proven production practice” from “research/experimental.”
- State assumptions explicitly (because code is not accessible).
- Prefer simpler designs when gains are similar.
- Include compatibility risk notes for Redis semantics.

## 8. Important Constraints

- You cannot inspect code, so do not request repository browsing as a prerequisite.
- If recommending a technique that depends on unknown implementation details, mark it as conditional and provide fallback options.
- Recommendations should still be ticket-ready using component-level descriptions from Section 2.
