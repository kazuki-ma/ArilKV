# DeepResearch Instruction (No Repo Access): Kernel-Path Reduction for GET/SET Hot Path

## Important Constraints

- Assume you **cannot access this repository**.
- Build your answer only from public primary/official sources and production-proven engineering references.
- Focus on Linux-first guidance (x86_64 + ARM64), with notes when macOS behavior differs.

## Project Context (Provided Manually)

We are implementing a Redis-compatible in-memory server in Rust (`garnet-rs`).

Current direction:

- Owner-thread architecture (`1 owner thread` per logical node/port path).
- We care about high-throughput small-value `GET/SET` workloads.
- We are intentionally avoiding large complexity spikes unless gain is clear.
- We need maintainable production code; benchmark-only hacks are not acceptable.

Recent profiling (single-thread / single-core style runs) indicates:

- Kernel/network syscall path is large (`sendto` / `recvfrom` are major leaf hotspots).
- Wakeup/scheduling/handoff overhead is also large.
- Parser overhead is small relative to kernel + wakeup cost.

Working hypothesis from the team:

- “Maybe a lot of this can be removed by mode/options, not massive rewrites.”
- We want to test that rigorously before touching core implementation.

## Research Goals

1. Determine whether kernel-path cost can be significantly reduced with:
   - socket/kernel tunables only,
   - moderate I/O model changes,
   - kernel-bypass options.
2. Rank options by:
   - expected throughput and p99 impact,
   - implementation complexity,
   - operational burden,
   - portability and maintenance risk.
3. Produce an execution plan that starts with low-risk/high-confidence steps.

## Questions To Answer

1. For Redis-like tiny-request/tiny-response workloads, what are the most effective techniques to reduce kernel overhead?
2. Which techniques are realistic as “option-level” changes (minimal code churn)?
3. Which techniques require architectural changes (event loop model, batching, completion APIs)?
4. What is the practical benefit/risk envelope of:
   - `TCP_NODELAY`, socket buffer sizing, busy-poll options,
   - syscall reduction via read/write batching,
   - `epoll` vs `io_uring`,
   - `SO_REUSEPORT` listener sharding,
   - `MSG_ZEROCOPY` (and when it hurts),
   - AF_XDP / DPDK / other kernel-bypass paths?
5. Can “blocking I/O only” be safe/fast in this workload class, and under what strict conditions?
6. What causes head-of-line blocking in practice even with small requests, and what mitigations are proven?
7. What is the recommended measurement stack to separate:
   - user-space compute time,
   - syscall cost,
   - scheduler/wakeup cost,
   - NIC/driver bottlenecks?

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary` (10 bullets max)
2. `Technique Matrix` table:
   - Technique
   - Expected gain (throughput/p99 range, if known)
   - Prerequisites
   - Complexity
   - Ops risk
   - Portability
   - “Adopt now / later / avoid”
3. `Phase Plan`:
   - Phase 0: option-only experiments
   - Phase 1: minimal code changes
   - Phase 2: moderate architecture updates
   - Phase 3: bypass track (only if justified)
4. `Measurement Protocol`:
   - concrete command-level profiling checklist
   - what counters/metrics to collect and compare
   - pass/fail criteria for each phase
5. `Failure Modes`:
   - what commonly regresses p99 or stability
   - how to detect each quickly
6. `References`:
   - primary sources only (official docs, papers, kernel docs, production engineering posts)

## Decision Criteria We Will Apply

- We prefer steps with the highest “gain / complexity” ratio first.
- If a proposal increases complexity significantly, it must show clear evidence of material wins.
- If a proposal harms portability or operability, we need an explicit mitigation plan.
- Recommendations must be benchmark- and observability-driven, not anecdotal.

