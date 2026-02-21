# Actor Lock Minimization Notes (2026-02-21)

## Source

- DeepResearch report (imported): `docs/performance/actor-lock-minimization-deepresearch-2026-02-21.md`
- Instruction used: `docs/performance/DEEPRESEARCH_ACTOR_LOCK_MINIMIZATION_INSTRUCTION_2026-02-21.md`

## Key Findings (Actionable)

- Default design should be `actor-only serialization` for mutating data paths.
- Shared locking should be removed from hot paths by making owner-thread the single writer per shard/key-space.
- Optimistic read (`version validate`) should be limited to read-mostly metadata/snapshots only.
- Optimistic path must always have a deterministic fallback path (retry or actor-routed locked path).
- Global coordination concerns (transaction boundary, replication ordering, migration boundary) should use explicit coordinator/barrier semantics instead of coarse data locks.
- Expiration should stay Redis-compatible with passive plus active model, but active work should execute per shard via owner-thread tick.

## Assessment

- The report quality is good for this project: recommendations are concrete, architecture-aligned, and include fallback/rollback thinking.
- Highest-value guidance is consistent with current direction: actor-only write serialization, narrow optimistic-read scope, and explicit coordinator/barrier boundaries for global ordering.

## Proposed Repository-level Rules

- Rule 1: New command implementations route state mutation through owner-thread actor first; no new global mutex in hot path.
- Rule 2: If shared read optimization is added, it must include `validate` metric and fallback counter.
- Rule 3: Apply optimistic-read only to immutable snapshot objects or read-mostly metadata, not pointer-chasing mutable structures.
- Rule 4: Any cross-shard ordering guarantee must be explicit in coordinator code path and test-covered.

## Candidate Work Items

- Migrate remaining object-store shared paths to owner-thread ownership model.
- Add fallback metrics for optimistic-read trials (`validate_fail`, `fallback_count`, `retry_count`).
- Add coordinator/barrier test scenarios for transaction and replication ordering boundaries.
- Rework expiration scheduler to shard-tick execution model if any global lock path remains.

## Benchmark/Verification Checklist

- Throughput and latency: compare before/after with `binary_ab_local.sh`.
- Correctness: keep `cargo test -p garnet-server` gate and transaction/replication integration scenarios.
- Profiling: confirm lock-wait share decreases and no message-passing hotspot explosion in framegraph/perf captures.
