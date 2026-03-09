# ADR-0002: Keep Owner-Thread Shard Serialization and Ordered Locking

- Status: Accepted
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review references:
  - `.review_by_claude/00_EXECUTIVE_SUMMARY.md`
  - `.review_by_claude/06_CONCURRENCY.md`

## Context

The review flags per-shard serialization and sequential lock scopes as
throughput limits under contention.

Current architecture intentionally uses:

- owner-thread style shard-affine mutation paths
- deterministic lock ordering via `OrderedMutex`
- TLA+-annotated critical sections in blocking/connection paths

## Decision

Keep the current owner-thread shard serialization model and `OrderedMutex`
ordering invariants as the baseline concurrency contract.

Do not introduce broad shared-read/write redesign in this iteration.

## Consequences

- We keep deterministic concurrency behavior and current safety invariants.
- Some throughput opportunities are intentionally left on the table for now.
- New optimization work should avoid invalidating modeled/annotated concurrency
  behavior without a dedicated design phase.

## Revisit Triggers

Reopen this decision when:

1. a concrete proposal can preserve or re-prove critical path invariants
   (including modeled paths), and
2. benchmark evidence shows current serialization as dominant bottleneck in
   representative workloads.

