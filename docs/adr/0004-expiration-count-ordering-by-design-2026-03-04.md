# ADR-0004: Keep Relaxed Expiration-Count Ordering Semantics

- Status: Accepted
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review reference:
  - `.review_by_claude/06_CONCURRENCY.md` (expiration count ordering analysis)

## Context

Per-shard expiration counters are used as fast-path hints to decide whether an
expiration scan may be needed.

The review notes the current asymmetric ordering (`Acquire` load, `Release`
updates) and evaluates it as intentional for hint semantics.

## Decision

Keep current atomic ordering for expiration-count hints.

Do not strengthen ordering or convert to strict correctness synchronization
state in this iteration.

## Consequences

- Fast-path hint checks remain lightweight.
- Counter remains advisory, not authoritative source of expiration truth.
- Avoids unnecessary synchronization cost on high-frequency paths.

## Revisit Triggers

Reopen this decision when:

1. correctness incidents are traced to this counter ordering, or
2. expiration scheduling architecture changes require stronger semantics.

