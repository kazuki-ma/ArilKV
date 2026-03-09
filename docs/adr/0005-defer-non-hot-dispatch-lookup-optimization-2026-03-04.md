# ADR-0005: Defer Non-Hot Dispatch Lookup Rewrite (PHF/HashMap)

- Status: Accepted (Deferred)
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review references:
  - `.review_by_claude/01_PERFORMANCE_CRITICAL.md`
  - `.review_by_claude/13_CSHARP_COMPARISON.md`
  - `docs/performance/experiments/2026-02-21/12.31-command-catalog-sso/README.md`

## Context

Review suggests replacing linear non-hot command lookup with PHF/HashMap style
O(1) mapping.

Current dispatch already keeps hot-command fast path
(`GET/SET/DEL/INCR`) and previous command-catalog unification was measured as
near-neutral in local A/B runs.

## Decision

Defer PHF/HashMap rewrite for non-hot dispatch lookup.

Treat it as optimization backlog, not immediate implementation work.

## Consequences

- Current lookup simplicity is preserved.
- No additional table-generation complexity is introduced now.
- Potential non-hot lookup savings are postponed.

## Revisit Triggers

Reopen this decision when:

1. profiler evidence shows non-hot dispatch lookup as sustained hotspot
   (for example >5% CPU in representative production-like runs), and
2. a proposed rewrite includes benchmark plan and rollback criteria.

