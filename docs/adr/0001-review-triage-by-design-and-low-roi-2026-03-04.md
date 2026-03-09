# ADR-0001: Review Triage (By-Design / Low-ROI) from 2026-03-04 Audit

- Status: Superseded
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Source review set: `.review_by_claude/*.md`

Superseded by:

- ADR-0002
- ADR-0003
- ADR-0004
- ADR-0005
- ADR-0006
- ADR-0007

## Context

The 2026-03-04 review batch identified many potential improvements across
performance, readability, data structures, and concurrency.

Some findings should not be treated as immediate implementation tasks because:

1. they are explicit design choices aligned with current architecture goals, or
2. implementation complexity/risk is high relative to measured benefit.

This ADR records those decisions so future reviews do not repeatedly reopen the
same items without new evidence.

## Decision

This umbrella ADR was split into one-record-per-decision:

- ADR-0002: Keep owner-thread shard serialization and `OrderedMutex` model
- ADR-0003: Keep inline expiration prefix format for string values
- ADR-0004: Keep relaxed Acquire/Release ordering for expiration-count hints
- ADR-0005: Defer non-hot dispatch lookup rewrite (PHF/HashMap)
- ADR-0006: Defer Lua VM pooling/reuse
- ADR-0007: Defer readability-only large module split refactors

## Consequences

- These six items are tracked as intentionally kept/deferred decisions, not
  immediate bug backlog.
- New work should prioritize measured hotspots, correctness, compatibility, and
  formally modeled concurrency paths first.

## Revisit Triggers

Use revisit triggers documented in ADR-0002 through ADR-0007.
