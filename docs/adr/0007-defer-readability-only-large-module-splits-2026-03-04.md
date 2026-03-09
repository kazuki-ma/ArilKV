# ADR-0007: Defer Readability-Only Large Module Split Refactors

- Status: Accepted (Deferred)
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review reference:
  - `.review_by_claude/10_READABILITY.md`

## Context

Review recommends splitting very large modules (for example
`connection_handler.rs`, `request_lifecycle.rs`) into narrower files.

This is a valid maintainability direction, but these edits are broad and touch
high-risk integration surfaces (routing, blocking, replication, lifecycle).

## Decision

Defer large module split refactors when the primary motivation is readability
and no immediate correctness/performance incident is tied to the current
structure.

## Consequences

- Current file-size/readability debt remains.
- We avoid high regression-surface refactors without direct payoff evidence.
- Refactors should be scoped when they can be attached to concrete behavior
  change or measured incident prevention.

## Revisit Triggers

Reopen this decision when:

1. a concrete maintenance incident is traced to current module size/shape, or
2. a feature effort naturally scopes safe extraction boundaries, and
3. extraction plan includes test gates proving no behavior regression.

