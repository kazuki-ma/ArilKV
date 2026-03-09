# ADR-0006: Defer Lua VM Pooling/Re-use

- Status: Accepted (Deferred)
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review references:
  - `.review_by_claude/11_MISSING_FEATURES.md`
  - `.review_by_claude/13_CSHARP_COMPARISON.md`

## Context

Review notes that scripting currently creates fresh `Lua::new()` instances per
execution rather than reusing pooled VM state.

VM reuse may reduce setup cost, but introduces higher complexity for:

- state isolation
- safety around globals/libraries
- kill/timeout lifecycle
- memory-accounting behavior

## Decision

Defer Lua VM pooling/reuse in the current phase.

Keep per-execution VM isolation as the default correctness-first behavior.

## Consequences

- Script-heavy workloads may keep avoidable setup overhead.
- Scripting isolation model remains simpler and safer to reason about.
- Avoids introducing subtle cross-execution state leakage risks now.

## Revisit Triggers

Reopen this decision when:

1. script-heavy profiling shows VM creation as top bottleneck, and
2. a design proposal demonstrates safe isolation guarantees plus regression
   tests for kill/timeout/memory semantics.

