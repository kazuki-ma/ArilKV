# ADR-0003: Keep Inline Expiration Prefix Format for String Values

- Status: Accepted
- Date: 2026-03-04
- Owners: garnet-rs maintainers
- Review reference:
  - `.review_by_claude/04_DATA_STRUCTURES.md` (string `[u64 expiry][value]`)

## Context

String values are stored as:

- `[8-byte expiration millis prefix][user value bytes]`

The review points out fixed 8-byte overhead for non-expiring values.

## Decision

Keep the inline expiration prefix format as the current data-layout contract.

Do not switch to separate expiration metadata for string values in this phase.

## Consequences

- Read paths keep one-source value decode behavior.
- Non-expiring values keep fixed 8-byte overhead.
- Existing value codec assumptions and tests remain stable.

## Revisit Triggers

Reopen this decision when:

1. a migration plan is ready for backward-compatible data-layout transition, and
2. measured memory pressure from the 8-byte overhead is significant in target
   workloads, or
3. storage integration evolves to support cleaner metadata split without extra
   read-path cost.

