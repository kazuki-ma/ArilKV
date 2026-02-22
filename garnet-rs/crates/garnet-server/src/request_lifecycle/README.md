# request_lifecycle Notes

This directory owns command execution semantics and RESP response construction.

## Command Implementation Checklist

1. Implement handler in the relevant `*_commands.rs`.
2. Wire `CommandId` in `request_lifecycle.rs`.
3. Add command metadata in `../command_spec.rs`.
4. Add dispatch tests in `../command_dispatch.rs` as needed.
5. Add lifecycle tests in `tests.rs`.

## Patterns That Needed Re-checking

- RESP null type is command-dependent:
  - `BLPOP/BRPOP/LMPOP/BLMPOP` no-result uses null-array (`*-1`).
  - `RPOPLPUSH/BRPOPLPUSH/LMOVE/BLMOVE` no-result uses null-bulk (`$-1`).
- `LMPOP/BLMPOP` parsing is strict:
  - `numkeys` must be integer > 0.
  - direction must be `LEFT|RIGHT`.
  - optional `COUNT` must be `COUNT <positive integer>`.
- Blocking list commands currently behave as immediate-return operations:
  - timeout is parsed and validated (float, non-negative).
  - no sleeping/wait queue is used yet.
- For list pop operations, right-side multi-pop return order must follow pop order (`RIGHT` pops from tail first).

## Test Focus

- Always include:
  - happy-path RESP shape checks
  - syntax/value errors
  - wrongtype behavior
  - missing-key/null behavior
- Prefer `testkit` (`assert_command_response` / `assert_command_integer`) for simple command lines.
- Use RESP-frame helpers for precise nested-array validation.
