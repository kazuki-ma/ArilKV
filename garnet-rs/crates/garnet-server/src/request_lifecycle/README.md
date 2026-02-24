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
- Scripting (`EVAL*`, `SCRIPT`) is feature-gated by runtime env:
  - `GARNET_SCRIPTING_ENABLED=1` enables Lua execution.
  - default is disabled; command paths must preserve disabled behavior.
  - runtime hardening knobs:
    - `GARNET_SCRIPTING_MAX_SCRIPT_BYTES` (0 = unlimited, reject oversized script payloads).
    - `GARNET_SCRIPTING_CACHE_MAX_ENTRIES` (0 = unlimited, FIFO evicts oldest cached SHA).
    - `GARNET_SCRIPTING_MAX_MEMORY_BYTES` (0 = unlimited, per-Lua-state memory limit).
    - `GARNET_SCRIPTING_MAX_EXECUTION_MILLIS` (0 = unlimited, instruction-hook timeout).
  - `SCRIPT LOAD/EXISTS` and `EVALSHA*` share SHA1 cache semantics (`NOSCRIPT` on miss).
  - `EVAL_RO`/`EVALSHA_RO` must reject mutating commands via `redis.call`/`redis.pcall`.
  - `FUNCTION LOAD [REPLACE]` + `FCALL_RO` are minimally supported; `FCALL_RO` is restricted to functions registered with `no-writes`.
  - `INFO` exposes scripting observability (`scripting_cache_*`, `scripting_runtime_timeouts`, and configured limit values).
- Blocking list commands use owner-thread polling with wait-queue fairness:
  - timeout is parsed as float seconds and enforced (including non-turn queue waits).
  - waiter order is tracked per key; queue cleanup runs on wake/timeout/disconnect.
  - mutation paths may yield briefly when blocked clients exist to reduce wakeup ordering races.
- For list pop operations, right-side multi-pop return order must follow pop order (`RIGHT` pops from tail first).

## Test Focus

- Always include:
  - happy-path RESP shape checks
  - syntax/value errors
  - wrongtype behavior
  - missing-key/null behavior
- Prefer `testkit` (`assert_command_response` / `assert_command_integer`) for simple command lines.
- Use RESP-frame helpers for precise nested-array validation.
