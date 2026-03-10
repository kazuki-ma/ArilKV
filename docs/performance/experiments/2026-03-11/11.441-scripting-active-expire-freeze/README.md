# 11.441 scripting active-expire freeze

- Date: 2026-03-11
- Area: isolated `unit/scripting`
- Before commit: `f7747f3b70`
- After commit: working tree

## Goal

Close the remaining scripting frozen-time bucket:

- `Script block the time during execution`
- `RESTORE expired keys with expiration time`

The actual defect was a race: script/function execution froze logical time, but the background active-expire task could still delete keys before the script returned.

## Change summary

- Added processor-scoped frozen-time plumbing:
  - `CurrentProcessorScope`
  - `ScriptTimeSnapshot`
  - `current_unix_time_micros()`
  - `current_instant()`
- `TIME`, expiration math, and `RESTORE ABSTTL` now use the cached script timestamp while a script/function is executing.
- Added DEBUG-only expired-data access helper so `DEBUG OBJECT` can inspect expired values when Redis-compatible debug flags allow it.
- Tightened `DEBUG OBJECT` so truly missing keys still return `ERR no such key`.
- Active-expire tick now skips while `script_execution_in_progress()` is true.
- Added exact regressions for both `EVAL` and `FCALL`, including deterministic multishard race probes.
- Added TLA+ model `formal/tla/specs/ScriptActiveExpireFreeze.tla`.

## Validation

- Direct Tcl repro against release `garnet-server`
  - `FCALL` and `EVAL` both return equal positive `PEXPIRETIME` values after `RESTORE ... ABSTTL`, `DEBUG SLEEP`, and a second `RESTORE`.
- Targeted Rust:
  - `cargo test -p garnet-server scripting_freezes_key_expiration_during_execution_matches_external_scenario -- --nocapture`
  - `cargo test -p garnet-server scripting_restore_expired_keys_with_expiration_time_matches_external_scenario -- --nocapture`
  - `cargo test -p garnet-server scripting_function_freezes_key_expiration_during_execution_like_external_scenario -- --nocapture`
  - `cargo test -p garnet-server scripting_function_restore_expired_keys_with_expiration_time_like_external_scenario -- --nocapture`
  - `cargo test -p garnet-server scripting_function_freezes_expiration_while_active_expire_runs -- --nocapture`
  - `cargo test -p garnet-server scripting_function_restore_keeps_frozen_ttl_under_active_expire -- --nocapture`
- TLA+:
  - `./tools/tla/run_tlc.sh formal/tla/specs/ScriptActiveExpireFreeze.tla formal/tla/specs/ScriptActiveExpireFreeze_bug.cfg`
  - `./tools/tla/run_tlc.sh formal/tla/specs/ScriptActiveExpireFreeze.tla formal/tla/specs/ScriptActiveExpireFreeze_fixed.cfg`
- Core gate:
  - `make fmt`
  - `make test-server`
- Isolated external lane:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_RUN_ONLY_ISOLATED_UNIT=unit/scripting ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=3 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `545 + 23 + 1` pass
- direct Tcl repro:
  - `MODE=fcall result=<same positive> <same positive> <same positive>`
  - `MODE=eval result=<same positive> <same positive> <same positive>`
- external full-mode single-case probes for these exact names are currently skipped in external-server mode, so they are not useful as pass/fail validation for this bucket.
- isolated `unit/scripting` rerun:
  - before: `ok=295 err=15 ignore=26`
  - after: `ok=307 err=2 ignore=26`
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-scripting-isolated-11.441-20260311-005530/`
- fixed cases confirmed `[ok]` in isolated log:
  - `Script block the time during execution`
  - `RESTORE expired keys with expiration time`
- remaining isolated failures:
  - `Script ACL check`
  - `Script ACL check`

## Benchmark note

Use the 3-run median as the decision signal. The initial 1-run smoke showed noisy GET regression, but the 3-run rerun removed it.

- `artifacts/bench/comparison.txt`
  - `SET ops +0.01%`
  - `GET ops +3.60%`
  - `SET p99 -6.72%`
  - `GET p99 -13.71%`

This slice touches cron/expiration scheduling rather than the GET/SET hot path directly, so treat the benchmark as a non-regression guard, not as a throughput claim.

## Artifacts

- `artifacts/bench/comparison.txt`
- `diff.patch`
