# 11.442 scripting acl_check_cmd

- Date: 2026-03-11
- Area: isolated `unit/scripting`
- Before commit: `4dc1082a3f`
- After commit: working tree

## Goal

Close the final isolated scripting residual:

- `Script ACL check`

## Change summary

- Replaced the old ACL user-name set in `ServerMetrics` with a minimal live ACL profile map.
- Added profile fields needed for `redis.acl_check_cmd()`:
  - `enabled`
  - `allow_all_commands`
  - `allowed_commands`
  - `key_patterns`
- Extended `ACL SETUSER` side effects to parse and persist the subset used by Redis scripting tests:
  - `on|off`
  - `+cmd`
  - `+@all`
  - `-cmd`
  - `-@all`
  - `~pattern`
  - `resetkeys`
- Added `redis.acl_check_cmd(...)` to both `EVAL` and `FCALL` runtimes.
- `redis.acl_check_cmd(...)` now:
  - rejects invalid commands with `ERR Invalid command passed to redis.acl_check_cmd()`
  - checks the current authenticated client user
  - validates command permission
  - validates key-pattern permission for first-key and all-keys command shapes
- Added exact TCP regression:
  - `scripting_acl_check_cmd_matches_external_scenario_for_eval_and_function`

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server scripting_acl_check_cmd_matches_external_scenario_for_eval_and_function -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Isolated external lane:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_RUN_ONLY_ISOLATED_UNIT=unit/scripting ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=3 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `546 + 23 + 1` pass
- isolated `unit/scripting` rerun:
  - before: `ok=307 err=2 ignore=26`
  - after: `ok=309 err=0 ignore=26`
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-scripting-isolated-11.442-20260311-011334/`
- This zeroes the isolated `unit/scripting` lane.

## Benchmark note

This slice touches ACL bookkeeping and the scripting helper surface, not the steady-state GET/SET hot path. Treat the benchmark as a non-regression guard.

- `artifacts/bench/comparison.txt`
  - `SET ops -2.10%`
  - `GET ops +1.62%`
  - `SET p99 +7.21%`
  - `GET p99 -5.59%`

The deltas are small enough to keep the change, especially because the new logic is only exercised by scripting ACL checks and ACL SETUSER side effects.

## Artifacts

- `artifacts/bench/comparison.txt`
- `diff.patch`
