# 11.440 scripting argv-rewrite replication

- Date: 2026-03-10
- Area: isolated `unit/scripting`
- Before commit: `6bb29c5fa6`
- After commit: working tree

## Goal

Close the remaining isolated scripting replication-rewrite bucket:

- `SPOP: We can call scripts rewriting client->argv from Lua`
- `EXPIRE: We can call scripts rewriting client->argv from Lua`
- `INCRBYFLOAT: We can call scripts expanding client->argv from Lua`

## Change summary

- Added exact TCP regressions:
  - `sync_replication_stream_rewrites_script_spop_commands_like_external_scenario`
  - `sync_replication_stream_rewrites_script_expire_and_argv_expansion_like_external_scenarios`
- Added `ScriptReplicationEffect` queueing to `RequestProcessor`.
- Cleared/consumed script effects per request so `EVAL*` / `FCALL*` replicate inner mutating effects instead of the outer script command.
- Captured successful non-error inner `redis.call` / `redis.pcall` command argv and RESP replies for rewrite decisions.
- Added script-specific replication rewrites:
  - `SPOP` -> lowercase `srem`
  - expire-family -> lowercase `pexpireat` or `del`
  - `INCRBYFLOAT` -> lowercase `set key <value> KEEPTTL`
- Kept `FUNCTION LOAD` on the existing top-level replication path by restricting effect-based replication to `EVAL*` / `FCALL*`.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server sync_replication_stream_rewrites_script_ -- --nocapture`
  - `cargo test -p garnet-server replicaof_replication_propagates_function_load_and_fcall -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Targeted external:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting --only 'SPOP: We can call scripts rewriting client->argv from Lua'" ./redis_runtest_external_subset.sh`
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting --only 'EXPIRE: We can call scripts rewriting client->argv from Lua'" ./redis_runtest_external_subset.sh`
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting --only 'INCRBYFLOAT: We can call scripts expanding client->argv from Lua'" ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `534 + 23 + 1` pass
- targeted external scripting probes:
  - `SPOP...`: `ok=1 err=0 timeout=0 ignore=14`
  - `EXPIRE...`: `ok=1 err=0 timeout=0 ignore=14`
  - `INCRBYFLOAT...`: `ok=1 err=0 timeout=0 ignore=14`
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-scripting-argv-rewrite-20260310-235339/`

## Benchmark note

The smoke benchmark did not show a regression.

- `artifacts/bench/comparison.txt`:
  - `SET ops +5.78%`
  - `GET ops +9.92%`
  - `SET p99 -13.45%`
  - `GET p99 -11.19%`

Given that this slice changes scripting replication bookkeeping on the connection path, treat the result as a non-regression guard rather than as a throughput claim.

## Remaining buckets

- `acl_check_cmd()`
- frozen-time / expiration
- `RESTORE` expired TTL

## Artifacts

- `artifacts/bench/comparison.txt`
- `diff.patch`
