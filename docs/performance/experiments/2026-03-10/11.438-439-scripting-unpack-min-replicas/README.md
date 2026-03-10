# 11.438-439 scripting unpack and min-replicas

- Date: 2026-03-10
- Area: isolated `unit/scripting`
- Before commit: `346214b8d1`
- After commit: working tree

## Goal

Close two isolated Redis `unit/scripting` residual buckets together:

- `Script check unpack with massive arguments`
- `not enough good replicas`

These were bundled because both required the same Lua/runtime rebuild, isolated `unit/scripting` rerun, and benchmark/validation cycle.

## Change summary

- Added workspace patch override for vendored `lua-src`:
  - `[patch.crates-io] lua-src = { path = "third_party/lua-src-550.0.0" }`
- Raised Lua 5.1 `LUAI_MAXCSTACK` from `8000` to `32768` so Redis' `unpack(a)` script path can fan out `7999` arguments into `redis.call(...)` without tripping the Lua C-stack ceiling.
- Added exact regression `scripting_massive_unpack_arguments_match_external_scenario_for_eval_and_function` covering both `EVAL` and `FCALL`.
- Extended direct command execution with `min-replicas-to-write` gating.
- Added scripting-specific replica gating semantics:
  - shebang scripts require good replicas unless `flags=no-writes`
  - non-shebang `EVAL` only gates on mutating bodies
  - `FCALL` gates on loaded function read-only metadata
- Added exact TCP integration regression `scripting_min_replicas_gate_matches_external_scenario`.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server scripting_massive_unpack_arguments_match_external_scenario_for_eval_and_function -- --nocapture`
  - `cargo test -p garnet-server eval_unpack_with_huge_range_returns_error_instead_of_crash -- --nocapture`
  - `cargo test -p garnet-server scripting_min_replicas_gate_matches_external_scenario -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Targeted external:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting --only 'Script check unpack with massive arguments'" ./redis_runtest_external_subset.sh`
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting --only 'not enough good replicas'" ./redis_runtest_external_subset.sh`
- Isolated external follow-up:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_RUN_ONLY_ISOLATED_UNIT=unit/scripting ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`
  - `RUNS=3 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`
  - `RUNS=3 THREADS=1 CONNS=4 REQUESTS=5000 PRELOAD_REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `532 + 23 + 1` pass
- targeted external `Script check unpack with massive arguments`: `ok=2 err=0 timeout=0 ignore=14`
  - result dir: `/tmp/garnet-scripting-unpack-1773152118`
- targeted external `not enough good replicas`: `ok=1 err=0 timeout=0 ignore=14`
  - result dir: `/tmp/garnet-scripting-wait-1773152447`
- isolated `unit/scripting`: `ok=295 err=15 timeout=0 ignore=26`
  - previous snapshot: `ok=292 err=18 ignore=26`
  - result dir: `/tmp/garnet-scripting-isolated-1773152536`

## Benchmark note

The smoke benchmark was noisy and did not show a stable regression signal.

- `artifacts/bench/comparison.txt` (`RUNS=1`, `REQUESTS=2000`):
  - `SET ops -7.46%`
  - `GET ops +8.20%`
  - `SET p99 -12.60%`
  - `GET p99 -25.16%`
- `artifacts/bench-runs3/comparison.txt` (`RUNS=3`, `REQUESTS=2000`):
  - `SET ops -1.12%`
  - `GET ops -9.00%`
  - `SET p99 +0.00%`
  - `GET p99 +40.34%`
- `artifacts/bench-runs3-req5000/comparison.txt` (`RUNS=3`, `REQUESTS=5000`):
  - `SET ops -3.42%`
  - `GET ops +4.99%`
  - `SET p99 +46.60%`
  - `GET p99 -12.60%`

Given that these changes only touch scripting compatibility and one command-path gating branch, and the A/B deltas change sign across reruns, treat this as benchmark noise rather than a credible hot-path performance shift.

## Remaining buckets

- `Script ACL check`
- `Script block the time during execution`
- `Script delete the expired key`
- `TIME command using cached time`
- `Script block the time in some expiration related commands`
- `RESTORE expired keys with expiration time`
- `SPOP: We can call scripts rewriting client->argv from Lua`
- `EXPIRE: We can call scripts rewriting client->argv from Lua`
- replication stream mismatch around `KEEPTTL`

These now collapse into four implementation buckets: `acl_check_cmd()`, frozen-time / expiration, `RESTORE` expired TTL, and argv rewrite replication.

## Artifacts

- `artifacts/bench/comparison.txt`
- `artifacts/bench-runs3/comparison.txt`
- `artifacts/bench-runs3-req5000/comparison.txt`
- `diff.patch`
