# 11.437 scripting setresp resp3 bridge

- Date: 2026-03-10
- Area: isolated `unit/scripting`
- Before commit: `7f88be61c8`
- After commit: working tree

## Goal

Close the largest remaining isolated scripting bucket by implementing `redis.setresp()` and the RESP3-native scripting bridge used by Redis `tests/unit/scripting.tcl`.

## Change summary

- Added exact TCP regressions:
  - `scripting_resp3_map_external_scenario_runs_as_tcp_integration_test`
  - `scripting_resp_protocol_parsing_matrix_matches_external_scenarios`
- Added script-local RESP mode via thread-local override so `redis.call` / `redis.pcall` run under `RESP2` or `RESP3` independently of the client connection protocol.
- Implemented `redis.setresp(2|3)` in script and function runtimes.
- Extended the scripting RESP bridge with RESP3 native types:
  - map
  - set
  - double
  - big number
  - verbatim string
  - boolean
  - attribute-ignore passthrough
  - RESP3 null to Lua nil
- Filled the missing `DEBUG PROTOCOL` subcommands used by the external scripting suite:
  - `MAP`
  - `SET`
  - `DOUBLE`
  - `NULL`
- Expanded request-lifecycle coverage for `DEBUG PROTOCOL` RESP2/RESP3 wire shapes.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server scripting_resp3_map_external_scenario_runs_as_tcp_integration_test -- --nocapture`
  - `cargo test -p garnet-server scripting_resp_protocol_parsing_matrix_matches_external_scenarios -- --nocapture`
  - `cargo test -p garnet-server request_lifecycle::tests::debug_protocol_subcommands_cover_resp2_and_resp3_shapes -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Targeted external:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS="--single unit/scripting ..." ./redis_runtest_external_subset.sh`
- Isolated external follow-up:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_RUN_ONLY_ISOLATED_UNIT=unit/scripting ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `530 + 23 + 1` pass
- targeted external scripting probe: `ok=40 err=0 timeout=0 ignore=14`
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-scripting-setresp-20260310-1`
- isolated `unit/scripting`: `ok=292 err=18 timeout=0 ignore=26`
  - previous snapshot: `ok=216 err=94 ignore=26`
  - current result dir: `garnet-rs/tests/interop/results/redis-runtest-scripting-isolated-20260310-1`
- smoke benchmark (`artifacts/bench/comparison.txt`):
  - `SET ops +0.13%`
  - `GET ops +2.76%`
  - `SET p99 +0.00%`
  - `GET p99 -6.72%`

## Remaining buckets

- `Script check unpack with massive arguments`
- `Script ACL check`
- `Script block the time during execution`
- `Script delete the expired key`
- `TIME command using cached time`
- `Script block the time in some expiration related commands`
- `RESTORE expired keys with expiration time`
- `SPOP: We can call scripts rewriting client->argv from Lua`
- `EXPIRE: We can call scripts rewriting client->argv from Lua`
- replication stream mismatch around `KEEPTTL`
- `not enough good replicas`

These collapse into six implementation buckets: `unpack`, `acl_check_cmd()`, frozen-time expiration, `RESTORE` expired TTL, argv rewrite replication, and `WAIT` gate.

## Artifacts

- `artifacts/bench/base-7f88be61c8/summary.txt`
- `artifacts/bench/base-7f88be61c8/runs.csv`
- `artifacts/bench/worktree-11.437/summary.txt`
- `artifacts/bench/worktree-11.437/runs.csv`
- `artifacts/bench/comparison.txt`
- `diff.patch`
