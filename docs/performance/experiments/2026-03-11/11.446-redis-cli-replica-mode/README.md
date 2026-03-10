# 11.446 redis-cli replica mode

## Goal
Close the remaining `integration/redis-cli` compatibility bucket for `--replica` connection mode.

## Change Summary
- Mark downstream `SYNC` / `PSYNC` clients as `Replica` in `ServerMetrics`.
- Make `CLIENT KILL TYPE REPLICA|SLAVE` match and disconnect live downstream replica streams.
- Render `INFO REPLICATION` from live client state:
  - `connected_slaves:<n>`
  - `slaveN:ip=...,port=...,state=online,offset=0,lag=0`
- Add exact regressions:
  - `sync_client_appears_in_info_replication_and_client_kill_type_slave_disconnects_it`
  - `redis_cli_replica_mode_matches_external_scenario_when_repo_cli_is_available`

## Validation
- Targeted Rust:
  - `cargo test -p garnet-server sync_client_appears_in_info_replication_and_client_kill_type_slave_disconnects_it -- --nocapture`
  - `cargo test -p garnet-server redis_cli_replica_mode_matches_external_scenario_when_repo_cli_is_available -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
  - Result: `550 passed`, `23 passed`, `1 passed`
- Targeted external:
  - `REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis RUNTEXT_EXTRA_ARGS='--single integration/redis-cli --only "Connecting as a replica"' ./redis_runtest_external_subset.sh`
  - Result dir: `/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/tests/interop/results/redis-runtest-external-20260311-022609/`
  - Summary: `ok=1 err=0 ignore=1`

## Benchmark Smoke
- Base commit: `2c9580ab3f`
- New state: working tree for `11.446`
- Command:
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64 BASE_BIN=<before> NEW_BIN=<after> ./garnet-rs/benches/binary_ab_local.sh`
- Comparison:
  - `SET ops +2.05%`
  - `GET ops -11.68%`
  - `SET p99 -7.21%`
  - `GET p99 +19.16%`
- Absolute GET p99 moved from `0.167ms` to `0.199ms`.
- This change is on replica/admin client-state paths, not the steady-state GET/SET hot path.

## Artifacts
- `artifacts/comparison.txt`
- `artifacts/ab/`
- `diff.patch`
