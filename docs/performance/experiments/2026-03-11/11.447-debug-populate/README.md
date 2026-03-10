# 11.447 debug populate

- before_commit: `a0355b9377`
- after_commit: `working tree`
- scope: `DEBUG POPULATE` compatibility for `integration/redis-cli` RDB dump precondition

## Validation

- targeted Rust:
  - `cargo test -p garnet-server debug_populate_creates_requested_string_keys_without_overwriting_existing_values -- --nocapture`
  - `cargo test -p garnet-server debug_populate_matches_external_redis_cli_rdb_dump_precondition -- --nocapture`
- core gate:
  - `make fmt`
  - `make test-server`
- targeted external:
  - `REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis RUNTEXT_EXTRA_ARGS='--single integration/redis-cli --only "Dumping an RDB - functions only: no"' ./redis_runtest_external_subset.sh`
  - `REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis RUNTEXT_EXTRA_ARGS='--single integration/redis-cli --only "Dumping an RDB - functions only: yes"' ./redis_runtest_external_subset.sh`

## Results

- exact Rust regressions pass
- `make test-server` passes: `552 + 23 + 1`
- targeted external advanced past `DEBUG POPULATE` unknown-subcommand failure
- next blocker is `DEBUG RELOAD NOSAVE` staying no-op:
  - both `functions only: no` and `functions only: yes` now fail at `assert_equal {} [r get should-not-exist]`
  - result dirs:
    - `garnet-rs/tests/interop/results/redis-runtest-external-20260311-023843/`
    - `garnet-rs/tests/interop/results/redis-runtest-external-20260311-024000/`

## Benchmark smoke

- harness: `garnet-rs/benches/binary_ab_local.sh`
- params: `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`
- comparison: `artifacts/ab/comparison.txt`
- delta summary:
  - `SET ops -2.22%`
  - `GET ops -2.00%`
  - `SET p99 +0.00%`
  - `GET p99 -4.37%`
