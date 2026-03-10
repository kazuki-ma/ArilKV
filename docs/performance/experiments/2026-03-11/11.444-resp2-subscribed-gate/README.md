# 11.444 redis-cli RESP2 subscribed-mode gate

- Date: 2026-03-11
- Area: `integration/redis-cli`
- Before commit: `0e4e98332a`
- After commit: working tree

## Goal

Close the next real `integration/redis-cli` residual after the harness timeout was removed:

- `Subscribed mode`

## Change summary

- Added a RESP2 subscribed-context gate on the connection path.
- When a client is subscribed and currently using RESP2, only these commands are allowed:
  - `(P|S)SUBSCRIBE`
  - `(P|S)UNSUBSCRIBE`
  - `PING`
  - `QUIT`
  - `RESET`
- All other commands now return the Redis-compatible subscribed-mode error.
- Added exact TCP regression:
  - `subscribed_mode_resp2_after_hello2_rejects_regular_commands_like_external_redis_cli_scenario`

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server subscribed_mode_resp2_after_hello2_rejects_regular_commands_like_external_redis_cli_scenario -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- External evidence:
  - File-level rerun at `garnet-rs/tests/interop/results/redis-runtest-redis-cli-file-11.444-20260311-014013/`
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `547 + 23 + 1` pass
- The `Subscribed mode` failure disappeared from the redis-cli external rerun.
- Remaining redis-cli buckets are now:
  - command-line hinting
  - `DEBUG POPULATE` / RDB dump admin support
  - `--replica` connection mode

## Benchmark note

This branch is only exercised when a RESP2 client is already in subscribed mode. The smoke benchmark is a non-regression guard, not a throughput claim.

- `artifacts/comparison.txt`
  - `SET ops -0.10%`
  - `GET ops -1.72%`
  - `SET p99 +7.21%`
  - `GET p99 +9.58%`

The deltas are acceptable for this narrow compatibility fix.

## Artifacts

- `artifacts/comparison.txt`
- `artifacts/benchmark.stdout.txt`
- `diff.patch`
