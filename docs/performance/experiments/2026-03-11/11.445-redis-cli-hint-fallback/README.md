# 11.445 redis-cli hint fallback

- Date: 2026-03-11
- Area: `integration/redis-cli`
- Before commit: `bc74bd295d`
- After commit: working tree

## Goal

Close the `integration/redis-cli` command-line hinting residuals:

- `Test command-line hinting - latest server`
- `Test command-line hinting - no server`
- `Test command-line hinting - old server`

## Change summary

- Server side:
  - `COMMAND DOCS` now returns `ERR unknown subcommand` instead of an empty table.
  - `INFO SERVER`, `HELLO`, and `LOLWUT` now report Redis-compatible semver `8.4.0`.
  - Added exact TCP integration regression:
    - `redis_cli_hint_suite_matches_external_scenarios_when_repo_cli_is_available`
- Harness side:
  - `redis_runtest_external_subset.sh` now attempts `make redis-cli -j4` in `REDIS_REPO_ROOT` when the repo `src/redis-cli` is missing or not runnable.
  - This keeps external CLI probes on the repo-matched upstream `redis-cli` instead of a potentially stale system binary.

## Why both changes were needed

- `latest server` was broken because redis-cli treated Garnet's empty `COMMAND DOCS` response as authoritative.
- `old server` and `no server` were broken because the host system `redis-cli` did not match the repo hint suite.
- Returning an error from `COMMAND DOCS` is only sufficient if `INFO SERVER` reports a numeric version, otherwise redis-cli filters out nearly all static docs.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server redis_cli_hint_suite_matches_external_scenarios_when_repo_cli_is_available -- --nocapture`
  - `cargo test -p garnet-server info_supports_section_filters_and_multi_section_arguments -- --nocapture`
  - `cargo test -p garnet-server client_info_kill_caching_reply_and_config_rewrite_stubs -- --nocapture`
  - `cargo test -p garnet-server lolwut_returns_bulk_with_version_info -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Direct exact probes:
  - `artifacts/hint-latest.txt`
  - `artifacts/hint-no-server.txt`
  - `artifacts/hint-old-server.txt`
- Interop harness smoke:
  - repo `src/redis-cli` temporarily removed, then `redis_runtest_external_subset.sh` rerun to force the new auto-build path
- Hot-path smoke benchmark:
  - `garnet-rs/benches/binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `548 + 23 + 1` pass
- Exact CLI hint probes:
  - latest server: `SUCCESS: 69/69 passed`
  - no server: `SUCCESS: 69/69 passed`
  - old server: `SUCCESS: 69/69 passed`
- Interop harness smoke:
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-redis-cli-harness-smoke-11.445-20260311-020742/`
  - summary: `PASS`, repo `redis-cli` rebuilt as host Mach-O successfully

## Benchmark note

This slice touches admin/introspection paths and interop harness logic, not the steady-state GET/SET data path. The benchmark is a non-regression guard.

- `artifacts/comparison.txt`
  - `SET ops -1.26%`
  - `GET ops -0.18%`
  - `SET p99 +0.00%`
  - `GET p99 +29.63%`

The GET p99 change is a small absolute move from `0.135ms` to `0.175ms` in a one-run smoke profile, acceptable for this compatibility slice.

## Remaining buckets

- `DEBUG POPULATE` / RDB dump admin support
- `--replica` connection mode

## Artifacts

- `artifacts/comparison.txt`
- `artifacts/benchmark.stdout.txt`
- `artifacts/hint-latest.txt`
- `artifacts/hint-no-server.txt`
- `artifacts/hint-old-server.txt`
- `diff.patch`
