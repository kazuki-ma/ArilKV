# 13.11 ArgSlice Boundary Dispatch A/B (Accepted)

- date: 2026-02-24
- todo: 11.126
- before_commit: `889ce1308b` (`refactor(server): encapsulate ArgSlice unsafe access and capture A/B`)
- after_commit: `fa9b020a7e` (`refactor(server): dispatch request lifecycle on byte-slice args`)
- revert_commit: `N/A`

## Goal

Adopt the boundary-conversion design:

- convert `ArgSlice` -> `&[u8]` once at request execution boundary,
- execute command handlers on `&[&[u8]]`,
- avoid per-handler/per-call `ArgSlice` conversion overhead and scattered unsafe conversions.

## Code Scope

- `garnet-rs/crates/garnet-server/src/request_lifecycle.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/command_helpers.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/geo_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/hash_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/list_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/server_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/set_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/stream_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/string_commands.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/tests.rs`
- `garnet-rs/crates/garnet-server/src/request_lifecycle/zset_commands.rs`

## Validation

- `cargo check -p garnet-server` PASS
- `cargo test -p garnet-server -- --nocapture` PASS
  - `running 210 tests` -> `210 passed`
  - `running 23 tests` -> `23 passed`
  - `running 1 test` -> `1 passed`
- Redis external subset PASS
  - `garnet-rs/tests/interop/results/redis-runtest-external-20260224-084624`
  - `tests=6/4/2` + `redis_cli_type_probe` PASS

## Method

Built `before` and `after` binaries from exact commits and compared via:

```bash
cd garnet-rs
BASE_BIN=/tmp/garnet-server-base-889ce1308b \
NEW_BIN=/tmp/garnet-server-new-fa9b020a7e \
BASE_LABEL=before_889ce1308b \
NEW_LABEL=after_fa9b020a7e \
RUNS=5 \
./benches/binary_ab_local.sh
```

`binary_ab_local.sh` runs `perf_regression_gate_local.sh` internally, which enforces:

- expected `Threads/Connections per thread/Requests per client` checks from memtier stdout,
- fail-fast on `Connection error:` and `handle error response:`,
- median-based comparison output.

## Result (Median)

### Shape A: 1-thread hot path

- params: `THREADS=1 CONNS=4 REQUESTS=30000 PRELOAD_REQUESTS=30000 PIPELINE=32 SIZE_RANGE=1-256 OWNER_THREADS=1`
- SET ops/s: `225745` -> `248775` (`+10.20%`)
- GET ops/s: `269374` -> `278206` (`+3.28%`)
- SET p99 ms: `0.951` -> `0.831` (`-12.62%`)
- GET p99 ms: `0.815` -> `0.831` (`+1.96%`)

### Shape B: concurrent profile

- params: `THREADS=4 CONNS=8 REQUESTS=20000 PRELOAD_REQUESTS=20000 PIPELINE=1 SIZE_RANGE=1-1024 OWNER_THREADS=1`
- SET ops/s: `169020` -> `169814` (`+0.47%`)
- GET ops/s: `172519` -> `172418` (`-0.06%`)
- SET p99 ms: `0.351` -> `0.343` (`-2.28%`)
- GET p99 ms: `0.351` -> `0.343` (`-2.28%`)

Interpretation: accepted. The change improves the intended 1-thread hot path and is near-neutral under concurrent shape, with no material regression signal.

## Artifacts

- `comparison-single.txt`
- `summary-single-before.txt`
- `summary-single-after.txt`
- `runs-single-before.csv`
- `runs-single-after.csv`
- `comparison-concurrent.txt`
- `summary-concurrent-before.txt`
- `summary-concurrent-after.txt`
- `runs-concurrent-before.csv`
- `runs-concurrent-after.csv`
- `diff.patch`
