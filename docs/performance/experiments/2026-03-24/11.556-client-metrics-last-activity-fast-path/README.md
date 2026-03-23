# 11.556 client metrics last-activity fast path

- status: accepted
- before_commit: `baef4e3b9e073912484ed3ac52e2d873a81e1371`
- after_commit: unavailable; working-tree-only accepted change on top of `baef4e3b9e073912484ed3ac52e2d873a81e1371`
- revert_commit: not applicable

## Goal

Remove redundant `Instant::now()` work from the plain command path when the same request has already refreshed client activity through input/output byte accounting.

## Change

- kept `last_activity` sourced by real socket activity via `add_client_input_bytes(...)` and `add_client_output_bytes(...)`
- removed bookkeeping-only `last_activity = Instant::now()` updates from:
  - `ServerMetrics::set_client_last_command(...)`
  - `ServerMetrics::add_client_commands_processed(...)`
  - `ServerMetrics::set_client_wait_target_offset(...)`
  - `ServerMetrics::set_client_local_aof_wait_target_offset(...)`
- added exact regression `client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping`
- re-ran the existing `CLIENT INFO` / `CLIENT LIST` selected-db regression to keep user-visible client metadata covered

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `709 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.556 \
BASE_BIN=/tmp/garnet-11.556-baseline/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=metrics_activity_before \
NEW_LABEL=metrics_activity_after \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=30000 \
PRELOAD_REQUESTS=30000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

The owner-inline `RUNS=5` A/B was a clean keep:

- `SET ops 124345 -> 126536` (`+1.76%`)
- `GET ops 143070 -> 147450` (`+3.06%`)
- `SET p99 0.071 -> 0.071 ms` (`0.00%`)
- `GET p99 0.063 -> 0.063 ms` (`0.00%`)

The flamegraph hint was accurate: not every remaining `clock_gettime` in the client metrics path is necessary. Keeping `last_activity` tied to true socket activity preserves `CLIENT INFO/LIST` idle semantics while trimming a small but real common-path cost.

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
