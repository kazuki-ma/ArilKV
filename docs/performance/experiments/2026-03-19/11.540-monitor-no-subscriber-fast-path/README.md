# 11.540 Skip `MONITOR` formatting when nobody is subscribed

- Date: 2026-03-19
- Purpose: remove unconditional `MONITOR` event formatting/allocation from the plain command hot path while preserving external `MONITOR` behavior.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 824; monitor no-subscriber guard on top of the same HEAD)`
- Diff note: before/after are two focused measurements on the same `HEAD`. The runtime delta for this slice is limited to `lib.rs`, `connection_handler.rs`, and one exact TCP regression in `tests.rs`.

## Code change

- Added `ServerMetrics::has_monitor_subscribers()` as a cheap receiver-count probe on the monitor broadcast channel.
- `connection_handler` now checks that probe before calling:
  - `build_monitor_event_line(...)`
  - `build_monitor_lua_event_line(...)`
  - `publish_monitor_event(...)`
- This means ordinary commands stop allocating monitor payloads when no client has issued `MONITOR`.
- Added exact TCP regression `monitor_receives_set_events_when_subscribed_like_external_scenario` so the subscribed path remains fixed.

## Benchmark shape

- Owner-inline single-thread shape, same as the recent `11.537` to `11.539` `SET` investigations:
  - `RUNS=5`
  - `THREADS=1`
  - `CONNS=4`
  - `REQUESTS=30000`
  - `PRELOAD_REQUESTS=30000`
  - `PIPELINE=1`
  - `SIZE_RANGE=1-256`
  - wrapper env:
    - `GARNET_OWNER_THREAD_PINNING=1`
    - `GARNET_OWNER_THREAD_CPU_SET=0`
    - `GARNET_OWNER_EXECUTION_INLINE=1`
    - `OWNER_THREADS=1`
- I also ran an initial `RUNS=3` smoke A/B. That sample improved throughput but gave a noisy `SET p99` uptick, so the accepted result for this iteration is the `RUNS=5` rerun below.

## Result

- Final `RUNS=5` median A/B:
  - `SET ops/sec 99828 -> 113049` (`+13.24%`)
  - `GET ops/sec 119189 -> 138964` (`+16.59%`)
  - `SET p99 0.087 -> 0.079 ms` (`-9.20%`)
  - `GET p99 0.095 -> 0.071 ms` (`-25.26%`)
- Interpretation:
  - the unconditional monitor formatting really was showing up as hot-path waste;
  - guarding it is worthwhile even when the caller never uses `MONITOR`.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server monitor_receives_set_events_when_subscribed_like_external_scenario -- --nocapture`
  - `cargo test -p garnet-server acl_cluster_failover_monitor_and_shutdown_commands_cover_basic_shapes -- --nocapture`
- Formatting:
  - `make fmt`
  - `make fmt-check`
- Full server gate:
  - `make test-server`
  - Verified counts: `704 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Artifacts

- Summary:
  - [before-summary.txt](./before-summary.txt)
  - [after-summary.txt](./after-summary.txt)
- Runs:
  - [before-runs.csv](./before-runs.csv)
  - [after-runs.csv](./after-runs.csv)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
