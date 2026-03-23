# 11.541 Reuse the per-client `last_command` buffer

- Date: 2026-03-19
- Purpose: remove the per-command `Vec` rebuild in `ServerMetrics::set_client_last_command(...)` without changing any `CLIENT LIST` / ACL log surface.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 825; per-client last-command buffer reuse on top of the same HEAD)`
- Diff note: before/after are two focused measurements on the same `HEAD`. The runtime delta for this slice is limited to `lib.rs`.

## Code change

- `set_client_last_command(...)` used to assign a freshly collected lowercase `Vec<u8>` on every command.
- It now reuses the existing `client.last_command` buffer:
  - `clear()`
  - `reserve(...)`
  - extend lowercase command bytes in place
  - append lowercase `|subcommand` bytes in place when needed
- This keeps the exact external string surface unchanged while removing a small but unconditional allocation from every command.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`
  - `cargo test -p garnet-server acl_log_tracks_auth_and_acl_denials_like_external_scenarios -- --nocapture`
- Formatting:
  - `make fmt`
  - `make fmt-check`
- Full server gate:
  - `make test-server`
  - Verified counts: `704 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark shape

- Owner-inline single-thread shape, same as the recent `SET` hot-path work:
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

## Result

- Final `RUNS=5` median A/B:
  - `SET ops/sec 117910 -> 120125` (`+1.88%`)
  - `GET ops/sec 135035 -> 136981` (`+1.44%`)
  - `SET p99 0.071 -> 0.071 ms` (`0.00%`)
  - `GET p99 0.071 -> 0.071 ms` (`0.00%`)
- Interpretation:
  - this is a small cleanup, not a step-function change;
  - but it is a clean unconditional win with no p99 penalty in the acceptance run.

## Artifacts

- Summary:
  - [before-summary.txt](./before-summary.txt)
  - [after-summary.txt](./after-summary.txt)
- Runs:
  - [before-runs.csv](./before-runs.csv)
  - [after-runs.csv](./after-runs.csv)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
