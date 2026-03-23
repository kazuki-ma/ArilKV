# 11.542 commandstats lowercase lookup without per-command name allocation

- Date: 2026-03-19
- Purpose: remove the unconditional commandstats name allocation from the plain request hot path without changing `INFO COMMANDSTATS` / ACL rejection accounting surfaces.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 826; commandstats lowercase lookup + lazy command-call name materialization on top of the same HEAD)`
- Diff note: before/after are two focused measurements on the same `HEAD`. The baseline binary was built from a temp copy of the working tree with only this slice reverted in `request_lifecycle.rs`, `connection_handler.rs`, and the matching regression test file.

## Code change

- `connection_handler` used to allocate a `Vec<u8>` for the commandstats name on every command before any ACL/error path knew it needed that name later.
- `RequestProcessor::record_command_call(...)` also lowercased into a fresh `Vec<u8>` before every counter update.
- This slice does two things:
  - records command calls from borrowed `command` / `subcommand` parts, using a fixed stack scratch buffer for the common lookup path and allocating only on first insertion or unusually long names;
  - materializes the combined `command|subcommand` name lazily, only if ACL rejection / command failure handling actually needs it later in the request.
- Compatibility surface stays the same:
  - `INFO COMMANDSTATS` still renders lowercased names like `cmdstat_client|list`;
  - rejection/failure counters still use the same keys as before.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server commandstats_normalize_subcommand_names_when_recorded_from_parts -- --nocapture`
  - `cargo test -p garnet-server info_commandstats_counts_blocking_command_once -- --nocapture`
  - `cargo test -p garnet-server consistent_eval_error_reporting_matches_external_scripting_scenario -- --nocapture`
- Formatting:
  - `make fmt`
  - `make fmt-check`
- Full server gate:
  - `make test-server`
  - Verified counts: `705 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark shape

- Same owner-inline single-thread profile as the recent hot-path slices:
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
  - `SET ops/sec 117056 -> 122539` (`+4.68%`)
  - `GET ops/sec 138553 -> 141230` (`+1.93%`)
  - `SET p99 0.079 -> 0.071 ms` (`-10.13%`)
  - `GET p99 0.063 -> 0.063 ms` (`0.00%`)
- Interpretation:
  - this was a real request-path cost, not just bookkeeping noise;
  - the win is largest on `SET`, which matches the extra per-command allocation and lowercasing that used to happen before the request reached the actual mutation path.

## Artifacts

- Summary:
  - [before-summary.txt](./before-summary.txt)
  - [after-summary.txt](./after-summary.txt)
- Runs:
  - [before-runs.csv](./before-runs.csv)
  - [after-runs.csv](./after-runs.csv)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
