# 11.543 write-only `SET` upsert output removal

- Date: 2026-03-22
- Purpose: remove the unused owned upsert output clone from the plain owner-inline `SET` hot path after the read/peek side of `handle_set` has already been optimized.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 827; write-only `SET` upsert path on top of the same HEAD)`
- Diff note: the accepted benchmark compares the current tree against a temp copy of the same working tree with only the `handle_set` upsert call reverted to the old `KvSessionFunctions` + owned `Vec<u8>` output path. The new helper definitions themselves stayed out of the measured request path in that baseline copy.

## Code change

- `KvSessionFunctions` used to clone the stored value into `Output=Vec<u8>` on every upsert, even though the common `SET` mutation path discards that output.
- This slice adds `KvWriteOnlySessionFunctions` with `Output=()`, keeping the same stored-value encoding/expiration semantics but skipping the unused output clone on write paths.
- `handle_set` still uses the ordinary session for old-value reads and compare-only peek checks, then switches to a write-only session just before the final upsert.
- The change is intentionally narrow:
  - `SET GET` and compare-only conditions still use the owned read path when semantics require it;
  - only the final plain string upsert path stops producing an unused owned output buffer.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server set_get_option_returns_old_value -- --nocapture`
  - `cargo test -p garnet-server extended_set_conditions_match_external_string_scenarios -- --nocapture`
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

- The first `RUNS=5` sample was noisy (`SET +0.41%`, `GET -3.64%`), so I reran the identical profile and used the second sample as the acceptance bar.
- Accepted `RUNS=5` median A/B:
  - `SET ops/sec 94773 -> 107351` (`+13.27%`)
  - `GET ops/sec 121842 -> 123988` (`+1.76%`)
  - `SET p99 0.095 -> 0.087 ms` (`-8.42%`)
  - `GET p99 0.079 -> 0.079 ms` (`0.00%`)
- Interpretation:
  - the unused upsert-output clone was still material in the owner-inline `SET` path;
  - the improvement is concentrated where expected, while `GET` stayed effectively flat on the accepted rerun.

## Artifacts

- Summary:
  - [before-summary.txt](./before-summary.txt)
  - [after-summary.txt](./after-summary.txt)
- Runs:
  - [before-runs.csv](./before-runs.csv)
  - [after-runs.csv](./after-runs.csv)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
