# 11.544 forced-raw string-encoding empty fast path

- Date: 2026-03-22
- Purpose: remove an unconditional mutex hit from the plain owner-inline `SET` path by skipping `forced_raw_string_keys` lookups/removals when no key is currently marked forced-raw.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 828; forced-raw empty fast path on top of the same HEAD)`
- Diff note: the baseline binary was built from a temp copy of the same working tree with only this `forced_raw_string_keys_count` fast path reverted in `request_lifecycle.rs`.

## Code change

- Ordinary `SET` / overwrite paths call `clear_forced_raw_string_encoding(...)` even though the forced-raw string-encoding map is usually empty.
- Before this slice, that still meant taking the `forced_raw_string_keys` mutex and attempting a `remove(...)` on every write.
- This slice adds a small `forced_raw_string_keys_count` atomic to `DbCatalogSideState` and keeps it in sync only when the forced-raw set changes.
- Hot-path effects:
  - `string_encoding_is_forced_raw(...)` returns immediately when the count is zero;
  - `clear_forced_raw_string_encoding(...)` returns immediately when the count is zero;
  - duplicate inserts/removes do not perturb the count.
- Semantics stay the same for the rare forced-raw cases; the change only short-circuits the common empty-map case.

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server getbit_setbit_setrange_and_bitcount_follow_string_semantics -- --nocapture`
  - `cargo test -p garnet-server string_external_mutations_switch_integer_encoding_to_raw -- --nocapture`
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
  - `SET ops/sec 83286 -> 98684` (`+18.49%`)
  - `GET ops/sec 119429 -> 119624` (`+0.16%`)
  - `SET p99 0.127 -> 0.111 ms` (`-12.60%`)
  - `GET p99 0.087 -> 0.087 ms` (`0.00%`)
- Interpretation:
  - the empty forced-raw bookkeeping path was still showing up as a meaningful write-side cost;
  - the win is tightly concentrated on `SET`, which is exactly where the unconditional `clear_forced_raw_string_encoding(...)` call sits.

## Artifacts

- Summary:
  - [before-summary.txt](./before-summary.txt)
  - [after-summary.txt](./after-summary.txt)
- Runs:
  - [before-runs.csv](./before-runs.csv)
  - [after-runs.csv](./after-runs.csv)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
