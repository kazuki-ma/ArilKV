# Experiment 11.37 - Binary A/B Harness and Fail-Fast Validation

## Metadata

- experiment_id: `11.37`
- date: `2026-02-20`
- before_commit: `5eec063f5961eab218066e7df35d8fbf98fcc8c0`
- after_commit: `fd76470370d57921b878a86773edf4a295f2d71c`
- commit_message: `bench: add binary A/B harness and fail on memtier server errors`
- full_diff: `docs/performance/experiments/2026-02-20/11.37-binary-ab-harness-and-failfast/diff.patch`

## Goal

1. Add a reusable local harness for A/B comparison between arbitrary binaries.
2. Ensure benchmark scripts fail when memtier shows server-side errors (`handle error response:`), not only connection failures.

## Code Changes

- Added new script:
  - `garnet-rs/benches/binary_ab_local.sh`
- Added/updated fail-fast checks:
  - `garnet-rs/benches/perf_regression_gate_local.sh`
  - `garnet-rs/benches/local_hotspot_framegraph_macos.sh`
  - `garnet-rs/benches/sweep_string_store_shards_local.sh`
- Documentation and status updates:
  - `garnet-rs/benches/README.md`
  - `TODO_AND_STATUS.md`

## Detailed Commands and Test-by-Test Results

1. Shell syntax validation
   - command: `bash -n garnet-rs/benches/perf_regression_gate_local.sh`
   - result: `exit 0`
2. Shell syntax validation
   - command: `bash -n garnet-rs/benches/local_hotspot_framegraph_macos.sh`
   - result: `exit 0`
3. Shell syntax validation
   - command: `bash -n garnet-rs/benches/sweep_string_store_shards_local.sh`
   - result: `exit 0`
4. Shell syntax validation
   - command: `bash -n garnet-rs/benches/binary_ab_local.sh`
   - result: `exit 0`
5. A/B smoke run using two binaries
   - command:
     ```bash
     cd garnet-rs
     BASE_BIN=/tmp/garnet-server-shardfnv \
     NEW_BIN=/tmp/garnet-server-rxbuf-opt \
     RUNS=1 THREADS=4 CONNS=8 REQUESTS=3000 PRELOAD_REQUESTS=3000 \
     PORT=16401 OUTDIR=/tmp/garnet-binary-ab-smoke-20260220-142851 \
     ./benches/binary_ab_local.sh
     ```
   - result: `exit 0`
6. Negative test for server-error fail-fast
   - command:
     ```bash
     cd garnet-rs
     SERVER_BIN=/tmp/garnet-server-shardfnv \
     RUNS=1 THREADS=4 CONNS=8 REQUESTS=12000 PRELOAD_REQUESTS=12000 \
     PORT=16402 MAX_IN_MEMORY_PAGES=64 \
     OUTDIR=/tmp/garnet-perf-gate-errorcheck-20260220-142909 \
     ./benches/perf_regression_gate_local.sh
     ```
   - result: `exit 1`
   - key error: `server error responses detected in .../set.log`

## Benchmark Results (A/B Smoke)

Source: `artifacts/binary-ab-smoke-comparison.txt`

| Metric | Base | New | Delta |
|---|---:|---:|---:|
| median_set_ops | 121473.000 | 157468.000 | +29.63% |
| median_get_ops | 171814.000 | 178159.000 | +3.69% |
| median_set_p99_ms | 1.52700 | 0.64700 | -57.63% |
| median_get_p99_ms | 0.39100 | 0.39900 | +2.05% |

Note: this smoke run validated harness behavior only; later dedicated A/B runs determined the `rxbuf` rewrite was regressive and rejected.

## Diff Excerpt

```diff
+if grep -q 'handle error response:' "${log_file}"; then
+    echo "server error responses detected in ${log_file}" >&2
+    exit 1
+fi
```

```diff
+## Binary A/B (local)
+Use `binary_ab_local.sh` to compare two arbitrary `garnet-server` binaries
+under the same median gate harness.
```

## Artifacts

- `artifacts/binary-ab-smoke-comparison.txt`
- `artifacts/binary-ab-smoke-base-summary.txt`
- `artifacts/binary-ab-smoke-new-summary.txt`
- `artifacts/binary-ab-smoke-base-runs.csv`
- `artifacts/binary-ab-smoke-new-runs.csv`
- `artifacts/errorcheck-runs.csv`
- `artifacts/errorcheck-handle-error-response-excerpt.txt`
- `diff.patch`
