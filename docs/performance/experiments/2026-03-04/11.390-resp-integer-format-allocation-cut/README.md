# Experiment 11.390 - RESP Integer Formatting Allocation Cut

## Metadata

- experiment_id: `11.390`
- date: `2026-03-04`
- before_commit: `0fa2aeac3c`
- after_commit: `working-tree (uncommitted)`
- commit_message: `perf(server): avoid temporary String allocations for RESP integer/length formatting`
- full_diff: `docs/performance/experiments/2026-03-04/11.390-resp-integer-format-allocation-cut/diff.patch`

## Goal

Reduce hot-path heap allocations from integer/length RESP formatting by replacing
`to_string()`-based formatting with stack-buffer decimal emission in:

- `request_lifecycle/resp.rs`
- `connection_handler.rs`

## Code Change Summary

- Added stack-based decimal append helpers for `usize`/`i64`.
- Replaced `to_string().as_bytes()` in hot RESP writers and `encode_resp_frame`.
- Added regression tests for edge values (`i64::MIN`) and RESP frame shape.

## Validation

- `make fmt`
- Targeted tests:
  - `cargo test -p garnet-server --lib append_integer_handles_negative_and_min_values -- --nocapture`
  - `cargo test -p garnet-server --lib encode_resp_frame_preserves_expected_resp2_shape -- --nocapture`
- `make test-server` (`380 + 23 + 1` pass)

## Benchmark Procedure

```bash
BASE_COMMIT=$(git rev-parse HEAD)
BASE_DIR=/tmp/garnet-base-${BASE_COMMIT:0:12}-rt002
git worktree add --detach "$BASE_DIR" "$BASE_COMMIT"

cd "$BASE_DIR/garnet-rs" && cargo build --release -p garnet-server
cd /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs && cargo build --release -p garnet-server

OUTDIR=/tmp/garnet-rt002-ab-$(date +%Y%m%d-%H%M%S)
BASE_BIN="$BASE_DIR/garnet-rs/target/release/garnet-server"
NEW_BIN="/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server"
BASE_LABEL=before_head NEW_LABEL=after_rt002_asciifmt \
RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 PRELOAD_REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-1024 PORT=16451 \
  garnet-rs/benches/binary_ab_local.sh
```

## Results Snapshot

Source: `artifacts/ab_r3/comparison.txt`

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| median_set_ops | 141454 | 143628 | +1.54% |
| median_get_ops | 149741 | 150239 | +0.33% |
| median_set_p99_ms | 0.463 | 0.447 | -3.46% |
| median_get_p99_ms | 0.447 | 0.447 | 0.00% |

Interpretation: no regression in this local A/B run; small positive movement on
SET throughput and SET p99.

## Artifacts

- `artifacts/ab_r3/comparison.txt`
- `artifacts/ab_r3/before_head/summary.txt`
- `artifacts/ab_r3/before_head/runs.csv`
- `artifacts/ab_r3/after_rt002_asciifmt/summary.txt`
- `artifacts/ab_r3/after_rt002_asciifmt/runs.csv`
- `diff.patch`
