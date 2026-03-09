# Experiment 11.391 - Replication Frame Allocation Cut (RT-002 Slice 2)

## Metadata

- experiment_id: `11.391`
- date: `2026-03-04`
- before_commit: `0fa2aeac3c`
- after_commit: `working-tree (uncommitted)`
- commit_message: `perf(server): reduce transient allocations in replication frame rewrite/emit paths`
- full_diff: `docs/performance/experiments/2026-03-04/11.391-replication-frame-allocation-cut/diff.patch`

## Goal

Continue `RT-002` by reducing transient heap allocations in runtime replication
frame emission/rewrite paths inside `connection_handler.rs`.

## Code Change Summary

- Added borrowed-slice RESP frame encoder: `encode_resp_frame_slices`.
- Reworked transaction replication emit paths to avoid per-token `to_vec()` for
  static/dynamic command parts (`SELECT`, `MULTI`, `EXEC`, lazy-expire `DEL`).
- Reworked rewrite paths (`SET*`, `GETEX`, `EXPIRE*`, `RESTORE`, `BLMOVE`,
  `BRPOPLPUSH`, `LMPOP`) to use borrowed slices and stack decimal formatting
  (`u64_ascii_slice`) instead of `to_string().into_bytes()`.
- Reworked `EVALSHA -> EVAL` replication rewrite and `XREADGROUP` synthetic
  frame generation to reduce intermediate owned vectors.
- Added RESP frame capacity pre-sizing in both `encode_resp_frame` and
  `encode_resp_frame_slices`.

## Validation

- `make fmt`
- Targeted tests:
  - `cargo test -p garnet-server --lib replication_rewrites_ -- --nocapture`
  - `cargo test -p garnet-server --lib encode_resp_frame_preserves_expected_resp2_shape -- --nocapture`
- `make test-server` (`380 + 23 + 1` pass)

## Benchmark Procedure

```bash
BASE_BIN=/tmp/garnet-baseline-rt002-s2-20260304-134857
NEW_BIN=/tmp/garnet-new-rt002-s2b-20260304-135626

BASE_LABEL=before_11390 NEW_LABEL=after_11391 \
OUTDIR=/tmp/garnet-rt002-s2-ab-r5-20260304-1359 \
RUNS=5 THREADS=4 CONNS=8 REQUESTS=5000 PRELOAD_REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-1024 PORT=16457 \
  garnet-rs/benches/binary_ab_local.sh
```

## Results Snapshot (RUNS=5)

Source: `artifacts/ab_r5/comparison.txt`

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| median_set_ops | 130898 | 134228 | +2.54% |
| median_get_ops | 145624 | 150947 | +3.66% |
| median_set_p99_ms | 0.527 | 0.607 | +15.18% |
| median_get_p99_ms | 0.447 | 0.423 | -5.37% |

Interpretation: throughput moved positive in the longer run, while SET p99
worsened. Additional local noise checks are included in artifacts and showed
high run-to-run variance; keep this slice with explicit follow-up to re-evaluate
under a replication-heavy workload (`RT-002` next slice).

## Artifacts

- `artifacts/ab_r5/comparison.txt`
- `artifacts/ab_r5/before_11390/summary.txt`
- `artifacts/ab_r5/before_11390/runs.csv`
- `artifacts/ab_r5/after_11391/summary.txt`
- `artifacts/ab_r5/after_11391/runs.csv`
- `artifacts/noise_baseline/comparison.txt`
- `artifacts/noise_after/comparison.txt`
- `diff.patch`
