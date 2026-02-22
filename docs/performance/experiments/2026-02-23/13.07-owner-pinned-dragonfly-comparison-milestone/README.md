# Experiment 13.07 - Owner-Pinned Dragonfly Comparison Milestone

## Metadata

- experiment_id: `13.07`
- date: `2026-02-23`
- type: `MILESTONE`
- scope: `garnet` vs `dragonfly` under owner-pinned single-thread profile harness

## Why This Is A Milestone

Under the current owner-pinned + inline-owner execution conditions, Garnet median throughput exceeded Dragonfly on both `SET` and `GET` in this benchmark shape.

## Procedure

Command:

```bash
cd garnet-rs
RUNS=3 THREADS=1 CONNS=4 REQUESTS=30000 PRELOAD_REQUESTS=30000 PIPELINE=1 SIZE_RANGE=1-256 \
SERVER_CPU_SET=0 CLIENT_CPU_SET=1 \
GARNET_OWNER_THREAD_PINNING=1 GARNET_OWNER_THREAD_CPU_SET=0 GARNET_OWNER_EXECUTION_INLINE=1 \
./benches/linux_perf_diff_profile_median_local.sh
```

Output:

- `garnet-rs/benches/results/linux-perf-diff-median-20260223-065532/`

## Result

Median values:

- `garnet set`: `196325.12 ops/sec`, `p99=0.063 ms`
- `dragonfly set`: `86112.99 ops/sec`, `p99=0.087 ms`
- `garnet get`: `106203.99 ops/sec`, `p99=0.071 ms`
- `dragonfly get`: `92310.04 ops/sec`, `p99=0.087 ms`

Ratios:

- `dragonfly/garnet set = 0.439` (`garnet ≈ 2.28x`)
- `dragonfly/garnet get = 0.869` (`garnet ≈ 1.15x`)

## Notes

- This harness includes `perf` sampling overhead for both targets.
- Result is milestone-level for this specific shape (`1 thread`, `4 connections`, size `1-256`), not a blanket statement for all workloads.

## Artifacts

- `artifacts/profile-env.txt`
- `artifacts/summary.txt`
- `artifacts/median_summary.csv`
- `artifacts/runs.csv`
