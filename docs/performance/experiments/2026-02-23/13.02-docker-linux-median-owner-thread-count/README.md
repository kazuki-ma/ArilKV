# Experiment 13.02 - Docker Linux Median Sweep for Owner Thread Count

## Metadata

- experiment_id: `13.02`
- date: `2026-02-23`
- objective: validate `OWNER_THREADS=1` vs `OWNER_THREADS=2` under Linux Docker median-of-3
- decision: **keep default unchanged**

## Why This Experiment

Local sweeps (`13.01`) showed mixed but sometimes favorable results for `OWNER_THREADS=1`.
To avoid host-specific bias, we re-ran on the Dockerized Linux median harness.

## Procedure

Script:

- `garnet-rs/benches/linux_perf_diff_profile_median_local.sh`
- wrapped Docker differential profile (`RUNS=3`)

Common parameters:

- `THREADS=1`
- `CONNS=4`
- `REQUESTS=12000`
- `PRELOAD_REQUESTS=12000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`

Compared:

- baseline: `GARNET_STRING_OWNER_THREADS=2`
- candidate: `GARNET_STRING_OWNER_THREADS=1`

## Results

Source: `artifacts/comparison.txt`

- `SET median ops`: `+0.02%` (`owner1` vs `owner2`)
- `GET median ops`: `-0.53%`
- `SET p99`: `0.00%`
- `GET p99`: `0.00%`

The effect is effectively neutral/noise-level and does not support a default flip.

## Artifacts

- `artifacts/owner2/summary.txt`
- `artifacts/owner2/median_summary.csv`
- `artifacts/owner2/runs.csv`
- `artifacts/owner1/summary.txt`
- `artifacts/owner1/median_summary.csv`
- `artifacts/owner1/runs.csv`
- `artifacts/comparison.txt`
