# Experiment 13.04 - Owner Result Channel Mode (`sync_channel` vs `channel`) Rejected

## Metadata

- experiment_id: `13.04`
- date: `2026-02-23`
- candidate: replace per-request result return channel from `mpsc::sync_channel(1)` to `mpsc::channel()`
- outcome: **rejected** (regression under higher concurrency)

## Goal

Check whether a non-blocking return channel lowers owner-thread handoff overhead and improves throughput.

## Change Under Test

Patch: `diff.patch`

## Benchmark Matrix

Compared binaries with `garnet-rs/benches/binary_ab_local.sh`:

1. `RUNS=5`, `THREADS=1`, `CONNS=4`, `REQUESTS=12000`, `SIZE_RANGE=1-256`
2. `RUNS=5`, `THREADS=4`, `CONNS=8`, `REQUESTS=5000`, `SIZE_RANGE=1-1024`
3. `RUNS=5`, `THREADS=8`, `CONNS=16`, `REQUESTS=5000`, `SIZE_RANGE=1-1024`

## Results

### 1 thread / 4 conns

`artifacts/one_thread_4conns_runs5/comparison.txt`

- `SET ops`: `+2.75%`
- `GET ops`: `+0.91%`

### 4 threads / 8 conns

`artifacts/four_threads_8conns_runs5/comparison.txt`

- `SET ops`: `+0.13%`
- `GET ops`: `-0.86%`

### 8 threads / 16 conns (deciding run)

`artifacts/eight_threads_16conns_runs5/comparison.txt`

- `SET ops`: `-1.62%`
- `GET ops`: `-0.44%`
- `SET p99`: `+2.08%` (worse)

## Decision

- Reject this change.
- Keep current `sync_channel(1)` implementation.
- Reason: gains at light load do not hold at higher concurrency, where throughput regresses.
