# Experiment 13.01 - Owner Thread Count Sweep (owner=2 vs owner=1)

## Metadata

- experiment_id: `13.01`
- date: `2026-02-23`
- target: isolate owner-thread handoff overhead impact from hotspot data
- decision: **no default change yet** (mixed throughput at high concurrency)

## Goal

Check whether reducing owner-thread count from `2` to `1` improves throughput/p99 enough to justify changing defaults, given wake/scheduling dominance in Linux `perf`.

## Procedure

Used `garnet-rs/benches/perf_regression_gate_local.sh` with identical binary/settings except `OWNER_THREADS`:

- baseline: `OWNER_THREADS=2`
- candidate: `OWNER_THREADS=1`

Captured 3 cases:

1. `THREADS=1`, `CONNS=4`, `RUNS=5`, `REQUESTS=12000`, `SIZE_RANGE=1-256`
2. `THREADS=4`, `CONNS=8`, `RUNS=5`, `REQUESTS=5000`, `SIZE_RANGE=1-1024`
3. `THREADS=8`, `CONNS=16`, `RUNS=3`, `REQUESTS=5000`, `SIZE_RANGE=1-1024`

## Results

### 1 thread / 4 conns

Source: `artifacts/one_thread_4conns/comparison.txt`

- `SET ops`: `+3.01%` (`owner1` vs `owner2`)
- `GET ops`: `+0.26%`
- `SET p99`: `-7.77%`
- `GET p99`: `0.00%`

### 4 threads / 8 conns

Source: `artifacts/four_threads_8conns/comparison.txt`

- `SET ops`: `+1.77%`
- `GET ops`: `+2.55%`
- `SET p99`: `-6.02%`
- `GET p99`: `-6.84%`

### 8 threads / 16 conns

Source: `artifacts/eight_threads_16conns/comparison.txt`

- `SET ops`: `-0.73%`
- `GET ops`: `+0.36%`
- `SET p99`: `-3.00%`
- `GET p99`: `-16.14%`

## Decision

- We do **not** flip default owner-thread count from `2` to `1` based on this sweep alone.
- Reason: high-concurrency throughput is mixed (`SET` regresses at `8x16`), so a global default change is not yet evidence-safe.
- Follow-up should target architectural wake/handoff reduction directly rather than only thread-count tuning.
