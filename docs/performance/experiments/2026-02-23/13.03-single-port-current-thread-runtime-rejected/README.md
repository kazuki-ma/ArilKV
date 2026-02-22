# Experiment 13.03 - Single-Port `current_thread` Runtime (Rejected)

## Metadata

- experiment_id: `13.03`
- date: `2026-02-23`
- before: single-port path on Tokio `multi_thread` runtime
- candidate: switch single-port path to Tokio `current_thread`
- outcome: **rejected** (measured regression)

## Goal

Test whether reducing Tokio scheduler complexity on single-port mode lowers wake/scheduling overhead seen in Linux `perf`.

## Change Under Test

Patch: `diff.patch`

Summary:

- Replace async `#[tokio::main(flavor = "multi_thread")]` entrypoint with sync `main`.
- For single-port path, build a `current_thread` runtime and `block_on(run/run_with_cluster)`.

## Benchmark Procedure

Script:

- `garnet-rs/benches/binary_ab_local.sh`

Parameters:

- `RUNS=5`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=12000`
- `PRELOAD_REQUESTS=12000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`

## Results

Source: `artifacts/main_runtime_ab_1t/comparison.txt`

- `SET median ops`: `-5.38%`
- `GET median ops`: `-1.34%`
- `SET p99`: `+15.53%` (worse)
- `GET p99`: `0.00%`

## Decision

- Reject this runtime change.
- Keep existing `multi_thread` main runtime for single-port path.

## Artifacts

- `artifacts/main_runtime_ab_1t/comparison.txt`
- `artifacts/main_runtime_ab_1t/main_multithread/summary.txt`
- `artifacts/main_runtime_ab_1t/main_multithread/runs.csv`
- `artifacts/main_runtime_ab_1t/main_current_thread/summary.txt`
- `artifacts/main_runtime_ab_1t/main_current_thread/runs.csv`
- `diff.patch`
