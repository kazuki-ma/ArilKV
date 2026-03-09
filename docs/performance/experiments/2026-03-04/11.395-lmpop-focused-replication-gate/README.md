# Experiment 11.395 - LMPOP-Focused Replication Acceptance Gate

## Metadata

- experiment_id: `11.395`
- date: `2026-03-04`
- before_commit: `working-tree (11.391 binary)`
- after_commit: `working-tree (11.393 binary)`
- commit_message: `perf(server): LMPOP-focused replication benchmark gate for 11.393`
- full_diff: `docs/performance/experiments/2026-03-04/11.395-lmpop-focused-replication-gate/diff.patch` (benchmark-only; no code diff)

## Goal

Close follow-up `11.395` by benchmarking the exact LMPOP replication path touched
in `11.393`, then decide keep/revert using path-aligned metrics instead of generic
SET/GET mixed workload noise.

## Setup

- topology: primary + replica (`REPLICAOF`)
- runner: `tools/lmpop_replication_ab_local.sh`
- binaries:
  - before: `/tmp/garnet-new-rt002-s2b-20260304-135626`
  - after: `/tmp/garnet-new-rt002-s3-20260304-142147`
- workload:
  - custom memtier commands:
    - `LPUSH benchlist __data__`
    - `LMPOP 1 benchlist LEFT COUNT 1`
  - command ratio: `LPUSH:LMPOP = 1:1`
  - replica-health guard: per-run replication probe (`SET` on primary, `GET` on replica)
- parameters:
  - `RUNS=7`
  - `THREADS=1`, `CONNS=2`
  - `REQUESTS=20000`
  - `PIPELINE=1`, `SIZE_RANGE=1-1024`
- noise controls:
  - `GARNET_OWNER_THREAD_PINNING=1`
  - `GARNET_OWNER_THREAD_CPU_SET=0`
  - same-binary controls for before and after

## Results

### Cross-binary (11.391 -> 11.393)

Source: `artifacts/cross_binary/comparison.txt`

- `Lmpop ops`: `+0.20%`
- `Lmpop p99`: `+3.59%`

### Same-binary noise (before)

Source: `artifacts/noise_base/comparison.txt`

- `Lmpop ops`: `+0.63%`
- `Lmpop p99`: `+5.03%`

### Same-binary noise (after)

Source: `artifacts/noise_new/comparison.txt`

- `Lmpop ops`: `+2.24%`
- `Lmpop p99`: `-32.47%`

## Interpretation

Cross-binary LMPOP deltas are small and remain inside the observed same-binary
noise envelope. This path-aligned gate does not show a confirmed regression from
`11.393`, so keep `11.393` as accepted for `RT-002`.

Note: initial runner attempt used `LPUSH:LMPOP = 2:1` and failed with list-object
`RecordTooLarge` under long runs. The final accepted run uses balanced `1:1` ratio
for stable list size and reproducible completion.

## Artifacts

- `tools/lmpop_replication_ab_local.sh`
- `artifacts/cross_binary/comparison.txt`
- `artifacts/cross_binary/before_11391/summary.txt`
- `artifacts/cross_binary/before_11391/runs.csv`
- `artifacts/cross_binary/after_11393/summary.txt`
- `artifacts/cross_binary/after_11393/runs.csv`
- `artifacts/noise_base/comparison.txt`
- `artifacts/noise_new/comparison.txt`
- `diff.patch`
