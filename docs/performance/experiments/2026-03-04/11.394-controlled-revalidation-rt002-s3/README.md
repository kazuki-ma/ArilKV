# Experiment 11.394 - Controlled Revalidation for 11.393

## Metadata

- experiment_id: `11.394`
- date: `2026-03-04`
- before_commit: `working-tree (11.391 binary)`
- after_commit: `working-tree (11.393 binary)`
- commit_message: `perf(server): controlled-noise revalidation for 11.393`
- full_diff: `docs/performance/experiments/2026-03-04/11.394-controlled-revalidation-rt002-s3/diff.patch` (benchmark-only; no code diff)

## Goal

Close follow-up `11.394` by re-validating `11.393` under lower-noise conditions,
then compare cross-binary deltas against same-binary noise bounds.

## Controlled Setup

- topology: primary + replica (`REPLICAOF`)
- runner: `tools/replication_ab_local.sh`
- binaries:
  - before: `/tmp/garnet-new-rt002-s2b-20260304-135626`
  - after: `/tmp/garnet-new-rt002-s3-20260304-142147`
- parameters:
  - `RUNS=7`
  - `THREADS=1`, `CONNS=2`
  - `REQUESTS=20000`
  - `PIPELINE=1`, `SIZE_RANGE=1-1024`
- noise controls:
  - `GARNET_OWNER_THREAD_PINNING=1`
  - `GARNET_OWNER_THREAD_CPU_SET=0`
  - same-binary control runs for before and after

## Results

### Cross-binary (11.391 -> 11.393)

Source: `artifacts/cross_binary/comparison.txt`

- write-heavy:
  - `SET ops`: `-0.52%`
  - `SET p99`: `0.00%`
- mixed:
  - `SET/GET ops`: `-3.17%`
  - `SET p99`: `+12.70%`
  - `GET p99`: `+14.55%`

### Same-binary noise (before)

Source: `artifacts/noise_base/comparison.txt`

- mixed:
  - `ops`: `+0.17%`
  - `p99`: `0.00%`

### Same-binary noise (after)

Source: `artifacts/noise_new/comparison.txt`

- mixed:
  - `ops`: `+3.12%`
  - `SET p99`: `0.00%`
  - `GET p99`: `+14.55%`

## Interpretation

Controlled settings reduce variance versus prior runs, but mixed-path deltas still
partly overlap same-binary noise envelopes (notably GET p99).

Decision for `11.394`: treat `11.393` as **kept with caution** (no confirmed
path-specific regression), and add `11.395` to benchmark the actually modified
path (`LMPOP` replication rewrite) directly instead of relying on generic
SET/GET mixed workload.

## Artifacts

- `tools/replication_ab_local.sh`
- `artifacts/cross_binary/comparison.txt`
- `artifacts/cross_binary/before_11391/summary.txt`
- `artifacts/cross_binary/before_11391/runs.csv`
- `artifacts/cross_binary/after_11393/summary.txt`
- `artifacts/cross_binary/after_11393/runs.csv`
- `artifacts/noise_base/comparison.txt`
- `artifacts/noise_new/comparison.txt`
- `diff.patch`
