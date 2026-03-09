# Experiment 11.392 - Replication-Heavy Re-Benchmark for RT-002 Slice 2

## Metadata

- experiment_id: `11.392`
- date: `2026-03-04`
- before_commit: `0fa2aeac3c`
- after_commit: `working-tree (11.391 applied)`
- commit_message: `perf(server): validate RT-002 slice 2 under replication-heavy load`
- full_diff: `docs/performance/experiments/2026-03-04/11.392-replication-heavy-rebenchmark-rt002-s2/diff.patch`

## Goal

Close follow-up `11.392` by validating whether the mixed p99 signal seen in
single-node `11.391` persists when the workload actually exercises the
replication-frame rewrite path (primary with active replica).

## Procedure

- Same binaries used in `11.391`:
  - before: `/tmp/garnet-baseline-rt002-s2-20260304-134857`
  - after: `/tmp/garnet-new-rt002-s2b-20260304-135626`
- Topology per run:
  - primary on `127.0.0.1:<PRIMARY_PORT>`
  - replica on `127.0.0.1:<REPLICA_PORT>`
  - `REPLICAOF` from replica to primary
  - probe key must replicate before measurement
- Workloads (`memtier_benchmark`, same parameters for before/after):
  - write-heavy: `--ratio 1:0`
  - mixed: `--ratio 1:1`
- Fixed parameters:
  - `RUNS=3`, `THREADS=4`, `CONNS=8`, `REQUESTS=5000`, `PIPELINE=1`, `SIZE_RANGE=1-1024`
- Runner script: `tools/replication_ab_local.sh`

## Results

### Run 1

Source: `artifacts/run1/comparison.txt`

- write-heavy:
  - `SET ops`: `+16.64%`
  - `SET p99`: `-22.49%`
- mixed:
  - `SET ops`: `+10.01%`
  - `GET ops`: `+10.01%`
  - `SET p99`: `-26.43%`
  - `GET p99`: `-27.45%`

### Run 2 (rerun)

Source: `artifacts/run2/comparison.txt`

- write-heavy:
  - `SET ops`: `+8.42%`
  - `SET p99`: `-21.67%`
- mixed:
  - `SET ops`: `+9.14%`
  - `GET ops`: `+9.14%`
  - `SET p99`: `-21.03%`
  - `GET p99`: `-21.46%`

## Interpretation

Under replication-heavy conditions, `11.391` is consistently positive across
both throughput and p99 in two independent `RUNS=3` samples. The earlier
single-node mixed p99 concern is not reproduced on the replication path that
`RT-002` slice 2 targets.

Decision: mark `11.392` as complete and continue `RT-002` backlog on the next
allocation hot spots.

## Artifacts

- `tools/replication_ab_local.sh`
- `artifacts/run1/comparison.txt`
- `artifacts/run1/before_11390/summary.txt`
- `artifacts/run1/before_11390/runs.csv`
- `artifacts/run1/after_11391/summary.txt`
- `artifacts/run1/after_11391/runs.csv`
- `artifacts/run2/comparison.txt`
- `artifacts/run2/before_11390/summary.txt`
- `artifacts/run2/before_11390/runs.csv`
- `artifacts/run2/after_11391/summary.txt`
- `artifacts/run2/after_11391/runs.csv`
- `diff.patch`
