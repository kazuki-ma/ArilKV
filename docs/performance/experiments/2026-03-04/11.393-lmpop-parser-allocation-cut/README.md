# Experiment 11.393 - LMPOP Replication Parser Allocation Cut

## Metadata

- experiment_id: `11.393`
- date: `2026-03-04`
- before_commit: `working-tree (11.391)`
- after_commit: `working-tree (11.393)`
- commit_message: `perf(server): remove temporary Vec allocation in LMPOP replication response parse`
- full_diff: `docs/performance/experiments/2026-03-04/11.393-lmpop-parser-allocation-cut/diff.patch`

## Goal

Continue `RT-002` by removing remaining parser-side transient allocation in
LMPOP replication rewrite parsing:

- `parse_resp_bulk_string`: `Vec<u8>` -> borrowed `&[u8]`
- `parse_lmpop_replication_meta`: owned key -> borrowed key

## Code Change Summary

- Converted `ParsedBulkString` and `LmpopReplicationMeta` to lifetime-bound
  borrowed views over `frame_response`.
- Removed `to_vec()` in RESP bulk parsing path used by LMPOP/BLMPOP rewrite.
- Added regression test `replication_rewrites_lmpop_to_pop_with_count`.

## Validation

- `make fmt`
- `cargo test -p garnet-server --lib replication_rewrites_lmpop_to_pop_with_count -- --nocapture`
- `cargo test -p garnet-server --lib replication_rewrites_ -- --nocapture`
- `make test-server` (`381 + 23 + 1` pass)

## Benchmark Procedure

Used replication-heavy harness from `11.392`:

- runner: `tools/replication_ab_local.sh`
- topology: primary + replica (`REPLICAOF`)
- workload: write-heavy (`1:0`) and mixed (`1:1`)
- parameters: `RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-1024`

Compared binaries:

- before: `/tmp/garnet-new-rt002-s2b-20260304-135626` (11.391)
- after: `/tmp/garnet-new-rt002-s3-20260304-142147` (11.393)

## Results

### A/B run 1

Source: `artifacts/ab_run1/comparison.txt`

- write-heavy: `SET ops +2.53%`, `SET p99 +2.78%`
- mixed: `SET ops -5.85%`, `GET ops -5.85%`, `SET p99 +31.63%`, `GET p99 +33.39%`

### A/B run 2 (rerun)

Source: `artifacts/ab_run2/comparison.txt`

- write-heavy: `SET ops +6.61%`, `SET p99 -8.95%`
- mixed: `SET ops -8.56%`, `GET ops -8.56%`, `SET p99 +32.00%`, `GET p99 +28.22%`

### Noise checks (same-binary)

- base same-binary (`artifacts/noise_base/comparison.txt`): mixed metrics move by
  up to ~`15%` p99 and ~`0.7%` ops.
- new same-binary (`artifacts/noise_new/comparison.txt`): mixed metrics move by
  ~`-11.67%` ops and `+30%` p99 without code changes.

Interpretation: local mixed-path variance is high in current environment; the
observed mixed regression for `11.393` is not separable from same-binary noise.
Given scope is narrow (LMPOP rewrite parser path only) and correctness tests pass,
keep the allocation-cut change with explicit follow-up for controlled perf
revalidation in a quieter/pinned environment.

## Artifacts

- `tools/replication_ab_local.sh`
- `artifacts/ab_run1/comparison.txt`
- `artifacts/ab_run1/before_11391/summary.txt`
- `artifacts/ab_run1/before_11391/runs.csv`
- `artifacts/ab_run1/after_11393/summary.txt`
- `artifacts/ab_run1/after_11393/runs.csv`
- `artifacts/ab_run2/comparison.txt`
- `artifacts/ab_run2/before_11391/summary.txt`
- `artifacts/ab_run2/before_11391/runs.csv`
- `artifacts/ab_run2/after_11393/summary.txt`
- `artifacts/ab_run2/after_11393/runs.csv`
- `artifacts/noise_base/comparison.txt`
- `artifacts/noise_new/comparison.txt`
- `diff.patch`
