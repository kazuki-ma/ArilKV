# Experiment 11.397 - Hash Single-Field Payload Fast Path (RT-001 Slice 1)

## Metadata

- experiment_id: `11.397`
- date: `2026-03-04`
- before_commit: `working-tree (pre-11.397 binary)`
- after_commit: `working-tree (11.397 binary)`
- commit_message: `perf(hash): add single-field payload fast path for HSET/HDEL`
- full_diff: `docs/performance/experiments/2026-03-04/11.397-hash-single-field-payload-fast-path/diff.patch`

## Goal

Execute first `RT-001` redesign slice on hash mutation path and validate with the
`11.396` object-mutation gate.

## Implementation

- Added single-field fast path for hash mutations:
  - `HSET key field value` (single pair)
  - `HDEL key field` (single field)
- Fast path condition:
  - only when per-key hash-field expiration metadata is absent
- Fast path behavior:
  - operate directly on serialized hash payload bytes (`count + entries`) without
    materializing full `BTreeMap`
  - preserve sorted field order to keep existing response order semantics
  - preserve wrong-type handling, keyspace notifications, and delete-on-empty
- Added codec-level tests for serialized hash payload upsert/delete helpers.

## Validation

- `make fmt`
- `make test-server`
  - pass summary: `383 + 23 + 1`, failed: `0`

## Benchmark Setup

Runner: `tools/object_mutation_ab_local.sh` (copied from `11.396` harness)

Fixed params:

- `RUNS=3`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=8000`
- `PRELOAD_REQUESTS=4096`
- `PIPELINE=1`, `SIZE_RANGE=16-128`
- `KEY_MAX=4096`

Binaries:

- before: `/tmp/garnet-new-rt002-s3-20260304-142147`
- after: `/tmp/garnet-new-rt001-s1-20260304-181252`

## Results

### Cross-binary (`before_11396` -> `after_11397`)

Source: `artifacts/cross_binary/comparison.txt`

- hash:
  - `HSET ops`: `+307.26%`
  - `HDEL ops`: `+307.26%`
  - `HSET p99`: `-81.58%`
  - `HDEL p99`: `-79.39%`
- non-target families (list/set/zset): near-neutral to small noise-scale movement
  - ops roughly within `-1.44%` to `+0.96%`

### Same-binary noise (after)

Source: `artifacts/noise_after/comparison.txt`

- hash noise reference:
  - `HSET/HDEL ops`: `+1.47%`
  - `HSET p99`: `-4.70%`
  - `HDEL p99`: `+14.55%`

## Interpretation

`11.396` gate target (`>= +8%` on touched-family throughput, with noise band `+/-3%`)
is decisively passed on hash mutation path. Observed hash improvements are far
outside same-binary noise and non-target families remain near baseline.

Decision: keep `11.397` and continue RT-001 on remaining object paths.

## Artifacts

- `tools/object_mutation_ab_local.sh`
- `artifacts/cross_binary/comparison.txt`
- `artifacts/cross_binary/before_11396/summary.txt`
- `artifacts/cross_binary/before_11396/runs.csv`
- `artifacts/cross_binary/after_11397/summary.txt`
- `artifacts/cross_binary/after_11397/runs.csv`
- `artifacts/noise_after/comparison.txt`
- `diff.patch`
