# Experiment 11.398 - Hash Multi-Field Payload Fast Path (RT-001 Slice 2)

## Metadata

- experiment_id: `11.398`
- date: `2026-03-04`
- before_commit: `working-tree (11.397 binary)`
- after_commit: `working-tree (11.398 binary)`
- commit_message: `perf(hash): add multi-field payload fast path for HSET/HDEL without regressing single-field`
- full_diff: `docs/performance/experiments/2026-03-04/11.398-hash-multi-field-payload-fast-path/diff.patch`

## Goal

Expand `RT-001` hash fast path from single-field to multi-field mutation batches:

- `HSET key field value [field value ...]`
- `HDEL key field [field ...]`

while keeping no-expiration semantics and avoiding regression on the already-optimized single-field path from `11.397`.

## Implementation

- Extended hash fast path eligibility to all `HSET/HDEL` arities when per-key hash-field expiration metadata is absent.
- Added serialized-payload batch helpers in `value_codec`:
  - `upsert_hash_fields_payload_batch`
  - `delete_hash_fields_payload_batch`
- Batch helpers preserve sorted field order and Redis duplicate-field counting semantics (last-write-wins for `HSET`, deduplicated removals for `HDEL`).
- Added regression tests:
  - codec-level batch merge/delete behavior (including duplicates)
  - request-lifecycle duplicate-field command semantics
- Kept specialized single-field helpers and dispatch for `args.len()==4` (`HSET`) and `args.len()==3` (`HDEL`) after first benchmark attempt showed regression with a naive batch-only path.

## Validation

- `make fmt`
- `make test-server`
  - pass summary: `386 + 23 + 1`, failed: `0`

## Benchmark Setup

Runner: `tools/object_mutation_ab_local.sh`

Fixed params:

- `RUNS=3`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=8000`
- `PRELOAD_REQUESTS=4096`
- `PIPELINE=1`, `SIZE_RANGE=16-128`
- `KEY_MAX=4096`

Binaries:

- before (`11.397`): `/tmp/garnet-new-rt001-s1-20260304-181252`
- after (`11.398`): `/tmp/garnet-new-rt001-s2-20260304-184836-opt`

## Results

### Cross-binary attempt 1 (batch-only path, rejected)

Source: `artifacts/cross_binary_attempt1_regressed/comparison.txt`

- `HSET ops`: `-15.84%`
- `HDEL ops`: `-15.84%`
- `HSET p99`: `+29.09%`
- `HDEL p99`: `+18.50%`

Decision: rejected; single-field specialized path restored.

### Cross-binary attempt 2 (after single-path restore, noisy)

Source: `artifacts/cross_binary_attempt2_outlier/comparison.txt`

- hash returned near-neutral/improved (`ops -0.35%`, p99 improved)
- non-target list p99 showed a large outlier

Decision: rerun for stable read.

### Cross-binary final (accepted)

Source: `artifacts/cross_binary_final/comparison.txt`

- hash:
  - `HSET ops`: `+0.24%`
  - `HDEL ops`: `+0.24%`
  - `HSET p99`: `-7.48%`
  - `HDEL p99`: `-4.49%`
- non-target families:
  - ops within `-1.12%` to `-0.26%`
  - p99 mostly small movement; `ZADD p99 +19.70%` observed in this sample

### Same-binary noise (after)

Source: `artifacts/noise_after/comparison.txt`

- confirms notable p99 noise on this local host during the run window (for example `list_lpush_p99 +15.80%` without code change)

## Interpretation

`11.398` is accepted with the final cross-binary run:

- target family (`hash`) is non-regressing on throughput and improved on p99
- multi-field fast path is now enabled
- single-field hot path remains protected from the batch-path regression seen in attempt 1

## Artifacts

- `tools/object_mutation_ab_local.sh`
- `artifacts/cross_binary_attempt1_regressed/comparison.txt`
- `artifacts/cross_binary_attempt2_outlier/comparison.txt`
- `artifacts/cross_binary_final/comparison.txt`
- `artifacts/noise_after/comparison.txt`
- `diff.patch`
