# Experiment 11.399 - HMSET/HGETDEL Payload Fast Path (RT-001 Slice 3)

## Metadata

- experiment_id: `11.399`
- date: `2026-03-04`
- before_commit: `working-tree (11.398 binary)`
- after_commit: `working-tree (11.399 binary)`
- commit_message: `perf(hash): add HMSET/HGETDEL payload fast paths with duplicate-field parity`
- full_diff: `docs/performance/experiments/2026-03-04/11.399-hmset-hgetdel-payload-fast-path/diff.patch`

## Goal

Extend hash serialized-payload fast path coverage from `HSET/HDEL` (`11.398`) to the remaining multi-field mutation commands:

- `HMSET key field value [field value ...]`
- `HGETDEL key FIELDS num field [field ...]`

under the same safety condition:

- no per-key hash-field expiration metadata for target key

while preserving duplicate-field/order semantics and avoiding regression on previously optimized `HSET/HDEL` paths.

## Implementation

- Added fast-path dispatch in `hash_commands`:
  - `handle_hmset` -> `handle_hmset_batch_fast_path` when no hash-field TTL metadata
  - `handle_hgetdel` -> `handle_hgetdel_batch_fast_path` when no hash-field TTL metadata
- Added codec helper in `value_codec`:
  - `get_and_delete_hash_fields_payload_batch`
  - returns ordered per-request values (`Some/None`) plus serialized payload after deduplicated deletion
- Preserved duplicate semantics:
  - `HMSET`: last-write-wins in same command
  - `HGETDEL`: first occurrence returns value, duplicate requests for same field return null
- Added tests:
  - `hmset_and_hgetdel_multi_field_paths_preserve_duplicate_and_order_semantics`
  - `get_and_delete_hash_fields_payload_batch_returns_ordered_values_and_updates_payload`

## Validation

- `make fmt`
- `make test-server`
  - pass summary: `388 + 23 + 1`, failed: `0`
- command-coverage workflow:
  - `build_compatibility_report.sh` reached step `[3/4]` and again stalled at
    `unit/pause` heartbeat (`elapsed=330s ok=612 err=10 ignore=129 timeout=0`)
    before manual stop
  - step `[1/4]` and `[2/4]` matrix artifacts were regenerated

## Benchmark Setup

### A. Touched-path gate (`HMSET/HGETDEL`)

Runner: `tools/hmset_hgetdel_ab_local.sh`

Fixed params:

- `RUNS=3`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=12000`
- `PIPELINE=1`, `SIZE_RANGE=16-128`

Workload pair:

- `HMSET objhash f1 __data__ f2 __data__`
- `HGETDEL objhash FIELDS 2 f1 f2`

Binaries:

- before (`11.398`): `/tmp/garnet-new-rt001-s2-20260304-184836-opt`
- after (`11.399`): `/tmp/garnet-new-rt001-s3-20260304-221006`

### B. Guard gate (`HSET/HDEL` regression check)

Runner: `11.398` object mutation harness
`docs/performance/experiments/2026-03-04/11.398-hash-multi-field-payload-fast-path/tools/object_mutation_ab_local.sh`

Fixed params: same as `11.398` (`RUNS=3`, `REQUESTS=8000`, `PRELOAD_REQUESTS=4096`, `THREADS=1`, `CONNS=2`, `KEY_MAX=4096`).

## Results

### A. Cross-binary (`HMSET/HGETDEL`)

Source: `artifacts/cross_binary/comparison.txt`

- `HMSET ops`: `+0.56%`
- `HGETDEL ops`: `+0.56%`
- `HMSET p99`: `0.00%`
- `HGETDEL p99`: `0.00%`

### A. Same-binary noise (after)

Source: `artifacts/noise_after/comparison.txt`

- ops noise observed around `+2.47%`
- p99 noise substantially larger in this environment (`-18%` to `-45%`)

Interpretation: touched-path throughput is non-regressing; observed cross-binary movement is within local noise band.

### B. Guard gate (`HSET/HDEL`)

Source: `artifacts/object_mutation_guard/comparison.txt`

- `HSET/HDEL ops`: `-0.98%`
- `HSET p99`: `-6.26%`
- `HDEL p99`: `0.00%`

Interpretation: no material regression on previously optimized hash mutation path.

## Decision

Keep `11.399`.

- `HMSET/HGETDEL` fast path is enabled for no-hash-field-TTL keys
- duplicate/order semantics are covered by tests
- touched path is non-regressing within measured noise
- `HSET/HDEL` prior gains remain intact within noise

## Artifacts

- `tools/hmset_hgetdel_ab_local.sh`
- `artifacts/cross_binary/comparison.txt`
- `artifacts/noise_after/comparison.txt`
- `artifacts/object_mutation_guard/comparison.txt`
- `diff.patch`
