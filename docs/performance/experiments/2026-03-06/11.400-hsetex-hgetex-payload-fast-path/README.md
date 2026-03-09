# Experiment 11.400 - HSETEX/HGETEX Payload Fast Path (RT-001 Slice 4)

## Metadata

- experiment_id: `11.400`
- date: `2026-03-06`
- before_commit: `working-tree (11.399 binary)`
- after_commit: `working-tree (11.400 binary)`
- commit_message: `perf(hash): add HSETEX/HGETEX payload fast paths without hash-field-ttl metadata`
- full_diff: `docs/performance/experiments/2026-03-06/11.400-hsetex-hgetex-payload-fast-path/diff.patch`

## Goal

Extend hash serialized-payload fast path coverage to:

- `HSETEX key [PX|PXAT] FIELDS ...`
- `HGETEX key [PX|PXAT] FIELDS ...`

for the no-hash-field-expiration-metadata case, preserving:

- duplicate field semantics,
- immediate-expire (`PXAT` past) behavior,
- response ordering/shape parity.

## Implementation

- `hash_commands`:
  - Added no-hash-field-TTL fast-path dispatch for `HSETEX`/`HGETEX`.
  - Added `handle_hsetex_batch_fast_path` and `handle_hgetex_batch_fast_path`.
  - Preserved `PXAT` past immediate-expire behavior by applying payload delete path after response/value selection.
- `value_codec`:
  - Added `get_hash_fields_payload_batch` helper (ordered values, duplicate request support).
  - Added regression test for duplicate field retrieval order.
- command tests:
  - Added `hsetex_hgetex_fast_paths_preserve_duplicate_and_immediate_expire_semantics`.

## Validation

- `make fmt`
- `make test-server`
  - pass summary: `397 + 23 + 1`, failed: `0`
- command coverage workflow:
  - `cd garnet-rs/tests/interop && REDIS_REPO_ROOT=... RUNTEXT_TIMEOUT_SECONDS=15 RUNTEXT_WALL_TIMEOUT_SECONDS=120 ./build_compatibility_report.sh`
  - completed `[4/4]`; external probe summary remained `ok=612 err=10 timeout=1 ignore=129`, CLI probes PASS.

## Benchmark Setup

### A. Touched-path gate (`HSETEX/HGETEX`)

Runner: `tools/hsetex_hgetex_ab_local.sh`

Fixed params:

- `RUNS=3`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=12000`
- `PIPELINE=1`, `SIZE_RANGE=16-128`

Workload pair:

- `HSETEX objhash PX 60000 FIELDS 2 f1 __data__ f2 __data__`
- `HGETEX objhash FIELDS 2 f1 f2`

Binaries:

- before (`11.399`): `/tmp/garnet-server-11.400-before`
- after (`11.400`): `/tmp/garnet-server-11.400-after`

### B. Guard gate (object mutation suite)

Runner: `tools/object_mutation_ab_local.sh`

Fixed params:

- `RUNS=5`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=8000`, `PRELOAD_REQUESTS=4096`
- `KEY_MAX=4096`, `PIPELINE=1`, `SIZE_RANGE=16-128`

## Results

### A. Cross-binary (`HSETEX/HGETEX`)

Source: `artifacts/cross_binary/comparison.txt`

- `HSETEX ops`: `+3.53%`
- `HGETEX ops`: `+3.53%`
- `HSETEX p99`: `-26.17%`
- `HGETEX p99`: `-71.24%`

### A. Same-binary noise (after)

Source: `artifacts/noise_after/comparison.txt`

- `ops` noise around `-0.66%`
- `p99` variance remained large in this environment (`-32%` to `-38%`)

Interpretation: touched-path throughput improved with clear non-regressing signal; p99 is favorable in this run.

### B. Guard gate (`RUNS=5`)

Cross-binary source: `artifacts/object_mutation_guard_r5/comparison.txt`  
Same-binary source: `artifacts/object_mutation_noise_after_r5/comparison.txt`

- hash mutation throughput remained near-neutral (`HSET/HDEL ops -0.23%` cross-binary).
- object-family throughput stayed within about `-0.90%` to `+0.62%`.
- p99 variance on non-target paths is high in both cross-binary and same-binary runs (e.g., `HDEL +31.47%` cross vs `+25.24%` same-binary; `SREM +102.78%` cross vs `+27.37%` same-binary).

Interpretation: no clear throughput regression signal on guard workloads; p99 variance remains noisy and requires continued monitoring in subsequent RT-001 slices.

## Decision

Keep `11.400`.

- `HSETEX/HGETEX` payload fast path is enabled for no-hash-field-TTL keys.
- duplicate and immediate-expire semantics are covered by tests.
- touched path improves; guard throughput remains near-neutral under noisy latency conditions.

## Artifacts

- `tools/hsetex_hgetex_ab_local.sh`
- `tools/object_mutation_ab_local.sh`
- `artifacts/cross_binary/comparison.txt`
- `artifacts/noise_after/comparison.txt`
- `artifacts/object_mutation_guard/comparison.txt`
- `artifacts/object_mutation_noise_after/comparison.txt`
- `artifacts/object_mutation_guard_r5/comparison.txt`
- `artifacts/object_mutation_noise_after_r5/comparison.txt`
- `diff.patch`
