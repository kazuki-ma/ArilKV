# 11.426 replybuf rbs tracking benchmark

## Purpose

Smoke-check that wiring live reply-buffer bookkeeping into `ServerMetrics` and the connection output path does not introduce an obvious throughput or p99 regression.

## Scope

- Before: `4c2a0e1b90`
- After: working tree for iteration `623` (pre-commit)
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters:
  - `RUNS=1`
  - `THREADS=1`
  - `CONNS=4`
  - `REQUESTS=2000`
  - `PRELOAD_REQUESTS=2000`
  - `PIPELINE=1`
  - `SIZE_RANGE=1-64`

## Result

This was a smoke run, not a noise-resistant benchmark. The observed direction is non-regressing:

- SET ops/sec: `61204 -> 68734` (`+12.30%`)
- GET ops/sec: `56462 -> 62153` (`+10.08%`)
- SET p99: `0.167ms -> 0.103ms` (`-38.32%`)
- GET p99: `0.191ms -> 0.151ms` (`-20.94%`)

Interpretation: the reply-buffer tracking change does not show a local regression in this smoke sample. Because `RUNS=1`, treat this only as a guardrail check, not as a claim of material improvement.

## Artifacts

- comparison: `artifacts/binary-ab/comparison.txt`
- per-binary summaries:
  - `artifacts/binary-ab/before-4c2a0e1b90/summary.txt`
  - `artifacts/binary-ab/after-working-tree/summary.txt`
- per-run logs:
  - `artifacts/binary-ab/*/run-1/`
- patch: `diff.patch`
