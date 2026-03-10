# 11.433 list-3 object page-size smoke benchmark

## Purpose

Smoke-check that raising the object-store page-size floor for `R-009` does not introduce an obvious server-wide throughput or tail-latency regression.

## Scope

- Before: `fb37d1a646`
- After: working tree for iteration `630` (pre-commit)
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

This was a smoke run, not a noise-resistant benchmark. The change affects the default object-store page size under the current monolithic object-value model, so the benchmark is a guardrail against broad fallout rather than a claim about hot-path improvement.

- SET ops/sec: `67307 -> 66461` (`-1.26%`)
- GET ops/sec: `63353 -> 65950` (`+4.10%`)
- SET p99: `0.119ms -> 0.119ms` (`+0.00%`)
- GET p99: `0.167ms -> 0.127ms` (`-23.95%`)

Interpretation: `RUNS=1` is too small to claim a real delta either way. The smoke sample does not show an obvious server-wide regression from the larger object page-size floor.

## Artifacts

- comparison: `artifacts/binary_ab_smoke/comparison.txt`
- per-binary summaries:
  - `artifacts/binary_ab_smoke/base-fb37d1a646/summary.txt`
  - `artifacts/binary_ab_smoke/worktree-r009/summary.txt`
- per-run csv:
  - `artifacts/binary_ab_smoke/base-fb37d1a646/runs.csv`
  - `artifacts/binary_ab_smoke/worktree-r009/runs.csv`
- patch: `diff.patch`
