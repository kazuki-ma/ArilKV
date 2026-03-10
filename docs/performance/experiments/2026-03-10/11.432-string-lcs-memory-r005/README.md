# 11.432 string LCS and memory usage smoke benchmark

## Purpose

Smoke-check that the `R-005` fixes for `LCS IDX` decomposition and `MEMORY USAGE - STRINGS` do not introduce an obvious server-wide throughput or tail-latency regression.

## Scope

- Before: `6467db7f54`
- After: working tree for iteration `629` (pre-commit)
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

This was a smoke run, not a noise-resistant benchmark. The code paths changed here are `LCS` and string memory introspection, so the benchmark is only a guardrail against accidental hot-path fallout.

- SET ops/sec: `65117 -> 66242` (`+1.73%`)
- GET ops/sec: `71574 -> 65609` (`-8.33%`)
- SET p99: `0.119ms -> 0.111ms` (`-6.72%`)
- GET p99: `0.103ms -> 0.135ms` (`+31.07%`)

Interpretation: `RUNS=1` is too small to claim a real delta. There is no obvious catastrophic regression, but the GET tail increase should be treated as noise until reproduced with more runs if this area expands further.

## Artifacts

- comparison: `artifacts/binary_ab_smoke/comparison.txt`
- per-binary summaries:
  - `artifacts/binary_ab_smoke/base-6467db7f54/summary.txt`
  - `artifacts/binary_ab_smoke/worktree-r005/summary.txt`
- per-run csv:
  - `artifacts/binary_ab_smoke/base-6467db7f54/runs.csv`
  - `artifacts/binary_ab_smoke/worktree-r005/runs.csv`
- patch: `diff.patch`
