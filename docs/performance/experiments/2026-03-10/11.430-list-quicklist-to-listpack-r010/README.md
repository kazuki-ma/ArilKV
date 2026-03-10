# 11.430 list quicklist to listpack downgrade benchmark

## Purpose

Smoke-check that the `LSET` quicklist downgrade fix for `R-010` does not introduce an obvious server-wide throughput or tail-latency regression.

## Scope

- Before: `4a383f5521`
- After: working tree for iteration `627` (pre-commit)
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters:
  - `RUNS=1`
  - `THREADS=1`
  - `CONNS=2`
  - `REQUESTS=2000`
  - `PRELOAD_REQUESTS=2000`
  - `PIPELINE=1`
  - `SIZE_RANGE=32-256`

## Result

This was a smoke run, not a noise-resistant benchmark. The observed direction is effectively flat for throughput and neutral on GET p99, with a small SET p99 increase that should be treated as noise until reproduced:

- SET ops/sec: `44339 -> 43288` (`-2.37%`)
- GET ops/sec: `44006 -> 43917` (`-0.20%`)
- SET p99: `0.087ms -> 0.103ms` (`+18.39%`)
- GET p99: `0.119ms -> 0.119ms` (`+0.00%`)

Interpretation: this list-encoding fix does not show a material server-wide regression in the smoke sample, but `RUNS=1` is too small to claim a real performance delta either way.

## Artifacts

- comparison: `artifacts/comparison.txt`
- per-binary summaries:
  - `artifacts/base/summary.txt`
  - `artifacts/new/summary.txt`
- per-run csv:
  - `artifacts/base/runs.csv`
  - `artifacts/new/runs.csv`
- patch: `diff.patch`
