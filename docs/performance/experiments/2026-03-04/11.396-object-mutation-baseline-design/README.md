# Experiment 11.396 - Object Mutation Baseline and Benchmark Design (RT-001)

## Metadata

- experiment_id: `11.396`
- date: `2026-03-04`
- before_commit: `working-tree`
- after_commit: `working-tree`
- commit_message: `perf(rt-001): add object-mutation baseline harness and initial noise baseline`
- full_diff: `docs/performance/experiments/2026-03-04/11.396-object-mutation-baseline-design/diff.patch` (benchmark-harness/doc changes only; no runtime code diff)

## Goal

Create a reproducible benchmark baseline for `RT-001` (object-command ser/deser hotspot)
before incremental object-update redesign work.

## Benchmark Design

Runner: `tools/object_mutation_ab_local.sh`

Workloads (all mutation-heavy):

- hash churn: `HSET objhash __key__ __data__` + `HDEL objhash __key__`
- list churn: `LPUSH objlist __data__` + `LPOP objlist`
- set churn: `SADD objset __key__` + `SREM objset __key__`
- zset churn: `ZADD objzset 1 __key__` + `ZREM objzset __key__`

Per-run setup:

- single-node server (no replica)
- preload each object with `PRELOAD_REQUESTS` entries
- then execute workload with balanced `1:1` mutation pair ratio
- reject runs containing:
  - `Connection error:`
  - `handle error response:`

Recorded metrics:

- ops/sec and p99 for each command row
- per-label medians in `summary.txt`
- label delta in `comparison.txt`

## Initial Baseline Run (same-binary noise)

Command profile:

- `RUNS=3`
- `THREADS=1`, `CONNS=2`
- `REQUESTS=8000`
- `PRELOAD_REQUESTS=4096`
- `PIPELINE=1`, `SIZE_RANGE=16-128`
- `KEY_MAX=4096`
- binary used for both labels: `/tmp/garnet-new-rt002-s3-20260304-142147`

Source: `artifacts/noise_baseline/comparison.txt`

Noise deltas (`baseline_a` -> `baseline_b`):

- ops:
  - hash: `-0.42%`
  - list: `-0.05%`
  - set: `+1.31%`
  - zset: `-0.29%`
- p99:
  - hash: `+8.06%` (`HSET`), `+13.52%` (`HDEL`)
  - list: `-37.06%` (`LPUSH`), `+2.61%` (`LPOP`)
  - set: `-5.22%` (`SADD`), `-15.33%` (`SREM`)
  - zset: `+12.44%` (`ZADD`), `+4.00%` (`ZREM`)

Current baseline medians (`baseline_a`):

- hash: `~2771 ops`, p99 `~2.37 ms`
- list: `~2188 ops`, p99 `~0.92-1.51 ms`
- set: `~4307 ops`, p99 `~1.69-1.88 ms`
- zset: `~4307 ops`, p99 `~1.42-1.60 ms`

## Acceptance Gate Draft for RT-001 Follow-ups

Based on initial same-binary noise:

- primary signal: throughput on the touched object family
  - treat `< +/-3%` as noise band
  - target `>= +8%` median ops gain on mutated command pair
- latency guard:
  - reject if both commands in touched family regress `> +20%` p99
  - for list path, rerun with higher `RUNS` before concluding due higher observed p99 variance

This gate is intentionally conservative for the first redesign slice and will be
updated after the first real cross-binary RT-001 change.

## Artifacts

- `tools/object_mutation_ab_local.sh`
- `artifacts/noise_baseline/comparison.txt`
- `artifacts/noise_baseline/baseline_a/summary.txt`
- `artifacts/noise_baseline/baseline_a/runs.csv`
- `artifacts/noise_baseline/baseline_b/summary.txt`
- `artifacts/noise_baseline/baseline_b/runs.csv`
- `diff.patch`
