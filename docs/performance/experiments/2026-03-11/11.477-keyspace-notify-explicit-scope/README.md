# 11.477 keyspace notification + key-scoped helper DB explicitness

- Date: 2026-03-11
- Base commit: `6ea21a73a1`
- New commit: iteration `11.477` changeset in this tree
- Scope: delete ambient selected-DB lookup from keyspace-notification and remaining DB-local notification/encoding helper surfaces.

## Why this exists

This is the required benchmark milestone for task `11.477` in `TODO_AND_STATUS.md`:
- harden keyspace notification and overwrite event rendering to use explicit `DbName` inputs.
- update remaining blocking/helper call paths that previously derived DB from thread-local request context.
- run before/after microbenchmark and one Dragonfly differential profile check.

## Benchmark artifacts

- Garnet-only A/B (`binary_ab_local.sh`): `comparison.txt`
- Dragonfly differential comparison (Linux docker, one run): `dragonfly_summary.txt`, `dragonfly_median_summary.csv`, `runs.csv`
- Patch: `diff.patch`

## Artifacts and deltas

- Garnet before/after (`THREADS=1`, `CONNS=2`, `REQUESTS=2000`, `PRELOAD=2000`, `PIPELINE=1`, `SIZE_RANGE=1-64`):
  - `set ops`: `42240.5 -> 44084.0` (`+4.36%`)
  - `get ops`: `36885.5 -> 43110.5` (`+16.88%`)
  - `set p99 (ms)`: `0.123 -> 0.115` (`-6.50%`)
  - `get p99 (ms)`: `0.171 -> 0.123` (`-28.07%`)
- Dragonfly differential (`THREADS=1`, `CONNS=2`, `REQUESTS=500`, `PRELOAD=500`, `PIPELINE=1`, `SIZE_RANGE=1-64`):
  - `ratio set`: `0.288`
  - `ratio get`: `0.187`
  - Garnet `set`/`get` throughput: `60624.43 / 29542.97`
  - Dragonfly `set`/`get` throughput: `17487.41 / 5535.29`
