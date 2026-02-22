# Redis Benchmark/Compatibility Tooling Notes (2026-02-21)

## Source

- Imported DeepResearch report:
  - `docs/performance/redis-benchmark-tools-deepresearch-2026-02-21.md`

## Project-Relevant Findings

- Redis official test suite supports external-server mode and should be run against Garnet:
  - `./runtest --host <host> --port <port> --singledb`
  - RESP mode can be forced with `--force-resp3` when validating RESP3 behavior.
- Compatibility and performance must be treated as separate gates:
  - Compatibility: Redis test-suite external mode + command-level integration checks.
  - Performance: memtier-centered workload matrix with fixed parameters.
- For fair cross-engine comparison, benchmark parameters must be strictly identical:
  - same workload ratio, keyspace/distribution, pipeline, connection count, and placement.
- `redis-benchmark` is useful for smoke/per-command sanity but not the primary fairness baseline.
- `memtier_benchmark` remains the main throughput/latency benchmark for apples-to-apples server comparison.

## Immediate Integration Plan

- Add/maintain a script that runs selected Redis `runtest` units in external mode against local Garnet.
- Keep command additions gated by:
  1. `cargo test -p garnet-server`
  2. Redis external compatibility CLI subset (`runtest --host/--port`)
- Extend benchmark docs with explicit separation:
  - PR gate: lightweight compatibility + perf smoke
  - Nightly/manual: full memtier matrix

