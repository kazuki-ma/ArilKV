# Performance Report

Last updated: 2026-05-10

This document records the current Garnet-rs performance baseline and the exact
benchmark shapes used for comparison. Numbers below were collected on a macOS
host through Docker Desktop's Linux runtime, so they are useful as directional
evidence for implementation choices, not as absolute production Linux capacity.

## Compatibility-Surface Caveat

Simple `SET`/`GET` benchmarks are useful, but they can hide product-shape
differences. ArilKV keeps Redis-compatible ACL checks, ACL audit logging, slow
log tracking, command statistics, client tracking, monitor/replication hooks,
WAIT/WAITAOF semantics, and persistence-related command surface in the same
server that is being benchmarked. Some high-throughput Redis-compatible systems
either implement a narrower subset, provide a different equivalent, or publish
headline benchmarks against a thinner hot path.

For external comparisons, record whether the following are present and enabled:

- ACL user/rule enforcement and ACL LOG style security auditing.
- Slow log and command statistics accounting.
- CLIENT TRACKING, MONITOR, key access metadata, and client bookkeeping.
- Replication, WAIT/WAITAOF, AOF/RDB/snapshot, and maxmemory/eviction behavior.
- Pipeline batching strategy: command-at-a-time execution versus classified
  single-key batch execution.

The public checklist lives at:
`docs/benchmarking-notes.html` / <https://kazuki-ma.github.io/ArilKV/benchmarking-notes.html>.

## Current Default Policy

Garnet uses inline owner execution by default. In this mode, the listener
executes single-key string command work directly on the owner path, avoiding the
older listener-to-owner cross-thread handoff.

Override only for legacy experiments:

```bash
GARNET_OWNER_EXECUTION_INLINE=0
```

Valkey `9.0.4` defaults to `io-threads=1`, which means its ordinary startup
also uses the main event loop for the tested simple string path. This makes
Garnet's inline owner default the closest comparable single-process policy.

## Baseline Comparison

Command:

```bash
CAPTURE_PERF=0 \
THREADS=1 CONNS=4 REQUESTS=50000 PRELOAD_REQUESTS=50000 \
PIPELINE=1 SIZE_RANGE=1-256 \
SERVER_CPU_SET=0 CLIENT_CPU_SET=1 TOKIO_WORKER_THREADS=1 \
DRAGONFLY_PROACTOR_THREADS=1 DRAGONFLY_CONN_IO_THREADS=1 \
TARGETS="garnet dragonfly valkey" WORKLOADS="set get" \
benches/docker_linux_perf_diff_profile.sh
```

Artifact:

```text
benches/results/linux-perf-diff-docker-20260510-042719/
```

| Target | SET ops/sec | SET p99 | GET ops/sec | GET p99 |
|---|---:|---:|---:|---:|
| Garnet | 177,999.76 | 0.055 ms | 218,181.50 | 0.047 ms |
| Dragonfly | 150,864.72 | 0.055 ms | 176,304.03 | 0.055 ms |
| Valkey | 110,537.36 | 0.063 ms | 111,818.36 | 0.055 ms |

Relative throughput:

| Comparison | SET | GET |
|---|---:|---:|
| Garnet / Dragonfly | 1.18x | 1.24x |
| Garnet / Valkey | 1.61x | 1.95x |

Integrity checks:

- `memtier_benchmark` reported the expected thread, connection, and request
  settings.
- No `Connection error:` or `handle error response:` lines were present.
- SET and GET totals were non-zero for all targets.

## Pipeline Scaling

Pipeline checks were run against Garnet only, with the same default inline owner
policy and single-server-core shape.

Shared command shape:

```bash
CAPTURE_PERF=0 \
THREADS=1 CONNS=4 REQUESTS=100000 PRELOAD_REQUESTS=100000 \
SIZE_RANGE=1-256 SERVER_CPU_SET=0 CLIENT_CPU_SET=1 TOKIO_WORKER_THREADS=1 \
TARGETS="garnet" WORKLOADS="set get" \
benches/docker_linux_perf_diff_profile.sh
```

| Pipeline | SET ops/sec | SET p99 | GET ops/sec | GET p99 | Artifact |
|---:|---:|---:|---:|---:|---|
| 16 | 526,033.39 | 0.255 ms | 735,647.07 | 0.279 ms | `benches/results/garnet-pipeline-smoke-p16-20260510-042940/` |
| 64 | 778,973.94 | 0.511 ms | 1,345,967.48 | 0.303 ms | `benches/results/garnet-pipeline-smoke-p64-20260510-043035/` |

Interpretation:

- GET crosses 1M ops/sec in this Docker/Linux single-server-core shape at
  `PIPELINE=64`.
- SET improves substantially with pipelining, but in this exact shape it is
  still below 1M ops/sec.
- Tail latency rises with deeper pipeline because more work is queued per
  connection before responses drain.

## Methodology Notes

- `REQUESTS` is memtier's requests per client. With `CONNS=4`, total operations
  per workload are roughly `REQUESTS * 4`.
- The Docker harness pins server and client work with `taskset` when
  `SERVER_CPU_SET` and `CLIENT_CPU_SET` are provided.
- Docker Desktop is not a substitute for real Linux NIC/RSS/RPS/RFS evidence.
  For network-stack tuning, use a Linux host and the NIC affinity tools under
  `benches/`.
- For perf/flamegraph capture, use the same harness with `CAPTURE_PERF=1`.

## Reproduction

Fresh checkout setup:

```bash
benches/setup_perf_compare_env.sh
```

Full three-target comparison with perf data:

```bash
THREADS=1 CONNS=4 REQUESTS=500000 PRELOAD_REQUESTS=500000 \
PIPELINE=1 SIZE_RANGE=1-256 \
SERVER_CPU_SET=0 CLIENT_CPU_SET=1 TOKIO_WORKER_THREADS=1 \
DRAGONFLY_PROACTOR_THREADS=1 DRAGONFLY_CONN_IO_THREADS=1 \
TARGETS="garnet dragonfly valkey" WORKLOADS="set get" \
benches/docker_linux_perf_diff_profile.sh
```
