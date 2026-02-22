# Experiment 13.09 - SET Allocation Hotpath Reduction

## Metadata

- experiment_id: `13.09`
- date: `2026-02-23`
- type: `hot-path allocation reduction`
- status: `accepted`
- before_commit: `f0dec40124`
- after_commit: `PENDING`

## Goal

Reduce high-concurrency `SET` (`pipeline=1`) allocation/free overhead visible in `perf`/framegraph.

## Changes Under Test

1. `handle_set` no longer allocates an intermediate `value.to_vec()` before `encode_stored_value`.
2. `track_string_key_in_shard` now avoids duplicate key allocation (`contains` check before `insert`).

## Benchmark Shape

- target: `garnet`
- workload: `SET`
- `THREADS=8`
- `CONNS=16`
- `REQUESTS=12000`
- `PRELOAD_REQUESTS=12000`
- `PIPELINE=1`
- `SIZE_RANGE=1-1024`
- `SERVER_CPU_SET=0`
- `CLIENT_CPU_SET=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`

Command:

```bash
THREADS=8 CONNS=16 REQUESTS=12000 PRELOAD_REQUESTS=12000 PIPELINE=1 SIZE_RANGE=1-1024 \
PERF_FREQ=199 TARGETS='garnet' WORKLOADS='set' SERVER_CPU_SET=0 CLIENT_CPU_SET=1 \
GARNET_OWNER_THREAD_PINNING=1 GARNET_OWNER_THREAD_CPU_SET=0 GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/docker_linux_perf_diff_profile.sh
```

## Before/After (Measured)

- before outdir: `garnet-rs/benches/results/linux-perf-diff-docker-20260223-072014`
- after outdir: `garnet-rs/benches/results/linux-perf-diff-docker-20260223-072808`

Throughput:

- before: `274076.98 ops/sec`
- after: `280317.75 ops/sec`
- delta: `+2.28%`

Top symbol (`perf report`):

- `cfree`: `10.85% -> 9.61%` (`-1.24 pp`)

Folded-stack aggregates:

- alloc+free share: `16.41% -> 14.37%`
- `malloc` samples: `100,502,500 -> 60,301,500` (`-40.0%`)
- `cfree` samples: `597,989,875 -> 517,587,875` (`-13.4%`)

## Artifacts

- `artifacts/summary.txt`
- `artifacts/alloc_counts.csv`
- `diff.patch`
- before alloc framegraph: `garnet-rs/benches/results/linux-perf-diff-docker-20260223-072014/garnet/set/flame-garnet-set-allocfree.svg`
- after alloc framegraph: `garnet-rs/benches/results/linux-perf-diff-docker-20260223-072808/garnet/set/flame-garnet-set-allocfree.svg`

## Decision

Accept. The change is low risk and measurably reduces allocator pressure with a positive throughput delta for this target shape.
