# Experiment 13.06 - Linux Hotspot Refresh with Owner-Pinned Single-Thread Path

## Metadata

- experiment_id: `13.06`
- date: `2026-02-23`
- target: `garnet` only
- objective: refresh `perf + framegraph` evidence after listener/owner co-location work, while forcing owner pinning path in benchmark runner

## Goal

Re-capture 1-thread hotspots under the current architecture and pinning/owner settings so optimization decisions remain evidence-driven.

## Procedure

Command:

```bash
cd garnet-rs
THREADS=1 CONNS=4 REQUESTS=30000 PRELOAD_REQUESTS=30000 PIPELINE=1 SIZE_RANGE=1-256 \
PERF_FREQ=199 TARGETS='garnet' WORKLOADS='set get' \
SERVER_CPU_SET=0 CLIENT_CPU_SET=1 \
GARNET_OWNER_THREAD_PINNING=1 GARNET_OWNER_THREAD_CPU_SET=0 GARNET_OWNER_EXECUTION_INLINE=1 \
./benches/docker_linux_perf_diff_profile.sh
```

Output run:

- `garnet-rs/benches/results/linux-perf-diff-docker-20260223-064732/`

Framegraph generation:

- `stackcollapse-perf.pl + flamegraph.pl` on `perf-script-*.txt`
- outputs copied into this experiment artifact directory

## Results

Memtier totals:

- `SET`: `158140.27 ops/sec`, `p99=0.079 ms`
- `GET`: `110417.42 ops/sec`, `p99=0.071 ms`

Compared to prior non-pinned single-thread hotspot run (`...-064243`):

- `SET`: `37054.68 -> 158140.27` (`+326.78%`)
- `GET`: `37422.71 -> 110417.42` (`+194.98%`)

Hotspot summary from `perf report` (top lines):

- Dominant remains wake/scheduler/network-kernel side (for example `__wake_up_sync_key`).
- Prior top marker `try_to_wake_up` no longer dominates top samples in this pinned-owner run.
- App-side samples are more spread across request parsing/storage paths (`handle_set/get`, `HashIndex::find_tag_address`, network write path), rather than owner handoff waits.

## Artifacts

- `artifacts/profile-env.txt`
- `artifacts/memtier-set-totals.txt`
- `artifacts/memtier-get-totals.txt`
- `artifacts/perf-top-set.txt`
- `artifacts/perf-top-get.txt`
- `artifacts/flame-garnet-set.svg`
- `artifacts/flame-garnet-get.svg`
