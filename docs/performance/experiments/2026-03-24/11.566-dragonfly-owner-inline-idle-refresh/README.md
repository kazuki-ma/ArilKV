# 11.566 Dragonfly owner-inline idle refresh

- status: measurement-only
- before_commit: `6cb244eac29785a75764f6c4198dc3277cc8bba0`
- after_commit: `6cb244eac29785a75764f6c4198dc3277cc8bba0`
- revert_commit: not applicable

## Goal

Refresh the latest Garnet vs Dragonfly owner-inline Docker/Linux comparison while the host is intentionally left idle, so the current post-`11.565` comparison point is less distorted by foreground host noise.

## Benchmark shape

- Harness: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Targets: `garnet`, `dragonfly`
- Workloads: `set`, `get`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=50000`
- `PRELOAD_REQUESTS=50000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`
- `SERVER_CPU_SET=0`
- `CLIENT_CPU_SET=1`
- `TOKIO_WORKER_THREADS=1`
- `GARNET_STRING_OWNER_THREADS=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`
- `DRAGONFLY_PROACTOR_THREADS=1`
- `DRAGONFLY_CONN_IO_THREADS=1`

## Benchmark command

```bash
mkdir -p docs/performance/experiments/2026-03-24/11.566-dragonfly-owner-inline-idle-refresh && \
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.566-dragonfly-owner-inline-idle-refresh/results \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
SERVER_CPU_SET=0 \
CLIENT_CPU_SET=1 \
TOKIO_WORKER_THREADS=1 \
GARNET_STRING_OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
DRAGONFLY_PROACTOR_THREADS=1 \
DRAGONFLY_CONN_IO_THREADS=1 \
./garnet-rs/benches/docker_linux_perf_diff_profile.sh
```

## Result

- Garnet `SET`: `134038.02 ops/sec`, `p99 0.087 ms`
- Garnet `GET`: `153871.37 ops/sec`, `p99 0.079 ms`
- Dragonfly `SET`: `81162.90 ops/sec`, `p99 0.119 ms`
- Dragonfly `GET`: `82666.35 ops/sec`, `p99 0.095 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `+65.15%`
- Garnet vs Dragonfly `GET ops`: `+86.14%`
- Garnet vs Dragonfly `SET p99`: `-26.89%`
- Garnet vs Dragonfly `GET p99`: `-16.84%`

## Perf note

- Garnet:
  - `SET`: top sampled symbol `__wake_up_sync_key` `16.00%`
  - `GET`: top sampled symbol `__kernel_clock_gettime` `11.25%`
- Dragonfly:
  - `SET`: top sampled symbol `__wake_up_sync_key` `16.00%`
  - `GET`: top sampled symbol `__wake_up_sync_key` `19.80%`

## Interpretation

- With the host intentionally idle, the current owner-inline single-thread Docker/Linux shape still has Garnet clearly ahead of Dragonfly on both throughput and p99 for plain `SET` and `GET`.
- The comparison widened relative to the earlier owner-inline refresh, so the recent Garnet hot-path slices did not merely recover host-noise variance; they moved the steady comparison point.
- The new notable hotspot on Garnet `GET` is timekeeping rather than loopback wakeup, which makes the next optimization target more concrete than in the prior rerun.

## Artifacts

- `comparison.txt`
- `summary.txt`
- `garnet-set-top.txt`
- `garnet-get-top.txt`
- `dragonfly-set-top.txt`
- `dragonfly-get-top.txt`
- `diff.patch`
