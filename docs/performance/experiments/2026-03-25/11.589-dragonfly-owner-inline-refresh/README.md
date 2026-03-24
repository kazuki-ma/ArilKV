# 11.589 Dragonfly owner-inline refresh

- status: measurement-only
- before_commit: `9451c13b7667673556f6fe22ee1642844efb4d5b`
- after_commit: `9451c13b7667673556f6fe22ee1642844efb4d5b`
- revert_commit: not applicable

## Goal

Refresh the matched Docker/Linux owner-inline Garnet vs Dragonfly comparison on
the latest tree after the recent hot-path bookkeeping work.

The point is to confirm whether the current `HEAD` still holds the same
cross-engine lead under the exact same comparison shape.

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
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-25/11.589-dragonfly-owner-inline-refresh/results/docker-linux-compare \
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

- Garnet `SET`: `136497.99 ops/sec`, `p99 0.079 ms`
- Garnet `GET`: `154211.16 ops/sec`, `p99 0.071 ms`
- Dragonfly `SET`: `80565.44 ops/sec`, `p99 0.127 ms`
- Dragonfly `GET`: `87389.17 ops/sec`, `p99 0.095 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `+69.42%`
- Garnet vs Dragonfly `GET ops`: `+76.46%`
- Garnet vs Dragonfly `SET p99`: `-37.80%`
- Garnet vs Dragonfly `GET p99`: `-25.26%`

## Perf note

- Garnet:
  - `SET`: top sampled symbol `__wake_up_sync_key` `6.00%`
  - `GET`: top sampled symbol `__kernel_clock_gettime` `12.16%`
- Dragonfly:
  - `SET`: top sampled symbol `__kernel_clock_gettime` `13.46%`
  - `GET`: top sampled symbol `__wake_up_sync_key` `20.62%`

## Interpretation

- The latest matched Docker/Linux owner-inline comparison still shows Garnet
  clearly ahead of Dragonfly on both throughput and p99 for plain `GET` and
  `SET`.
- The top sampled symbols stay consistent with the recent local flamegraphs:
  wakeup/loopback and timekeeping remain the dominant second-order costs rather
  than a newly exposed codec or storage regression.
- This refresh therefore does not change the current optimization direction.
  The next evidence-based targets remain internal bookkeeping and timekeeping on
  the plain request path.

## Validation

- Measurement-only iteration.
- No runtime or test code changed.
- No additional `make test-server` run was required for this refresh.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `garnet-set-top.txt`
- `garnet-get-top.txt`
- `dragonfly-set-top.txt`
- `dragonfly-get-top.txt`
- `diff.patch`
