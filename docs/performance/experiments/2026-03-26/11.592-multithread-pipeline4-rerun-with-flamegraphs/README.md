# 11.592 Multithread client pipeline-4 rerun with flamegraphs

- status: measurement-only
- before_commit: `30c1bd3178b1afcfadce02f8b509f684d2d8dfe2`
- after_commit: `30c1bd3178b1afcfadce02f8b509f684d2d8dfe2`
- revert_commit: not applicable

## Goal

Re-test the `11.591` shape to see how much of the large Dragonfly lead might
have been host noise, and capture actual Linux flamegraphs for both engines
under the same workload.

## Important shape clarification

This is still the "single-lane server, multi-threaded client" shape:

- client: `THREADS=8`, `CONNS=16`, `PIPELINE=4`
- Garnet server: `TOKIO_WORKER_THREADS=1`, `GARNET_STRING_OWNER_THREADS=1`,
  `GARNET_OWNER_EXECUTION_INLINE=1`
- Dragonfly server: `DRAGONFLY_PROACTOR_THREADS=1`,
  `DRAGONFLY_CONN_IO_THREADS=1`

So yes: this comparison intentionally keeps both servers on a one-thread-ish
execution shape while increasing client-side parallelism.

## Benchmark command

```bash
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.592-multithread-pipeline4-rerun-with-flamegraphs/results/docker-linux-compare \
THREADS=8 \
CONNS=16 \
REQUESTS=20000 \
PRELOAD_REQUESTS=20000 \
PIPELINE=4 \
SIZE_RANGE=1-256 \
SERVER_CPU_SET=0 \
CLIENT_CPU_SET=1-7 \
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

- Garnet `SET`: `195492.64 ops/sec`, `p99 4.991 ms`
- Garnet `GET`: `271013.64 ops/sec`, `p99 3.823 ms`
- Dragonfly `SET`: `427756.59 ops/sec`, `p99 1.911 ms`
- Dragonfly `GET`: `414685.17 ops/sec`, `p99 2.367 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `-54.30%`
- Garnet vs Dragonfly `GET ops`: `-34.65%`
- Garnet vs Dragonfly `SET p99`: `+161.17%`
- Garnet vs Dragonfly `GET p99`: `+61.51%`

## Comparison to 11.591

`11.591` had:

- Garnet `SET 139981.45`, `GET 285733.77`
- Dragonfly `SET 393218.88`, `GET 435444.43`

So there was some noise:

- Garnet `SET` improved materially on rerun (`139981 -> 195493`)
- Dragonfly `SET` also improved (`393219 -> 427757`)
- Garnet `GET` softened slightly (`285734 -> 271014`)
- Dragonfly `GET` softened slightly (`435444 -> 414685`)

But the high-level result did not flip. Dragonfly remained ahead on both
throughput and p99 in this shape.

## Flamegraph outputs

- [garnet-set.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.592-multithread-pipeline4-rerun-with-flamegraphs/flamegraphs/garnet-set.flame.svg)
- [garnet-get.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.592-multithread-pipeline4-rerun-with-flamegraphs/flamegraphs/garnet-get.flame.svg)
- [dragonfly-set.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.592-multithread-pipeline4-rerun-with-flamegraphs/flamegraphs/dragonfly-set.flame.svg)
- [dragonfly-get.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.592-multithread-pipeline4-rerun-with-flamegraphs/flamegraphs/dragonfly-get.flame.svg)

## Perf note

- Garnet:
  - `SET`: top sampled symbol `__wake_up_sync_key` `17.07%`
  - `GET`: top sampled symbol `__kernel_clock_gettime` `19.81%`
- Dragonfly:
  - `SET`: top sampled symbol `__wake_up_sync_key` `7.85%`
  - `GET`: top sampled symbol `_mi_stat_increase` `8.49%`

## Interpretation

- The rerun suggests `11.591` was somewhat noisy, especially on `SET`, but not
  wrong in direction.
- In this shape, Garnet still spends a visibly larger fraction of samples in
  wakeup/timekeeping-heavy paths, while Dragonfly's cost is more spread into
  engine/allocator work.
- That is consistent with the intuition that Garnet's current owner-inline
  single-lane request path is strong at low parallelism, but loses ground when
  many client threads concurrently feed that single lane even at only
  `PIPELINE=4`.

## Validation

- Measurement-only iteration.
- No runtime or test code changed.
- No additional `make test-server` run was required.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `perf-top.txt`
- `diff.patch`
- `flamegraphs/*.flame.svg`
