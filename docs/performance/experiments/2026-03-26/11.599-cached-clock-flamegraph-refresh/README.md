# 11.599 Cached request-clock flamegraph refresh

- status: measurement-only
- before_commit: `e455417dfe`
- after_commit: `f268e91b24`
- revert_commit: not applicable

## Goal

Re-run the matched Docker/Linux `THREADS=8 CONNS=16 PIPELINE=4` single-lane-server comparison after `11.598` to verify whether routing `RequestTimeSnapshot::capture()` through the cached request clock actually removed the `__kernel_clock_gettime` hotspot and narrowed the Dragonfly gap.

## Shape

- client: `THREADS=8`, `CONNS=16`, `PIPELINE=4`
- Garnet server: `TOKIO_WORKER_THREADS=1`, `GARNET_STRING_OWNER_THREADS=1`, `GARNET_OWNER_THREAD_PINNING=1`, `GARNET_OWNER_THREAD_CPU_SET=0`, `GARNET_OWNER_EXECUTION_INLINE=1`
- Dragonfly server: `DRAGONFLY_PROACTOR_THREADS=1`, `DRAGONFLY_CONN_IO_THREADS=1`

## Benchmark command

```bash
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.599-cached-clock-flamegraph-refresh/results/docker-linux-compare \
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

- Garnet `SET`: `229134.67 ops/sec`, `p99 2.23404 ms`
- Garnet `GET`: `337783.99 ops/sec`, `p99 1.51505 ms`
- Dragonfly `SET`: `453371.05 ops/sec`, `p99 1.12855 ms`
- Dragonfly `GET`: `481831.29 ops/sec`, `p99 1.06109 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `-49.46%`
- Garnet vs Dragonfly `GET ops`: `-29.90%`
- Garnet vs Dragonfly `SET p99`: `+97.96%`
- Garnet vs Dragonfly `GET p99`: `+42.78%`

## Comparison to 11.592

`11.592` had:

- Garnet `SET 195492.64`, `GET 271013.64`
- Dragonfly `SET 427756.59`, `GET 414685.17`
- Garnet `GET` top sampled symbol `19.81% __kernel_clock_gettime`

This rerun shows:

- Garnet `SET` improved materially (`195493 -> 229135`)
- Garnet `GET` improved materially (`271014 -> 337784`)
- Dragonfly also improved, but less on the relative gap
- Garnet `GET` no longer tops out on `__kernel_clock_gettime`

## Flamegraph outputs

- [garnet-set.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.599-cached-clock-flamegraph-refresh/flamegraphs/garnet-set.flame.svg)
- [garnet-get.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.599-cached-clock-flamegraph-refresh/flamegraphs/garnet-get.flame.svg)
- [dragonfly-set.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.599-cached-clock-flamegraph-refresh/flamegraphs/dragonfly-set.flame.svg)
- [dragonfly-get.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.599-cached-clock-flamegraph-refresh/flamegraphs/dragonfly-get.flame.svg)

## Perf note

- Garnet:
  - `SET`: top sampled symbol `13.06% __wake_up_sync_key`
  - `GET`: top sampled symbol `14.71% __wake_up_sync_key`
  - `GET`: `__kernel_clock_gettime` fell to `0.54%`
  - `SET`: `__kernel_clock_gettime` fell to `0.27%`
- Dragonfly:
  - `SET`: top sampled symbol `8.70% dfly::DbSlice::AutoUpdater::AutoUpdater(...)`
  - `GET`: top sampled symbol `7.53% _mi_stat_increase`

## Interpretation

- `11.598` did what it was supposed to do: the old Garnet `GET` clock hotspot is effectively gone.
- The relative Dragonfly gap narrowed from `11.592`, but did not disappear.
- With clock capture demoted, the next dominant Garnet cost in this shape is now clearly wakeup / request-boundary overhead.
- Local Dragonfly source also points in a different direction than a background cached wall clock: it captures `time_now_ms_` per transaction (`src/server/transaction.cc`) via `GetCurrentTimeMs()` (`src/server/engine_shard_set.h`), while still paying attention to cheaper time primitives than plain `clock_gettime`.

## Validation

- Measurement-only iteration.
- No runtime or test code changed after `11.598`.
- No additional `make test-server` run was required for this measurement-only rerun.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `perf-top.txt`
- `diff.patch`
- `flamegraphs/*.flame.svg`
