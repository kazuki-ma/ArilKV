# 11.591 Dragonfly multithread client pipeline-4 refresh

- status: measurement-only
- before_commit: `1955066cd786ea8627f2c0bae42ec49a80dac6b5`
- after_commit: `1955066cd786ea8627f2c0bae42ec49a80dac6b5`
- revert_commit: not applicable

## Goal

Measure a more demanding `PIPELINE=4` shape where the client is explicitly
multi-threaded, while keeping the server on the same owner-inline single-thread
shape used by the recent plain-path comparisons.

This answers a narrower question than `11.590`: not "how pipeline scales in
general", but "what happens if the client is multi-threaded while pipeline is
already greater than one".

## Assumption

Interpreted the request as:

- multi-threaded memtier client
- matched Docker/Linux loopback comparison
- same owner-inline single-thread server shape as the recent baseline

Concretely, this run used `THREADS=8` and `CONNS=16`, while keeping
`TOKIO_WORKER_THREADS=1`, `GARNET_STRING_OWNER_THREADS=1`,
`GARNET_OWNER_EXECUTION_INLINE=1`, `DRAGONFLY_PROACTOR_THREADS=1`, and
`DRAGONFLY_CONN_IO_THREADS=1`.

## Benchmark shape

- Harness: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Targets: `garnet`, `dragonfly`
- Workloads: `set`, `get`
- `THREADS=8`
- `CONNS=16`
- `REQUESTS=20000`
- `PRELOAD_REQUESTS=20000`
- `PIPELINE=4`
- `SIZE_RANGE=1-256`
- `SERVER_CPU_SET=0`
- `CLIENT_CPU_SET=1-7`
- `TOKIO_WORKER_THREADS=1`
- `GARNET_STRING_OWNER_THREADS=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`
- `DRAGONFLY_PROACTOR_THREADS=1`
- `DRAGONFLY_CONN_IO_THREADS=1`

## Benchmark command

```bash
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-26/11.591-dragonfly-multithread-pipeline4-refresh/results/docker-linux-compare \
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

- Garnet `SET`: `139981.45 ops/sec`, `p99 8.767 ms`
- Garnet `GET`: `285733.77 ops/sec`, `p99 2.991 ms`
- Dragonfly `SET`: `393218.88 ops/sec`, `p99 2.767 ms`
- Dragonfly `GET`: `435444.43 ops/sec`, `p99 1.895 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `-64.40%`
- Garnet vs Dragonfly `GET ops`: `-34.38%`
- Garnet vs Dragonfly `SET p99`: `+216.84%`
- Garnet vs Dragonfly `GET p99`: `+57.84%`

## Comparison to the single-thread-client `PIPELINE=4` shape

From `11.590`, the single-thread-client `PIPELINE=4` run with the same
owner-inline single-thread server shape was:

- Garnet `SET`: `399041.50 ops/sec`, `p99 0.079 ms`
- Garnet `GET`: `516266.26 ops/sec`, `p99 0.079 ms`
- Dragonfly `SET`: `216443.42 ops/sec`, `p99 0.143 ms`
- Dragonfly `GET`: `220355.81 ops/sec`, `p99 0.143 ms`

So in this exact "client only got more parallel" experiment, the cross-engine
ordering flipped:

- single-thread client + `PIPELINE=4`: Garnet ahead on both `SET` and `GET`
- multi-thread client + `PIPELINE=4`: Dragonfly ahead on both `SET` and `GET`

## Perf note

- Garnet:
  - `SET`: top sampled symbol `__wake_up_sync_key` `14.12%`
  - `GET`: top sampled symbol `__wake_up_sync_key` `18.09%`
- Dragonfly:
  - `SET`: top sampled symbol `dfly::DbSlice::AutoUpdater::AutoUpdater(...)` `8.50%`
  - `GET`: top sampled symbol `_mi_stat_increase` `7.85%`

## Interpretation

- In this narrow shape, the bottleneck is no longer the same as the earlier
  single-thread-client owner-inline comparisons.
- Once the client becomes heavily parallel while the server stays on a single
  owner-inline execution lane, Garnet falls behind badly, especially on `SET`.
- That strongly suggests the single-owner execution path scales well for low
  parallelism and low pipeline, but not for this "many client threads already
  feeding a batched server" shape.
- So the earlier "Garnet is about 2x faster" result is real for the narrow
  `THREADS=1, PIPELINE=1` shape, but it does not generalize to this
  multi-thread-client `PIPELINE=4` shape.

## Validation

- Measurement-only iteration.
- No runtime or test code changed.
- No additional `make test-server` run was required for this refresh.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `perf-top.txt`
- `diff.patch`
