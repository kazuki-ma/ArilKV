# 11.546 Dragonfly owner-inline refresh

- Date: 2026-03-22
- Purpose: rerun the owner-inline Garnet vs Dragonfly comparison on the current tree after the recent `SET` hot-path cleanup slices.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 830; measurement/docs only)`
- Diff note: no runtime code changes in this iteration; this is a measurement-only rerun.

## Benchmark shape

- Harness: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Targets: `garnet`, `dragonfly`
- Workloads: `set`, `get`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=30000`
- `PRELOAD_REQUESTS=30000`
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

## Result

- Garnet `SET`: `118509.27 ops/sec`, `p99 0.103 ms`
- Garnet `GET`: `127112.99 ops/sec`, `p99 0.103 ms`
- Dragonfly `SET`: `79285.22 ops/sec`, `p99 0.135 ms`
- Dragonfly `GET`: `89565.74 ops/sec`, `p99 0.119 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `+49.47%`
- Garnet vs Dragonfly `GET ops`: `+41.92%`
- Garnet vs Dragonfly `SET p99`: `-23.70%`
- Garnet vs Dragonfly `GET p99`: `-13.45%`

## Perf note

- In this rerun, both servers still show loopback wakeup cost near the top of the sampled stacks.
- Garnet:
  - `SET`: `__wake_up_sync_key` `13.89%`
  - `GET`: `__wake_up_sync_key` `11.48%`
- Dragonfly:
  - `SET`: `__wake_up_sync_key` `10.94%`
  - `GET`: `__wake_up_sync_key` `22.41%`

## Interpretation

- In the owner-inline single-thread Docker/Linux shape, current Garnet is ahead of Dragonfly on both throughput and p99 for plain `SET` and `GET`.
- That means the recent owner-inline `SET` cleanup slices materially changed the current comparison point; the earlier large Dragonfly lead does not hold in this shape.
