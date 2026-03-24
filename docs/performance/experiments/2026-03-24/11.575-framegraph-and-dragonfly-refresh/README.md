# 11.575 framegraph and Dragonfly refresh

- status: measurement-only
- before_commit: `7697efaa19549d38fe1220adb27876994c486918`
- after_commit: `7697efaa19549d38fe1220adb27876994c486918`
- revert_commit: not applicable

## Goal

Refresh the current evidence after the rejected queued-tail clock-sharing
experiment:

- rerun the matched Docker/Linux Garnet vs Dragonfly owner-inline comparison on
  the current tree
- regenerate a fresh local macOS Garnet flamegraph from the rebuilt current
  binary

The point is to verify whether the dominant cost has shifted, not to reuse older
captures.

## Benchmark shape

### Docker/Linux comparison

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

### Local framegraph

- Harness: `garnet-rs/benches/local_hotspot_framegraph_macos.sh`
- Target: current local `garnet-server` release binary rebuilt from `HEAD`
- Workloads: `set`, `get`
- `THREADS=1`
- `CONNS=4`
- `REQ_PER_CLIENT=50000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`
- `STRING_STORE_SHARDS=2`
- `TOKIO_WORKER_THREADS=1`
- `GARNET_STRING_OWNER_THREADS=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`

## Benchmark commands

```bash
OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.575-framegraph-and-dragonfly-refresh/results/docker-linux-compare \
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

```bash
OUTDIR=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.575-framegraph-and-dragonfly-refresh/results/local-hotspots \
THREADS=1 \
CONNS=4 \
REQ_PER_CLIENT=50000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
STRING_STORE_SHARDS=2 \
TOKIO_WORKER_THREADS=1 \
GARNET_STRING_OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/local_hotspot_framegraph_macos.sh
```

## Result

- Garnet `SET`: `141700.26 ops/sec`, `p99 0.079 ms`
- Garnet `GET`: `144459.93 ops/sec`, `p99 0.079 ms`
- Dragonfly `SET`: `81235.82 ops/sec`, `p99 0.111 ms`
- Dragonfly `GET`: `90499.79 ops/sec`, `p99 0.111 ms`

## Relative comparison

- Garnet vs Dragonfly `SET ops`: `+74.43%`
- Garnet vs Dragonfly `GET ops`: `+59.62%`
- Garnet vs Dragonfly `SET p99`: `-28.83%`
- Garnet vs Dragonfly `GET p99`: `-28.83%`

## Perf note

### Matched Docker/Linux comparison

- Garnet:
  - `SET`: top sampled symbol `__wake_up_sync_key` `10.64%`
  - `GET`: top sampled symbol `__wake_up_sync_key` `9.88%`
- Dragonfly:
  - `SET`: top sampled symbol `__wake_up_sync_key` `17.35%`
  - `GET`: top sampled symbol `__wake_up_sync_key` `18.37%`

### Fresh local macOS flamegraph

- `GET` still shows the expected `handle_connection -> handle_get ->
  TsavoriteSession::peek` path, but second-order cost is not just socket I/O:
  `append_bulk_string`, `record_key_access`, `capture_time_snapshot`,
  `add_client_input_bytes`, and `add_client_output_bytes` are all visible in the
  sampled stack.
- `SET` still shows `handle_connection -> owner-thread execution ->
  execute_bytes_in_db -> handle_set`, with `record_key_access`,
  `HashIndex::find_tag_entry`, `track_string_key_in_shard`, and metrics/time
  bookkeeping showing up behind the socket leaves.

## Interpretation

- The cross-engine comparison did not uncover a new regression. In the matched
  Docker/Linux loopback shape, Garnet remains clearly ahead of Dragonfly on both
  throughput and p99 for plain owner-inline `GET` and `SET`.
- The Linux-side sampled tops still point first at wakeup/loopback overhead, so
  the cross-engine result is not being dominated by a newly exposed storage
  codec issue.
- The fresh local flamegraph still gives useful second-order targets inside
  Garnet itself: `record_key_access(...)`, RESP bulk emission, and client
  metrics/time bookkeeping remain visible common-path work.
- The current evidence therefore keeps the next optimization target on internal
  bookkeeping cleanup rather than revisiting queued-request clock sharing.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `garnet-set-top.txt`
- `garnet-get-top.txt`
- `dragonfly-set-top.txt`
- `dragonfly-get-top.txt`
- `local-garnet-set-top.txt`
- `local-garnet-get-top.txt`
- `garnet-set.flame.svg`
- `garnet-get.flame.svg`
- `diff.patch`
