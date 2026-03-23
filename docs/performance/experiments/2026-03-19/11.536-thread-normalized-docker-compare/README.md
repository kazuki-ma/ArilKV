# 11.536 Thread-normalized Docker/Linux compare

- Date: 2026-03-19
- Purpose: rerun the Docker/Linux Garnet vs Dragonfly comparison after correcting the obvious server-thread-count mismatch in the benchmark harness.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 820; benchmark harness threading knobs + rerun)`
- Diff note: this iteration changes only benchmark harnesses and experiment docs.

## What changed in the harness

- The previous harness matched client load (`THREADS`, `CONNS`) and CPU affinity sets, but did not explicitly normalize server-side internal thread counts.
- New optional knobs were added so the benchmark can force:
  - `TOKIO_WORKER_THREADS`
  - `GARNET_STRING_OWNER_THREADS`
  - `DRAGONFLY_PROACTOR_THREADS`
  - `DRAGONFLY_CONN_IO_THREADS`
- For this run the benchmark used:
  - `SERVER_CPU_SET=0-1`
  - `CLIENT_CPU_SET=2-3`
  - `THREADS=2`
  - `CONNS=4`
  - `TOKIO_WORKER_THREADS=2`
  - `GARNET_STRING_OWNER_THREADS=2`
  - `DRAGONFLY_PROACTOR_THREADS=2`
  - `DRAGONFLY_CONN_IO_THREADS=2`

## Command

- `THREADS=2 CONNS=4 REQUESTS=10000 PRELOAD_REQUESTS=10000 PIPELINE=1 SIZE_RANGE=1-256 SERVER_CPU_SET=0-1 CLIENT_CPU_SET=2-3 TOKIO_WORKER_THREADS=2 GARNET_STRING_OWNER_THREADS=2 DRAGONFLY_PROACTOR_THREADS=2 DRAGONFLY_CONN_IO_THREADS=2 TARGETS='garnet dragonfly' WORKLOADS='set get' OUTDIR_HOST=/tmp/garnet-linux-perf-diff-dragonfly-threads2-20260319 ./garnet-rs/benches/docker_linux_perf_diff_profile.sh`

## Results

- Garnet `SET`: `41632.36 ops/sec`, `p99 0.383 ms`
- Garnet `GET`: `49765.70 ops/sec`, `p99 0.351 ms`
- Dragonfly `SET`: `194981.66 ops/sec`, `p99 0.111 ms`
- Dragonfly `GET`: `163859.21 ops/sec`, `p99 0.127 ms`
- Relative to Dragonfly, Garnet was:
  - `SET -78.65%`
  - `GET -69.63%`
  - `SET p99 +245.05%`
  - `GET p99 +176.38%`

## Flamegraph/top-stack readout

- Garnet still shows the same core shape, but more starkly:
  - wakeups on Tokio workers
  - synchronous owner-thread handoff
  - `ShardOwnerThreadPool::execute_sync` in the hot path
- Dragonfly still spreads cost across proactor/io_uring scheduling rather than exposing one equivalent synchronous owner-thread handoff hotspot.

## Interpretation

- The user’s thread-count concern was correct. The previous harness was not thread-normalized.
- After normalizing server-side thread counts explicitly, the performance gap did not disappear. It widened.
- That strongly suggests the current Garnet plain GET/SET gap is not mainly an artifact of thread-count mismatch.
- The new run reinforces the earlier flamegraph conclusion: the main enemy is the synchronous owner-thread boundary, not a newly introduced storage/value-codec regression.
