# Linux `perf` Differential Profile (`garnet-rs` vs Dragonfly) — 2026-02-23 (1-thread)

## Environment

- Host: macOS + Docker Desktop (`28.5.1`)
- Execution: Linux container (`rust:latest`, Debian trixie, `aarch64`)
- Dragonfly: `v1.36.0` (`dragonfly-aarch64`)
- Script: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Artifact root:
  `garnet-rs/benches/results/linux-perf-diff-docker-20260223-012725`

## Workload

- `THREADS=1`
- `CONNS=4`
- `REQUESTS=12000`
- `PRELOAD_REQUESTS=12000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`
- `WORKLOADS="set get"`

## Throughput/Latency Snapshot

| Target | Workload | Ops/sec | Avg Lat (ms) | p99 (ms) |
|---|---:|---:|---:|---:|
| garnet | SET | 37,076.92 | 0.10768 | 0.183 |
| garnet | GET | 37,443.63 | 0.10657 | 0.183 |
| dragonfly | SET | 55,391.56 | 0.07214 | 0.143 |
| dragonfly | GET | 54,768.26 | 0.07294 | 0.151 |

## Hotspot Summary (`perf report`)

Garnet top sampled symbols are dominated by wake/scheduling:

- `try_to_wake_up` (`~24.47%` SET owner thread, `~22.92%` GET runtime worker)
- `__wake_up_sync_key` (`~17.02%` SET runtime worker, `~12.50%` GET runtime worker)
- owner-thread sync path remains visible:
  `garnet_server::shard_owner_threads::ShardOwnerThreadPool::execute_sync`

Secondary user-space hotspots include hash/store internals (`HashIndex::find_*`) and mutex wait paths.

## Evidence Gate Notes

- A previously proposed `ServerMetrics` fast path is visible only as a low-weight sampled symbol (`~1.06%` in this capture) and is not currently a top hotspot.
- Optimization acceptance should prioritize currently dominant buckets:
  owner-thread wake/scheduling and sync handoff behavior.

## Next Candidate Experiments (only hotspot-backed)

1. Reduce owner-thread wakeup frequency in request handoff path and re-measure.
2. Evaluate batching/coalescing at owner-thread boundary to reduce `execute_sync` wake pressure.
3. Re-run same profile with median-of-N wrapper if single-run variance is high.
