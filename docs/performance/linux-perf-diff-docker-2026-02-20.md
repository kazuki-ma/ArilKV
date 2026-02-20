# Linux `perf` Differential Profile (`garnet-rs` vs Dragonfly) — 2026-02-20

## Environment

- Host: macOS + Docker Desktop (`28.5.1`)
- Execution: Linux container (`rust:latest`, Debian trixie, `aarch64`)
- Dragonfly: `v1.36.0` (`dragonfly-aarch64`)
- memtier: built from source (`RedisLabs/memtier_benchmark`)
- Script: `garnet-rs/benches/linux_perf_diff_profile.sh`
- Wrapper: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Latest artifact root:
  `garnet-rs/benches/results/linux-perf-diff-docker-20260220-132624`
- Prior same-day artifact (pre `11.30/11.31` refresh):
  `/tmp/garnet-linux-perf-slotcache-20260220-131115`

## Workload

- `THREADS=8`
- `CONNS=16`
- `REQUESTS=5000`
- `PRELOAD_REQUESTS=5000`
- `PIPELINE=1`
- `SIZE_RANGE=1-1024`
- `WORKLOADS="set get"`
- `TARGETS="garnet dragonfly"`

## Throughput/Latency Snapshot (memtier)

| Target | Workload | Ops/sec | Avg Lat (ms) | p99 (ms) |
|---|---:|---:|---:|---:|
| garnet | SET | 397,580 | 0.322 | 0.711 |
| garnet | GET | 456,954 | 0.280 | 0.623 |
| dragonfly | SET | 720,657 | 0.179 | 0.695 |
| dragonfly | GET | 718,145 | 0.178 | 0.703 |

Compared with the earlier same-day run using low hash-index default sizing, this
reduced the throughput gap materially (roughly from ~3x to ~1.5-1.8x in this
environment).

`11.30/11.31` follow-up (expiration-count fast path + `TCP_NODELAY`) did not
materially change the Linux differential gap in this single refreshed run.

## `perf report` Hotspot Notes

`perf` samples were captured for all 4 runs (`garnet/dragonfly` x `set/get`).
Report files include initial `unwind: get_proc_name unsupported` warnings, but
sample tables and `perf script` traces are present.

### Garnet (user-space symbols)

- SET: `tsavorite::hash_index::HashIndex::find_tag_entry` (~1.8%)
- GET: `tsavorite::hash_index::HashIndex::find_tag_address` (~2.9%)
- Shared: `std::sys::sync::mutex::futex::Mutex::lock_contended`
  (~2.1% SET / ~1.8% GET)
- Shared: `garnet_cluster::redis_hash_slot`
  (~0.35% SET / ~2.04% GET)
- Kernel-side dominant buckets include wakeups (`__wake_up_sync_key`,
  `try_to_wake_up`) across both workloads.

### Dragonfly (user-space symbols)

- GET: parse/hash path symbols (`dfly::detail::ascii_unpack`,
  `dfly::DbSlice::FindInternal`, transaction scheduling)
- SET: `DbSlice::AddOrFindInternal` and scheduler/proactor symbols
  (`util::fb2::*`, `ontop_fcontext`, io_uring submit path)
- Overall per-symbol concentration is flatter than Garnet in this run.

## Interpretation for U3 (network vs storage vs allocator)

This run indicates the gap is not dominated by a single allocator function.
For Garnet, visible concentration is in:

1. kernel wake/scheduling overhead
2. command-path hashing/routing (`redis_hash_slot`) and remaining hash-index
   lookup/update path (`find_tag_*`)
3. contended mutex path

Dragonfly shows more distributed io_uring/proactor + parser/hash hotspots with
higher aggregate throughput under the same load shape.

## Validity guards applied

- Benchmark run fails on memtier `Connection error:`
- Benchmark run fails on memtier `handle error response:`
- Garnet profile run uses higher default capacity:
  `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=262144`
- Server-side default hash index sizing is set to
  `DEFAULT_SERVER_HASH_INDEX_SIZE_BITS=16` (override via
  `GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS`).
