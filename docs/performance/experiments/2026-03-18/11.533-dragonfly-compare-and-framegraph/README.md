# 11.533 Dragonfly compare and framegraph refresh

- Date: 2026-03-18
- Purpose: refresh the GET/SET comparison against Dragonfly and re-check the current hot-path shape with a flamegraph after the recent replication, persistence, and test-harness work.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 817; benchmark wrapper fix + experiment docs)`
- Diff note: this experiment started as measurement-only work, but the Docker/Linux comparison harness needed a small fix because it exported empty optional CPU-pinning env vars that now fail server startup.

## Commands

- Local Garnet median benchmark:
  - `RUNS=3 THREADS=2 CONNS=4 REQUESTS=10000 PRELOAD_REQUESTS=10000 PIPELINE=1 SIZE_RANGE=1-256 OUTDIR=/tmp/garnet-vs-dragonfly-garnet-20260318 ./garnet-rs/benches/perf_regression_gate_local.sh`
- Local Dragonfly comparison:
  - launched `docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0` on `127.0.0.1:16390`
  - ran `memtier_benchmark` with `THREADS=2`, `CONNS=4`, `REQUESTS=10000`, `PRELOAD_REQUESTS=10000`, `PIPELINE=1`, `SIZE_RANGE=1-256` for `SET` and `GET`
- macOS flamegraph:
  - `cd garnet-rs && OUTDIR=/tmp/garnet-hotspots-20260318-dragonfly-compare THREADS=2 CONNS=4 REQ_PER_CLIENT=10000 PIPELINE=1 SIZE_RANGE=1-256 SAMPLE_SECONDS=10 ./benches/local_hotspot_framegraph_macos.sh`
- Docker/Linux apples-to-apples comparison:
  - `THREADS=2 CONNS=4 REQUESTS=10000 PRELOAD_REQUESTS=10000 PIPELINE=1 SIZE_RANGE=1-256 TARGETS='garnet dragonfly' WORKLOADS='set get' OUTDIR_HOST=/tmp/garnet-linux-perf-diff-dragonfly-20260318 ./garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Harness validation after the wrapper fix:
  - `bash -n garnet-rs/benches/docker_linux_perf_diff_profile.sh`

## Results

- Local native-vs-Docker comparison favored Garnet strongly:
  - `SET ops +340.95%`
  - `GET ops +540.85%`
  - `SET p99 -65.57%`
  - `GET p99 -86.82%`
- That result is not trustworthy as an engine comparison because Garnet was native on macOS while Dragonfly ran under Docker Desktop.
- Docker/Linux comparison is the more meaningful slice:
  - Garnet `SET`: `48236.74 ops/sec`, `p99 0.383 ms`
  - Garnet `GET`: `68245.88 ops/sec`, `p99 0.223 ms`
  - Dragonfly `SET`: `75248.37 ops/sec`, `p99 0.551 ms`
  - Dragonfly `GET`: `101747.38 ops/sec`, `p99 0.191 ms`
  - Relative to Dragonfly, Garnet was `-35.90%` on `SET` throughput and `-32.93%` on `GET` throughput.
  - Garnet had better `SET` p99 (`-30.49%`) but worse `GET` p99 (`+16.75%`).

## Flamegraph notes

- `sample` warned `Stack count is low` on both captures, so treat the macOS flamegraph as directional, not exhaustive.
- GET remained dominated by runtime scheduling and wakeup cost rather than value decode/serialization.
- SET showed the same boundary-heavy shape plus visible `HashIndex::find_tag_address` work.
- The current hypothesis still holds: for plain GET/SET, the main cost is crossing `connection -> owner-thread -> wakeup/socket` boundaries more than the core value path itself.

## Interpretation

- The misleading local comparison was worth keeping because it shows how much Docker Desktop can distort a cross-engine readout.
- Under matched Linux container conditions, Dragonfly is still ahead on plain GET/SET throughput on this machine.
- The hottest Garnet work remains the same class of cost as before: handoff, wakeup, synchronization, and socket activity.
- The immediate engineering takeaway is not "optimize value codecs first"; it is "reduce boundary and wakeup overhead first, then retest on real Linux hardware."
