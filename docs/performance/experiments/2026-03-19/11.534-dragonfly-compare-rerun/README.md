# 11.534 Dragonfly comparison rerun on an idle host

- Date: 2026-03-19
- Purpose: rerun the recent Garnet vs Dragonfly comparison after the machine became idle, to separate host-load noise from the real cross-engine result.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 818; measurement rerun + docs only)`
- Diff note: no product/runtime code changed in this iteration; this is a pure benchmark rerun using the same fixed Docker/Linux harness from iteration `817`.

## Commands

- Local Garnet median benchmark:
  - `RUNS=3 THREADS=2 CONNS=4 REQUESTS=10000 PRELOAD_REQUESTS=10000 PIPELINE=1 SIZE_RANGE=1-256 OUTDIR=/tmp/garnet-vs-dragonfly-garnet-20260319 ./garnet-rs/benches/perf_regression_gate_local.sh`
- Local Dragonfly comparison:
  - launched `docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0` on `127.0.0.1:16391`
  - ran `memtier_benchmark` with `THREADS=2`, `CONNS=4`, `REQUESTS=10000`, `PRELOAD_REQUESTS=10000`, `PIPELINE=1`, `SIZE_RANGE=1-256` for `SET` and `GET`, `RUNS=3`
- Docker/Linux matched comparison:
  - `THREADS=2 CONNS=4 REQUESTS=10000 PRELOAD_REQUESTS=10000 PIPELINE=1 SIZE_RANGE=1-256 TARGETS='garnet dragonfly' WORKLOADS='set get' OUTDIR_HOST=/tmp/garnet-linux-perf-diff-dragonfly-20260319 ./garnet-rs/benches/docker_linux_perf_diff_profile.sh`

## Results

- Local native-vs-Docker comparison remains heavily skewed by environment mismatch:
  - Garnet median `SET`: `65823 ops/sec`, `p99 0.255 ms`
  - Garnet median `GET`: `70339 ops/sec`, `p99 0.231 ms`
  - Dragonfly median `SET`: `13454.43 ops/sec`, `p99 3.967 ms`
  - Dragonfly median `GET`: `13960.54 ops/sec`, `p99 3.263 ms`
- Docker/Linux matched comparison is still the meaningful slice:
  - Garnet `SET`: `62619.56 ops/sec`, `p99 0.231 ms`
  - Garnet `GET`: `65358.46 ops/sec`, `p99 0.223 ms`
  - Dragonfly `SET`: `92489.29 ops/sec`, `p99 0.231 ms`
  - Dragonfly `GET`: `98088.02 ops/sec`, `p99 0.175 ms`
  - Relative to Dragonfly, Garnet was `-32.30%` on `SET` throughput and `-33.37%` on `GET` throughput.
  - `SET` p99 is now equal; Garnet `GET` p99 is `+27.43%` worse.

## What changed vs 2026-03-18

- Both engines improved materially on `SET` under the matched Docker/Linux run:
  - Garnet `SET ops` `+29.82%`, `SET p99` `-39.69%`
  - Dragonfly `SET ops` `+22.91%`, `SET p99` `-58.08%`
- `GET` changed much less:
  - Garnet `GET ops` `-4.23%`, `GET p99` unchanged
  - Dragonfly `GET ops` `-3.60%`, `GET p99` `-8.38%`

## Interpretation

- Host load clearly mattered for absolute numbers, especially `SET`.
- It did not change the main conclusion from the matched comparison: Dragonfly still leads this plain GET/SET workload by about one-third on throughput.
- What did change is the quality of the `SET` tail: Garnet moved from clearly better `SET p99` to parity.
- The previous local native-vs-Docker result is still not a useful engine comparison and should continue to be treated as a Docker Desktop artifact, not as product evidence.
