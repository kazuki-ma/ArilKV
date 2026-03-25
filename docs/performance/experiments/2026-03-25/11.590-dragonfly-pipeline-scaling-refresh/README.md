# 11.590 Dragonfly pipeline scaling refresh

- status: measurement-only
- before_commit: `c5b92fce60997b3cbf16a2e1eb02da83fd4af779`
- after_commit: `c5b92fce60997b3cbf16a2e1eb02da83fd4af779`
- revert_commit: not applicable

## Goal

Measure how the latest matched Docker/Linux owner-inline Garnet vs Dragonfly
gap changes as `PIPELINE` increases.

The question here is not absolute peak throughput in a different harness. It is
whether the current plain `GET`/`SET` lead survives, narrows, or flips when the
request path becomes progressively more batched.

## Benchmark shape

- Harness: `garnet-rs/benches/docker_linux_perf_diff_profile.sh`
- Targets: `garnet`, `dragonfly`
- Workloads: `set`, `get`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=50000`
- `PRELOAD_REQUESTS=50000`
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
- Primary sweep: `PIPELINE=1,4,16,64`
- Tie-break reruns: `PIPELINE=16,64`

## Benchmark commands

```bash
for p in 1 4 16 64; do
  OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-25/11.590-dragonfly-pipeline-scaling-refresh/results/pipeline-${p} \
  THREADS=1 \
  CONNS=4 \
  REQUESTS=50000 \
  PRELOAD_REQUESTS=50000 \
  PIPELINE="${p}" \
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
done
```

```bash
for p in 16 64; do
  OUTDIR_HOST=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-25/11.590-dragonfly-pipeline-scaling-refresh/results-rerun/pipeline-${p} \
  THREADS=1 \
  CONNS=4 \
  REQUESTS=50000 \
  PRELOAD_REQUESTS=50000 \
  PIPELINE="${p}" \
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
done
```

## Result

### Primary sweep

| Pipeline | Garnet SET ops | Dragonfly SET ops | SET delta | Garnet GET ops | Dragonfly GET ops | GET delta | Garnet SET p99 | Dragonfly SET p99 | Garnet GET p99 | Dragonfly GET p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 155640.57 | 81482.43 | +91.01% | 166482.01 | 81919.04 | +103.23% | 0.071 ms | 0.103 ms | 0.071 ms | 0.095 ms |
| 4 | 399041.50 | 216443.42 | +84.36% | 516266.26 | 220355.81 | +134.29% | 0.079 ms | 0.143 ms | 0.079 ms | 0.143 ms |
| 16 | 505525.39 | 516110.39 | -2.05% | 840283.18 | 594666.44 | +41.30% | 0.271 ms | 0.207 ms | 0.151 ms | 0.159 ms |
| 64 | 673067.54 | 969527.74 | -30.58% | 1032274.05 | 975638.31 | +5.80% | 0.695 ms | 0.535 ms | 0.383 ms | 0.487 ms |

### Tie-break reruns

| Pipeline | Garnet SET ops | Dragonfly SET ops | SET delta | Garnet GET ops | Dragonfly GET ops | GET delta | Garnet SET p99 | Dragonfly SET p99 | Garnet GET p99 | Dragonfly GET p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 rerun | 510428.04 | 534253.67 | -4.46% | 789596.28 | 573540.84 | +37.67% | 0.231 ms | 0.199 ms | 0.183 ms | 0.231 ms |
| 64 rerun | 638775.59 | 1012981.36 | -36.94% | 1000435.19 | 1031544.63 | -3.02% | 0.855 ms | 0.391 ms | 0.439 ms | 0.431 ms |

## Interpretation

- `PIPELINE=1` と `4` では Garnet がまだ明確に上です。throughput も p99 も両 workload で Garnet 優勢でした。
- `PIPELINE=16` では差がはっきり縮みます。`GET` は 2 回とも Garnet が `+37%` から `+41%` で勝っていますが、`SET` は 2 回ともほぼ並びか微負けです。
- `PIPELINE=64` では `SET` は 2 回とも Dragonfly 優勢に反転しました。差分は `-30.58%` と `-36.94%` です。
- `GET` は `PIPELINE=64` でほぼ拮抗まで縮みます。2 回の結果は `+5.80%` と `-3.02%` で、roughly parity と見るのが正確です。
- p99 は低 pipeline では Garnet が一貫して良いですが、高 pipeline では `SET` tail が Garnet 側で悪化しました。`GET` tail は `PIPELINE=16` では Garnet 優勢、`PIPELINE=64` ではほぼ並びです。

要するに、pipeline を上げるほど Garnet の優位は縮みます。現状の matched owner-inline shape では:

- low pipeline: Garnet clearly ahead
- medium pipeline: GET still ahead, SET near parity
- high pipeline: GET roughly parity, SET Dragonfly ahead

## Perf note

- `PIPELINE=1` は両 engine とも top sampled symbol が `__wake_up_sync_key` で、まだ wakeup/loopback 支配です。
- `PIPELINE=4` 以降は `__kernel_clock_gettime` が前に出始めます。
- `PIPELINE=16` rerun の Garnet `SET` top は `HashIndex::find_tag_entry`、`PIPELINE=64` primary の Garnet `SET` top は `malloc` でした。
- `PIPELINE=64` rerun の Dragonfly `GET` top は `dfly::MultiCommandSquasher::TrySquash(...)` で、batched command handling 側の寄与が見えています。

## Validation

- Measurement-only iteration.
- No runtime or test code changed.
- No additional `make test-server` run was required for this refresh.

## Artifacts

- `summary.txt`
- `comparison.txt`
- `runs.csv`
- `perf-top.txt`
- `diff.patch`
