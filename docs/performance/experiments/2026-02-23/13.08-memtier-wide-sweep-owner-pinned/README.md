# Experiment 13.08 - Memtier Wide Sweep (Owner-Pinned)

## Metadata

- experiment_id: `13.08`
- date: `2026-02-23`
- type: `broad workload sweep`
- target pair: `garnet` vs `dragonfly`

## Goal

Sample a broader memtier workload range (threads/connections/pipeline/size) under the current owner-pinned + inline-owner execution setup.

## Shared Environment

- `SERVER_CPU_SET=0`
- `CLIENT_CPU_SET=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`

Harness:

- `garnet-rs/benches/linux_perf_diff_profile_median_local.sh`
- `RUNS=2` for each case

## Case Matrix

See `artifacts/case_matrix.csv`.

## High-Level Results

From `artifacts/sweep_summary.csv` (`dragonfly_over_garnet_*`):

1. `c1_t1_c4_p1_sz1_256`
   - set `0.552`, get `0.833` (Garnet higher on both)
2. `c2_t4_c8_p1_sz1_1024`
   - set `1.186`, get `1.006` (Dragonfly slightly higher)
3. `c3_t8_c16_p1_sz1_1024`
   - set `1.227`, get `1.008` (Dragonfly higher throughput)
4. `c4_t4_c8_p16_sz1_1024`
   - set `1.300`, get `0.521` (split: Dragonfly higher on set, Garnet much higher on get)

## Interpretation

- Single-thread small-value shape remains favorable for Garnet.
- At higher concurrency with pipeline=1, Dragonfly regains throughput advantage.
- Pipeline-heavy shape (`p16`) is mixed by operation; GET throughput strongly favors Garnet in this run set.

Latency detail is included in `artifacts/sweep_summary_with_p99.csv`.

## Artifacts

- `artifacts/case_matrix.csv`
- `artifacts/shared_env.txt`
- `artifacts/sweep_summary.csv`
- `artifacts/sweep_summary_with_p99.csv`
- `artifacts/c1_t1_c4_p1_sz1_256/{summary.txt,median_summary.csv,runs.csv}`
- `artifacts/c2_t4_c8_p1_sz1_1024/{summary.txt,median_summary.csv,runs.csv}`
- `artifacts/c3_t8_c16_p1_sz1_1024/{summary.txt,median_summary.csv,runs.csv}`
- `artifacts/c4_t4_c8_p16_sz1_1024/{summary.txt,median_summary.csv,runs.csv}`
