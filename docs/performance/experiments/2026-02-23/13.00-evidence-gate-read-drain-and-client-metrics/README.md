# Experiment 13.00 - Evidence Gate for Read-Drain and Client-Metrics Fast Path

## Metadata

- experiment_id: `13.00`
- date: `2026-02-23`
- base_commit: `7973166495`
- accepted_change: `connection_handler` read-drain path
- rejected_change: `ServerMetrics` tracked-client fast path
- full_diff: `docs/performance/experiments/2026-02-23/13.00-evidence-gate-read-drain-and-client-metrics/diff.patch`

## Goal

Apply an explicit evidence gate before optimization:

1. keep only changes with measurable wins under repeated A/B runs
2. reject changes without clear hotspot evidence or with mixed results

## Candidate Changes

1. `opt1` (accepted): `handle_connection` reads once with `await`, then drains immediately available bytes via `try_read` loop.
2. `opt2` (rejected): `ServerMetrics::set_client_last_command` lock-avoidance path using tracked-client count.

## Benchmark Procedure

All runs were executed via:

```bash
cd garnet-rs
BASE_BIN=<binary A> NEW_BIN=<binary B> \
RUNS=3 THREADS=1 CONNS=4 REQUESTS=12000 PRELOAD_REQUESTS=12000 PIPELINE=1 SIZE_RANGE=1-256 \
./benches/binary_ab_local.sh
```

Comparison sets:

1. `base` vs `opt1+opt2`:
   - `artifacts/ab_base_vs_new_combined/`
2. `base` vs `opt1`:
   - `artifacts/ab_base_vs_opt1/`
3. `opt1` vs `opt1+opt2`:
   - `artifacts/ab_opt1_vs_opt1_opt2/`

## Results Snapshot

From `comparison.txt` files:

| Comparison | SET ops | GET ops | SET p99 | GET p99 |
|---|---:|---:|---:|---:|
| `base` -> `opt1+opt2` | `+4.76%` | `-0.56%` | `-7.77%` | `0.00%` |
| `base` -> `opt1` | `+4.20%` | `-0.21%` | `0.00%` | `+8.42%` |
| `opt1` -> `opt1+opt2` | `+1.88%` | `-0.07%` | `-7.77%` | `+8.42%` |

Interpretation:

- `opt1` shows a repeatable SET throughput gain and is low risk; keep it.
- `opt2` impact is mixed and not tied to a clear flamegraph-top hotspot in this setup; reject it for now.

## Hotspot Relevance Check

We checked differential Linux `perf` reports from:

- `garnet-rs/benches/results/linux-perf-diff-docker-20260223-011150/`
- `garnet-rs/benches/results/linux-perf-diff-docker-20260223-011435/`

Observed top weight remains dominated by scheduler/wakeup and owner-thread execution paths (for example `try_to_wake_up`, Tokio worker park/run, owner-thread execute sync). There is no strong direct signal that the rejected `ServerMetrics` path is a dominant hotspot.

## Decision

1. Accept `opt1` (`connection_handler` read-drain).
2. Reject `opt2` (`ServerMetrics` tracked-client fast path).

## Artifacts

- `artifacts/ab_base_vs_new_combined/comparison.txt`
- `artifacts/ab_base_vs_opt1/comparison.txt`
- `artifacts/ab_opt1_vs_opt1_opt2/comparison.txt`
- per-run logs/CSV/summary files under each artifact directory.
