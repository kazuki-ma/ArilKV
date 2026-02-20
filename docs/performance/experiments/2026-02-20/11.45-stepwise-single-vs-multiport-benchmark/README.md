# Experiment 11.45 - Stepwise Single-Port vs Multi-Port Benchmark

## Metadata

- experiment_id: `11.45`
- date: `2026-02-20`
- before_commit: `70421660014324ccf9cbf97d0cee15c8ef0f7d4b`
- after_commit: `70421660014324ccf9cbf97d0cee15c8ef0f7d4b`
- commit_message: `benchmark-only (no code change)`
- full_diff: `docs/performance/experiments/2026-02-20/11.45-stepwise-single-vs-multiport-benchmark/diff.patch`

## Goal

Answer the interpretation question: does `4-port` in-process owner-node launch really regress throughput, or was prior comparison not load-matched?

## Step-by-Step Execution and Results

### Step 0: Failed attempts and fixes (recorded)

1. Protocol mismatch
   - issue: used `--protocol resp` (unsupported by local memtier build)
   - fix: switched to `--protocol redis`
2. Capacity error under preload
   - issue: `-ERR storage capacity exceeded`
   - fix: reduced request volume and constrained key-space (`--key-maximum=10000`)
3. Script hang
   - issue: first script used bare `wait` and blocked on background server process
   - fix: wait only on benchmark child PIDs

Artifacts:
- `artifacts/step0-failure-notes.txt`

### Step 1: Existing baseline gate (single-port, median-of-3)

Command:

```bash
cd garnet-rs
RUNS=3 THREADS=8 CONNS=16 REQUESTS=10000 PRELOAD_REQUESTS=10000 \
PIPELINE=1 PORT=17400 OUTDIR=/tmp/garnet-bench-baseline-20260220-221455 \
./benches/perf_regression_gate_local.sh
```

Result:
- `median_set_ops=176590`
- `median_get_ops=178458`
- `median_set_p99_ms=1.111`
- `median_get_p99_ms=0.991`

Artifacts:
- `artifacts/step1-baseline-summary.txt`
- `artifacts/step1-baseline-runs.csv`

### Step 2: Single-port quick run (threads=2, clients=8)

Command shape:

```bash
/opt/homebrew/bin/memtier_benchmark --server 127.0.0.1 --port 17420 \
  --protocol redis --threads 2 --clients 8 --requests 2000 --pipeline 1 \
  --ratio <set-or-get> --key-maximum=10000 --hide-histogram
```

Result:
- `set_ops=163962`, `set_p99_ms=0.271`
- `get_ops=163602`, `get_p99_ms=0.231`

Artifacts:
- `artifacts/step2-singleport-set-threads2-clients8.txt`
- `artifacts/step2-singleport-get-threads2-clients8.txt`

### Step 3: Multi-port aggregate run (4 ports in one process)

Server command:

```bash
GARNET_BIND_ADDR=127.0.0.1:17410 GARNET_OWNER_NODE_COUNT=4 \
  cargo run -p garnet-server --release
```

Per-port benchmark command shape:

```bash
/opt/homebrew/bin/memtier_benchmark --server 127.0.0.1 --port <17410..17413> \
  --protocol redis --threads 2 --clients 8 --requests 2000 --pipeline 1 \
  --ratio <set-or-get> --key-maximum=10000 --hide-histogram
```

Per-port SET ops:
- `17410:43425`
- `17411:43449`
- `17412:43687`
- `17413:43569`

Per-port GET ops:
- `17410:43243`
- `17411:43246`
- `17412:43439`
- `17413:43352`

Aggregate:
- `set_aggregate_ops=174130`
- `get_aggregate_ops=173280`
- `set_max_p99_ms=0.815`
- `get_max_p99_ms=0.727`

Artifacts:
- `artifacts/step3-multiport-set-17410.txt`
- `artifacts/step3-multiport-set-17411.txt`
- `artifacts/step3-multiport-set-17412.txt`
- `artifacts/step3-multiport-set-17413.txt`
- `artifacts/step3-multiport-get-17410.txt`
- `artifacts/step3-multiport-get-17411.txt`
- `artifacts/step3-multiport-get-17412.txt`
- `artifacts/step3-multiport-get-17413.txt`

### Step 4: Fairness control (single-port with total client load matched to Step 3)

Command shape:

```bash
/opt/homebrew/bin/memtier_benchmark --server 127.0.0.1 --port 17430 \
  --protocol redis --threads 8 --clients 8 --requests 2000 --pipeline 1 \
  --ratio <set-or-get> --key-maximum=10000 --hide-histogram
```

Result:
- `set_ops=176355`, `set_p99_ms=0.719`
- `get_ops=174291`, `get_p99_ms=0.751`

Artifacts:
- `artifacts/step4-singleport-fair-set-threads8-clients8.txt`
- `artifacts/step4-singleport-fair-get-threads8-clients8.txt`

## Interpretation

- Step 2 vs Step 3 looked like slight multi-port gain only (`+~6%`), but load was not equivalent.
- Step 3 vs Step 4 (load-matched) is near-parity:
  - `SET ratio = 0.987`
  - `GET ratio = 0.994`
- Therefore, this run does **not** show an inherent 4-port regression; it shows roughly the same host-limited throughput envelope under matched offered load.

Reference summary artifacts:
- `artifacts/step2-step3-step4-summary.json`
- `artifacts/step2-step3-step4-summary.csv`

## Diff Excerpt

```diff
No code change in this experiment (benchmark-only run).
```
