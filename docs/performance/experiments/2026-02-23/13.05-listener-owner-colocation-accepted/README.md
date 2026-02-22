# Experiment 13.05 - Listener/Owner Co-Location in Multi-Port Mode (Accepted)

## Metadata

- experiment_id: `13.05`
- date: `2026-02-23`
- base_commit: `ff09d456bf`
- candidate: remove listener->owner cross-thread handoff on actor path
- outcome: **accepted** (clear throughput and latency wins)
- full_diff: `docs/performance/experiments/2026-02-23/13.05-listener-owner-colocation-accepted/diff.patch`

## Goal

Align runtime behavior with the actor design intent for multi-port mode:

1. one listener thread per port
2. same thread executes owner-routed command work
3. avoid cross-thread handoff/wakeup overhead for each request

## Change Under Test

Files:

- `garnet-rs/crates/garnet-server/src/shard_owner_threads.rs`
- `garnet-rs/crates/garnet-server/src/connection_owner_routing.rs`
- `garnet-rs/crates/garnet-server/src/connection_handler.rs`
- `garnet-rs/crates/garnet-server/src/main.rs`

Summary:

1. Added inline owner-execution backend (`ShardOwnerThreadPool::new_inline`) and `is_inline_execution()`.
2. Added fast path in owner routing to execute directly on the listener thread when inline mode is active.
3. Added `GARNET_OWNER_EXECUTION_INLINE` env parsing in pool builder.
4. Defaulted multi-port launch path to inline owner execution when the env var is not explicitly set.

## Benchmark Procedure

Harness: `garnet-rs/benches/binary_ab_local.sh`

A/B labels:

- `pinning_before`: baseline (`ff09d456bf`) wrapper with pinning mode
- `pinning_inline`: candidate binary wrapper with inline owner execution

### 1-thread run

- `RUNS=5`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=12000`
- `PRELOAD_REQUESTS=12000`
- `PIPELINE=1`
- `SIZE_RANGE=1-256`

### 4-thread run

- `RUNS=5`
- `THREADS=4`
- `CONNS=8`
- `REQUESTS=5000`
- `PRELOAD_REQUESTS=5000`
- `PIPELINE=1`
- `SIZE_RANGE=1-1024`

## Results

From artifact `comparison.txt` files:

### 1-thread (`artifacts/ab_1t/comparison.txt`)

- `SET ops`: `+92.82%`
- `GET ops`: `+116.24%`
- `SET p99`: `-21.62%`
- `GET p99`: `-23.30%`

### 4-thread (`artifacts/ab_4t/comparison.txt`)

- `SET ops`: `+100.55%`
- `GET ops`: `+94.40%`
- `SET p99`: `-51.24%`
- `GET p99`: `-46.32%`

## Validation Gates

1. `cd garnet-rs && cargo test -p garnet-server -- --nocapture`
   - `210 passed` (`src/lib.rs`)
   - `23 passed` (`src/main.rs`)
   - `1 passed` (`tests/redis_cli_compat.rs`)
2. `garnet-rs/tests/interop/redis_runtest_external_subset.sh`
   - `string` unit tests: `6`
   - `incr` unit tests: `4`
   - `keyspace` unit tests: `2`
   - `redis-cli TYPE` probe: PASS

## Decision

Accept the co-located owner execution path for multi-port mode default because it directly matches design intent and shows large A/B wins with no observed regression in current test gates.

## Artifacts

- `artifacts/ab_1t/comparison.txt`
- `artifacts/ab_1t/pinning_before.summary.txt`
- `artifacts/ab_1t/pinning_before.runs.csv`
- `artifacts/ab_1t/pinning_inline.summary.txt`
- `artifacts/ab_1t/pinning_inline.runs.csv`
- `artifacts/ab_4t/comparison.txt`
- `artifacts/ab_4t/pinning_before.summary.txt`
- `artifacts/ab_4t/pinning_before.runs.csv`
- `artifacts/ab_4t/pinning_inline.summary.txt`
- `artifacts/ab_4t/pinning_inline.runs.csv`
- `diff.patch`
