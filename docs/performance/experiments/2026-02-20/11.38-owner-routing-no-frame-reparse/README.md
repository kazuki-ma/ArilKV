# Experiment 11.38 - Owner Routing Without Frame Reparse

## Metadata

- experiment_id: `11.38`
- date: `2026-02-20`
- before_commit: `fd76470370d57921b878a86773edf4a295f2d71c`
- after_commit: `7438375d5749b5f72281b77301dd701ee76c7fa4`
- commit_message: `perf: avoid frame reparse in owner-routed execution`
- full_diff: `docs/performance/experiments/2026-02-20/11.38-owner-routing-no-frame-reparse/diff.patch`

## Goal

Reduce owner-thread path overhead by eliminating full RESP frame copy+reparse in routed execution.

## Before vs After Implementation

- Before
  - In `handle_connection`, owner-routed requests copied the entire frame bytes.
  - Worker closure called `execute_frame_via_processor`, which reparsed RESP before `RequestProcessor::execute`.
- After
  - In `handle_connection`, owner-routed requests copy parsed args (`Vec<Vec<u8>>`).
  - Worker closure calls `execute_owned_args_via_processor`, which directly executes without frame reparse.

## Detailed Commands and Test-by-Test Results

1. Unit test (new path parity)
   - command: `cd garnet-rs && cargo test -p garnet-server --lib execute_owned_args_via_processor_matches_direct_execution -- --nocapture`
   - result: `running 1 test`, `1 passed`
2. Unit test (existing frame path parity)
   - command: `cd garnet-rs && cargo test -p garnet-server --lib execute_frame_via_processor_matches_direct_execution -- --nocapture`
   - result: `running 1 test`, `1 passed`
3. Unit test (routing selection)
   - command: `cd garnet-rs && cargo test -p garnet-server --lib owner_routed_shard_selection_handles_single_and_multi_key_commands -- --nocapture`
   - result: `running 1 test`, `1 passed`
4. Integration compatibility test with owner routing enabled
   - command: `cd garnet-rs && GARNET_STRING_OWNER_THREADS=2 cargo test -p garnet-server --test redis_cli_compat -- --nocapture`
   - result: `running 1 test`, `1 passed`
5. Server loop regression checks
   - command: `cd garnet-rs && cargo test -p garnet-server --lib tests::accept_loop_spawns_connection_handlers -- --nocapture`
   - result: `running 1 test`, `1 passed`
   - command: `cd garnet-rs && cargo test -p garnet-server --lib tests::shutdown_signal_stops_accept_loop -- --nocapture`
   - result: `running 1 test`, `1 passed`
6. Full server lib regression sweep (post-change)
   - command: `cd garnet-rs && cargo test -p garnet-server --lib -- --nocapture`
   - result: `running 95 tests`, `95 passed`

## Benchmark A/B (Primary)

Command:

```bash
cd garnet-rs
BASE_BIN=/tmp/garnet-server-shardfnv \
NEW_BIN=/tmp/garnet-server-ownedargs \
BASE_LABEL=frame NEW_LABEL=owned \
RUNS=3 THREADS=8 CONNS=16 REQUESTS=10000 PRELOAD_REQUESTS=10000 \
PIPELINE=32 STRING_STORE_SHARDS=2 OWNER_THREADS=2 \
PORT=16460 OUTDIR=/tmp/garnet-owner2-ab-ownedargs-20260220-144824 \
./benches/binary_ab_local.sh
```

Source: `artifacts/owner2-p32-comparison.txt`

| Metric | Frame | Owned | Delta |
|---|---:|---:|---:|
| median_set_ops | 402749.000 | 403331.000 | +0.14% |
| median_get_ops | 409215.000 | 416236.000 | +1.72% |
| median_set_p99_ms | 49.91900 | 49.40700 | -1.03% |
| median_get_p99_ms | 24.06300 | 20.73500 | -13.83% |

## Benchmark A/B (Owner Disabled Smoke)

Command:

```bash
cd garnet-rs
BASE_BIN=/tmp/garnet-server-shardfnv \
NEW_BIN=/tmp/garnet-server-ownedargs \
BASE_LABEL=frame NEW_LABEL=owned \
RUNS=1 THREADS=8 CONNS=16 REQUESTS=10000 PRELOAD_REQUESTS=10000 \
PIPELINE=1 STRING_STORE_SHARDS=2 OWNER_THREADS=0 \
PORT=16470 OUTDIR=/tmp/garnet-owner0-ab-ownedargs-smoke-20260220-145108 \
./benches/binary_ab_local.sh
```

Source: `artifacts/owner0-p1-smoke-comparison.txt`

| Metric | Frame | Owned | Delta |
|---|---:|---:|---:|
| median_set_ops | 167658.000 | 167636.000 | -0.01% |
| median_get_ops | 169134.000 | 169043.000 | -0.05% |
| median_set_p99_ms | 1.83100 | 1.72700 | -5.68% |
| median_get_p99_ms | 1.55900 | 1.47900 | -5.13% |

## Diff Excerpt

```diff
- let frame = receive_buffer[frame_start..frame_end].to_vec();
- execute_frame_via_processor(&routed_processor, &frame)
+ let mut owned_args = Vec::with_capacity(meta.argument_count);
+ for arg in &args[..meta.argument_count] {
+     owned_args.push(unsafe { arg.as_slice() }.to_vec());
+ }
+ execute_owned_args_via_processor(&routed_processor, &owned_args)
```

```diff
+#[test]
+fn execute_owned_args_via_processor_matches_direct_execution() {
+    // parity with direct execute path
+}
```

## Artifacts

- `artifacts/owner2-p32-comparison.txt`
- `artifacts/owner2-p32-frame-summary.txt`
- `artifacts/owner2-p32-owned-summary.txt`
- `artifacts/owner2-p32-frame-runs.csv`
- `artifacts/owner2-p32-owned-runs.csv`
- `artifacts/owner0-p1-smoke-comparison.txt`
- `artifacts/owner0-p1-smoke-frame-summary.txt`
- `artifacts/owner0-p1-smoke-owned-summary.txt`
- `diff.patch`
