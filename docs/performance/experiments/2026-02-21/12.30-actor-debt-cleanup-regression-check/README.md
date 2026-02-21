# Experiment 12.30 - Owner-Thread Routing Refactor Regression Check

## Metadata

- experiment_id: `12.30`
- date: `2026-02-21`
- before_commit: `86de843ba5720440b421cdb268d2b480689950a6`
- after_commit: `003b46281b7c1e8f75ce68435c2b94284b66fac6`
- commit_message: `refactor: centralize owner-thread frame execution routing`
- full_diff: `docs/performance/experiments/2026-02-21/12.30-actor-debt-cleanup-regression-check/diff.patch`

## Goal

Remove owner-thread routing duplication (connection handler, transaction queue, replication apply path) while confirming there is no throughput/latency regression before adding more Redis commands.

## Code Change Summary

- Added `execute_frame_on_owner_thread(...)` to centralize:
  - shard-owner selection
  - frame argument capture
  - owner-thread execution
  - error mapping (`Protocol`, `Request`, `OwnerThreadUnavailable`)
- Switched three call sites to the shared helper:
  - `connection_handler.rs`
  - `connection_transaction.rs`
  - `redis_replication.rs`

## Benchmark Procedure

1. Build baseline binary at the parent commit:
   ```bash
   cd /tmp/garnet-base-86de843ba5/garnet-rs
   cargo build --release -p garnet-server
   ```
2. Build current binary:
   ```bash
   cd /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs
   cargo build --release -p garnet-server
   ```
3. Run local A/B benchmark:
   ```bash
   cd /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet
   BASE_BIN=/tmp/garnet-base-86de843ba5/garnet-rs/target/release/garnet-server \
   NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
   BASE_LABEL=before_86de843ba5 \
   NEW_LABEL=after_worktree_refactor \
   OUTDIR=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-02-21/12.30-actor-debt-cleanup-regression-check/artifacts/ab_86de_vs_worktree \
   RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 PRELOAD_REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-1024 PORT=16431 \
   garnet-rs/benches/binary_ab_local.sh
   ```

## Results Snapshot

Source: `artifacts/ab_86de_vs_worktree/comparison.txt`

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| median_set_ops | 164612 | 171282 | +4.05% |
| median_get_ops | 170328 | 175819 | +3.22% |
| median_set_p99_ms | 0.431 | 0.367 | -14.85% |
| median_get_p99_ms | 0.335 | 0.303 | -9.55% |

Interpretation: no regression observed for this refactor. Throughput and p99 both improved in this run.

## Diff Excerpt

```diff
+pub(crate) fn execute_frame_on_owner_thread(
+    processor: &Arc<RequestProcessor>,
+    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
+    args: &[ArgSlice],
+    command: CommandId,
+    frame: &[u8],
+) -> Result<Vec<u8>, OwnerThreadExecutionError> {
```

```diff
-                let shard_index = owner_shard_for_command(
-                    &processor,
-                    &args[..meta.argument_count],
-                    command,
-                );
-                ...
-                match owner_thread_pool.execute_sync(shard_index, move || {
-                    execute_owned_frame_args_via_processor(&routed_processor, &owned_args)
-                }) {
+                match execute_frame_on_owner_thread(
+                    &processor,
+                    &owner_thread_pool,
+                    &args[..meta.argument_count],
+                    command,
+                    &receive_buffer[frame_start..frame_end],
+                ) {
```

## Artifacts

- `artifacts/ab_86de_vs_worktree/comparison.txt`
- `artifacts/ab_86de_vs_worktree/comparison.stdout.txt`
- `artifacts/ab_86de_vs_worktree/before_86de843ba5/summary.txt`
- `artifacts/ab_86de_vs_worktree/before_86de843ba5/runs.csv`
- `artifacts/ab_86de_vs_worktree/after_worktree_refactor/summary.txt`
- `artifacts/ab_86de_vs_worktree/after_worktree_refactor/runs.csv`
- `diff.patch`
