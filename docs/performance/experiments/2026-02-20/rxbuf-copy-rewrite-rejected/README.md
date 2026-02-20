# Experiment - RX Buffer Copy Rewrite (Rejected)

## Metadata

- date: `2026-02-20`
- status: `REJECTED`
- before_commit: `N/A (working-tree-only experiment)`
- after_commit: `N/A (working-tree-only experiment)`
- revert_commit: `N/A (working-tree revert before commit)`
- before_binary: `/tmp/garnet-server-shardfnv`
- candidate_binary: `/tmp/garnet-server-rxbuf-opt`
- outdir_used: `/tmp/garnet-rxbuf-ab-20260220-141251` (later removed to recover disk space)

## Important Process Note

This was executed as a working-tree-only change (no experiment commit).
As a result, `before_commit` / `after_commit` hashes are unavailable.
This gap is the reason `docs/performance/EXPERIMENT_REPORTING_POLICY.md`
now requires one-experiment-one-commit with revert hash when rejected.

## Goal

Try replacing `Vec::drain(..consumed)` with a copy/truncate strategy in
`handle_connection` receive-buffer compaction.

## Candidate Diff (working-tree patch)

```diff
-if consumed > 0 {
-    receive_buffer.drain(..consumed);
-}
+if consumed == receive_buffer.len() {
+    receive_buffer.clear();
+} else {
+    let remaining = receive_buffer.len() - consumed;
+    receive_buffer.copy_within(consumed.., 0);
+    receive_buffer.truncate(remaining);
+}
```

## Benchmark Parameters

The A/B shell loop used these effective parameters:

- `RUNS=3`
- `THREADS=8`
- `CONNS=16`
- `REQ_PER_CLIENT=30000`
- `PIPELINE=1`
- `SIZE_RANGE=1-1024`
- `GARNET_TSAVORITE_STRING_STORE_SHARDS=2`
- workload: `set` then preload+`get` for each run and each binary

## Raw Per-Run Parsed Results

| run | base set ops | base get ops | new set ops | new get ops |
|---|---:|---:|---:|---:|
| 1 | 107822.46 | 169397.28 | 95900.89 | 167328.19 |
| 2 | 110827.42 | 163713.53 | 93701.10 | 168382.83 |
| 3 | 92440.92 | 169090.76 | 93729.18 | 158677.88 |

## Median Outcome

- `base_set_median=107822.46`
- `new_set_median=93729.18`
- `base_get_median=169090.76`
- `new_get_median=167328.19`
- `delta_set_ops=-13.07%`
- `delta_get_ops=-1.04%`
- `base_set_p99_median=2.27100`
- `new_set_p99_median=2.63900`
- `base_get_p99_median=1.65500`
- `new_get_p99_median=1.54300`

Decision:

- Throughput regression on SET was material.
- Change was dropped.

## Additional Notes

- The original script failed in aggregation on `zsh` because `mapfile` is bash-specific.
- The run also surfaced repeated `ERR storage capacity exceeded` lines, reinforcing the need for strict server-error fail-fast checks added in `11.37`.
