# Experiment 12.31 - Command Catalog Single Source of Truth

## Metadata

- experiment_id: `12.31`
- date: `2026-02-21`
- before_commit: `ae3db76eb427810615c542d8aabf723e519eacb0`
- after_commit: `01dbc5f528963145696ee88d3a8f722571328adb`
- commit_message: `refactor: unify command metadata for dispatch and routing specs`
- full_diff: `docs/performance/experiments/2026-02-21/12.31-command-catalog-sso/diff.patch`

## Goal

Reduce command-definition duplication before adding new Redis commands by making `command_spec` the single metadata source for:

- `CommandId` ownership
- name-to-id lookup for non-hot dispatch path
- command name metadata used by routing/arity/COMMAND output

## Code Change Summary

- Moved `CommandId` definition from `command_dispatch.rs` to `command_spec.rs`.
- Added centralized command metadata table in `command_spec.rs`.
- Switched `command_dispatch` slow path from repeated name-matching branches to `command_spec::command_id_from_name(...)`.
- Kept hot fast-path checks (`GET/SET/DEL/INCR`) in `dispatch_command_name`.

## Benchmark Procedure

1. Build baseline binary:
   ```bash
   cd /tmp/garnet-base-ae3db76eb4/garnet-rs
   cargo build --release -p garnet-server
   ```
2. Build changed binary:
   ```bash
   cd /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs
   cargo build --release -p garnet-server
   ```
3. Run A/B gate (noise-reduced run used for decision):
   ```bash
   cd /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet
   BASE_BIN=/tmp/garnet-base-ae3db76eb4/garnet-rs/target/release/garnet-server \
   NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
   BASE_LABEL=before_ae3db76 \
   NEW_LABEL=after_command_catalog_sso \
   OUTDIR=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-02-21/12.31-command-catalog-sso/artifacts/ab_r5 \
   RUNS=5 THREADS=4 CONNS=8 REQUESTS=5000 PRELOAD_REQUESTS=5000 PIPELINE=1 SIZE_RANGE=1-1024 PORT=16451 \
   garnet-rs/benches/binary_ab_local.sh
   ```

## Results Snapshot

Source: `artifacts/ab_r5/comparison.txt`

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| median_set_ops | 164618 | 165667 | +0.64% |
| median_get_ops | 170816 | 170203 | -0.36% |
| median_set_p99_ms | 0.391 | 0.383 | -2.05% |
| median_get_p99_ms | 0.335 | 0.343 | +2.39% |

Interpretation: within near-neutral range for this local setup; no material throughput regression observed after command-catalog refactor.

## Diff Excerpt

```diff
-use crate::command_dispatch::CommandId;
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+#[repr(u8)]
+pub enum CommandId {
+    ...
+}
```

```diff
-    dispatch_command_name_slow(command)
+    command_id_from_name(command).unwrap_or(CommandId::Unknown)
```

## Artifacts

- `artifacts/ab_r5/comparison.txt`
- `artifacts/ab_r5/comparison.stdout.txt`
- `artifacts/ab_r5/before_ae3db76/summary.txt`
- `artifacts/ab_r5/before_ae3db76/runs.csv`
- `artifacts/ab_r5/after_command_catalog_sso/summary.txt`
- `artifacts/ab_r5/after_command_catalog_sso/runs.csv`
- `diff.patch`
