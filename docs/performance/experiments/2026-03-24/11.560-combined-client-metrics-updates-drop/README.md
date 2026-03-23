# 11.560 combined client-metrics updates

- status: dropped
- before_commit: `402e1017fc2d1f03f31472ed678655add0de41a1`
- after_commit: unavailable; reverted working-tree experiment
- revert_commit: not applicable

## Goal

Reduce `ServerMetrics.clients` mutex traffic by combining adjacent per-command client bookkeeping updates.

## Change

- combined parse-path `add_client_input_bytes(...)` and `set_client_last_command(...)` into one lock-taking helper
- combined finalize-path `add_client_output_bytes(...)` and `add_client_commands_processed(...)` into one lock-taking helper
- kept the underlying last-command normalization and reply-buffer accounting logic unchanged

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server client_last_command_stores_lowercased_command_and_subcommand -- --nocapture`
- `cargo test -p garnet-server reply_buffer_limits_match_external_scenario -- --nocapture`
- `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`

All four passed.

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.560-r2 \
BASE_BIN=/tmp/garnet-11.560-baseline-41812/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=split_client_metrics_updates \
NEW_LABEL=combined_client_metrics_updates \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=30000 \
PRELOAD_REQUESTS=30000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

This was dropped because the first apparent win did not survive the rerun.

First run:

- `SET ops 50961 -> 89263` (`+75.16%`)
- `GET ops 56696 -> 100033` (`+76.44%`)
- `SET p99 0.239 -> 0.151 ms` (`-36.82%`)
- `GET p99 0.199 -> 0.111 ms` (`-44.22%`)

Identical rerun:

- `SET ops 138036 -> 125973` (`-8.74%`)
- `GET ops 158998 -> 136573` (`-14.10%`)
- `SET p99 0.071 -> 0.079 ms` (`+11.27%`)
- `GET p99 0.071 -> 0.079 ms` (`+11.27%`)

The reversal is too large to hand-wave away. This needs either a tighter harness or a different design; the code was reverted.

## Artifacts

- `first-comparison.txt`
- `first-before-summary.txt`
- `first-before-runs.csv`
- `first-after-summary.txt`
- `first-after-runs.csv`
- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
