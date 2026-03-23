# 11.564 combined finalize metrics update

- status: dropped
- before_commit: `63d823a732bd05506e17c80f0ae8d34d802d62d0`
- after_commit: unavailable; reverted working-tree experiment
- revert_commit: not applicable

## Goal

Reduce `ServerMetrics.clients` mutex traffic in the command finalize path by combining `add_client_output_bytes(...)` and `add_client_commands_processed(...)` into one lock-taking helper.

## Change

- added a temporary `record_client_command_completion(...)` helper to `ServerMetrics`
- rewired the single finalize-path call site in `connection_handler` to use that helper
- added exact regression `client_command_completion_updates_output_activity_and_command_count_together`
- kept parse-path updates and all existing reply-buffer accounting logic unchanged

## Validation

Targeted Rust tests passed before the benchmark:

- `cargo test -p garnet-server client_command_completion_updates_output_activity_and_command_count_together -- --nocapture`
- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server reply_buffer_limits_match_external_scenario -- --nocapture`

Full gate also passed with the slice applied:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `712 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.564-r1 \
BASE_BIN=/private/tmp/garnet-11.564-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.564-current-target/release/garnet-server \
BASE_LABEL=split_finalize_metrics_updates \
NEW_LABEL=combined_finalize_metrics_update \
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

This regressed badly on the first owner-inline run and was dropped immediately:

- `SET ops 105613 -> 85523` (`-19.02%`)
- `GET ops 140352 -> 123100` (`-12.29%`)
- `SET p99 0.087 -> 0.199 ms` (`+128.74%`)
- `GET p99 0.079 -> 0.087 ms` (`+10.13%`)

The per-run data also stayed worse rather than looking like a single noisy outlier, so there was no reason to keep or rerun this shape.

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
