# 11.557 client last-command repeat fast path

- status: accepted
- before_commit: `baef4e3b9e073912484ed3ac52e2d873a81e1371`
- after_commit: unavailable; stacked working-tree-only accepted change on top of `11.556`
- revert_commit: not applicable

## Goal

Remove the remaining repeated `client.last_command` rewrite cost from the plain hot path when the normalized command/subcommand did not actually change.

## Change

- added `last_command_matches(...)` to compare the stored normalized `cmd=` bytes with the incoming command/subcommand without allocating
- changed `ServerMetrics::set_client_last_command(...)` to return early when the normalized command is unchanged
- kept the existing lowercased `cmd=` surface for `CLIENT INFO` / `CLIENT LIST`
- added exact regression `client_last_command_stores_lowercased_command_and_subcommand`
- re-ran the `11.556` last-activity regression and the selected-db `CLIENT INFO/LIST` integration regression

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server client_last_command_stores_lowercased_command_and_subcommand -- --nocapture`
- `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`

Full gate:

- `make fmt-check`
- `make test-server`
- verified counts: `710 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.557-r2 \
BASE_BIN=/tmp/garnet-11.557-baseline/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=last_command_always_rewrite \
NEW_LABEL=last_command_repeat_fast_path \
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

The first owner-inline `RUNS=5` A/B was already positive (`first-comparison.txt`), and the identical rerun strengthened it:

- `SET ops 122725 -> 124947` (`+1.81%`)
- `GET ops 143344 -> 147005` (`+2.55%`)
- `SET p99 0.079 -> 0.071 ms` (`-10.13%`)
- `GET p99 0.063 -> 0.063 ms` (`0.00%`)

This is a clean keep. Once `last_activity` stopped calling `Instant::now()` on every command, the remaining work in `set_client_last_command(...)` was just lowercasing and buffer rewrite churn. Repeated-command workloads like plain memtier hit that exact pattern.

## Artifacts

- `comparison.txt`
- `first-comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
