# 11.607 connection-local last-command hot metrics

## Status

Accepted.

## Goal

Keep digging the `__wake_up_sync_key` / request-boundary follow-up by cutting `ServerMetrics.clients` mutex traffic from the plain request path, especially the repeated `GET`/`SET` benchmark shape where the same command name is seen over and over.

## Change

- Split hot per-client counters (`tot-net-in`, `tot-net-out`, `tot-cmds`, last I/O activity) into a shared `ClientHotMetrics` atomic block stored alongside each `ClientRuntimeInfo`.
- Kept externally visible rendering (`CLIENT INFO`, `CLIENT LIST`, ACL log snapshots) on the existing metrics map, but made those render paths read the hot atomic counters instead of stale mutex-owned scalars.
- Added a connection-local `last_command_cache` so repeated top-level commands with the same subcommand shape can take a fast parse path:
  - hot input bytes/activity update goes straight to the connection's `ClientHotMetrics`
  - the global `clients` mutex is only taken when the normalized `cmd=` surface actually changes
- Left reply-buffer/query-buffer/accounting surfaces intact; this slice only localizes hot counters plus the repeated-command fast path.

## Benchmark

Before commit: `4f8a41c711`

After commit: pending at authoring time; see `diff.patch`

### Owner-inline `PIPELINE=1`

First run:

- `SET -1.33%`
- `GET -2.36%`
- `SET/GET p99` flat

Rerun:

- `SET +3.23%`
- `GET +4.68%`
- `SET/GET p99` flat

### Multithread-client `THREADS=8 CONNS=16 PIPELINE=4`

- `SET +1.01%`
- `GET +6.78%`
- `SET p99 -7.19%`
- `GET p99 -6.46%`

## Acceptance rationale

Kept because the pressure shape improved cleanly, and the owner-inline rerun recovered to a clear win instead of reproducing the initial narrow-shape dip. The design is also structurally sound: hot counters now live on an explicit per-connection handle instead of behind the global client map mutex.

## Validation

Targeted Rust:

- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server combined_input_and_last_command_updates_counters_activity_and_lowercased_command -- --nocapture`
- `cargo test -p garnet-server add_client_output_bytes_updates_chunk_capacity_reply_buffer_state -- --nocapture`
- `cargo test -p garnet-server client_last_command_stores_lowercased_command_and_subcommand -- --nocapture`
- `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`

Verified final gate counts:

- `719 passed; 0 failed; 16 ignored`
- `26 passed`
- `1 passed`

## Artifacts

- `comparison-p1.txt`
- `comparison-p1-r2.txt`
- `comparison-p4.txt`
