# 11.582 combined output and command metrics update

Status: dropped

Goal:
- cut one client-table lock from the common request finalize path
- merge `add_client_output_bytes(...)` and `add_client_commands_processed(...)`
- leave input-byte accounting and `set_client_last_command(...)` unchanged

Candidate:
- added a narrow helper that updated `total_output_bytes`, `total_commands`, `last_activity`, and reply-buffer observation under one lock
- rewired only `finalize_client_command(...)` to use it

Validation while the candidate was live:
- `cargo test -p garnet-server combined_output_and_command_metrics_updates_client_counters_and_activity -- --nocapture`
- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Benchmark evidence:
- run 1: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.582-combined-output-and-command-metrics-drop/comparison-p1.txt)
- run 2: [comparison-p1-r2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.582-combined-output-and-command-metrics-drop/comparison-p1-r2.txt)
- run 3: [comparison-p1-r3.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.582-combined-output-and-command-metrics-drop/comparison-p1-r3.txt)

Decision:
- drop

Reason:
- first two runs were effectively noise-level and split by path
- the tie-break rerun was clearly bad on plain `GET`
- this helper adds API surface without enough repeatable win
