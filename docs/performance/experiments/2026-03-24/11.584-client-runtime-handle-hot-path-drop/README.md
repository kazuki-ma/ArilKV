# 11.584 client runtime handle hot path

Status: dropped

Goal:
- test a larger client-metrics design cut after the small accepted lock-coalescing win
- remove the global `ServerMetrics.clients` lookup/lock from the plain request path
- keep the global client index only for `CLIENT INFO/LIST`, kill/filtering, and other cross-connection surfaces

Candidate:
- changed `ServerMetrics.clients` to store `Arc<parking_lot::Mutex<ClientRuntimeInfo>>`
- added `register_client_with_handle(...)`
- kept a connection-local `ClientRuntimeHandle` inside `handle_connection(...)`
- rewired hot-path metrics calls (`input bytes + last command`, `output bytes`, `commands processed`, and query-buffer observation) to update through that handle instead of re-looking up the client in the global map

Validation while the candidate was live:
- `cargo test -p garnet-server combined_input_and_last_command_updates_counters_activity_and_lowercased_command -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server client_info_and_list_follow_selected_db_and_reset_to_zero -- --nocapture`

Benchmark evidence:
- run 1: [comparison-run1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.584-client-runtime-handle-hot-path-drop/comparison-run1.txt)
- run 2: [comparison-run2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.584-client-runtime-handle-hot-path-drop/comparison-run2.txt)
- run 3: [comparison-run3.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.584-client-runtime-handle-hot-path-drop/comparison-run3.txt)

Decision:
- drop

Reason:
- the signal never converged into a clean all-path win
- run 1 landed at `SET -2.12%`, `GET +0.33%`, `SET p99 +12.70%`
- run 2 flipped to `SET +3.95%`, `GET -0.80%`, p99 flat
- the tie-break run still kept `GET` negative and worsened `GET p99` (`SET +4.41%`, `GET -0.64%`, `GET p99 +12.70%`)
- this is a broad structural change, so a split result is not good enough to keep
