# 11.588 command-id last-command cache

Status: dropped

Goal:
- take a more targeted shot at the `last_command_matches(...)` work that still
  sits on the plain request path
- replace repeated byte-wise case-insensitive compares for common top-level
  commands with a cached `CommandId` fast path
- keep `CLIENT LIST` / `CLIENT INFO` output unchanged

Candidate:
- added `last_command_id` and `last_command_has_subcommand` to `ClientRuntimeInfo`
- introduced `add_client_input_bytes_and_command(...)` so the hot connection loop
  could pass the parsed `CommandId`
- changed the repeated-command fast path to skip the byte compare when the new
  request is a known top-level command with no subcommand and the cached
  `CommandId` already matches
- added exact regression
  `combined_input_and_last_command_caches_repeated_simple_command_ids`

Validation while the candidate was live:
- `cargo test -p garnet-server combined_input_and_last_command_caches_repeated_simple_command_ids -- --nocapture`
- `cargo test -p garnet-server combined_input_and_last_command_updates_counters_activity_and_lowercased_command -- --nocapture`
- `cargo test -p garnet-server client_last_command_stores_lowercased_command_and_subcommand -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `make fmt`
- `make fmt-check`

Benchmark evidence:
- run 1: [comparison-run1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.588-command-id-last-command-cache-drop/comparison-run1.txt)
- run 2: [comparison-run2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.588-command-id-last-command-cache-drop/comparison-run2.txt)

Decision:
- drop

Reason:
- the first owner-inline rerun improved `SET` but clearly regressed `GET`:
  `SET +6.10%`, `GET -4.30%`, flat p99
- the identical rerun then flipped the other way: `SET -1.02%`,
  `GET +14.36%`, `SET p99 +11.27%`
- with throughput direction changing across runs and the write tail getting worse
  on the tie-break rerun, the signal is too unstable to justify carrying extra
  cached state in `ClientRuntimeInfo`
