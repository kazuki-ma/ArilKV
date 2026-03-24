# 11.579 request-local time snapshot

Status: dropped

Goal:
- reuse one request-local timestamp inside `RequestProcessor::execute_bytes_in_db(...)`
- cut repeated `Instant::now()` / `SystemTime::now()` calls from helpers reached by plain `GET` / `SET`

Shapes tried:
- broad: capture both `Instant` and UNIX micros once per request and expose them through request-local helpers
- narrow: capture only the request-local `Instant` and leave wall-clock helpers unchanged

Validation while the candidate was live:
- `cargo test -p garnet-server request_time_snapshot_scope_reuses_fixed_time_within_request_helpers -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server scripting_time_command_uses_cached_time_like_external_scenario -- --nocapture`

Benchmark evidence:
- broad run 1: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.579-request-time-snapshot-drop/comparison-p1.txt)
- broad rerun: [comparison-p1-r2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.579-request-time-snapshot-drop/comparison-p1-r2.txt)
- narrow final: [comparison-p1-narrow.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.579-request-time-snapshot-drop/comparison-p1-narrow.txt)

Decision:
- drop

Reason:
- the broad version split across identical reruns
- the narrow version still regressed plain `GET` throughput and `GET p99`
- this path adds common-path state even when no script is running, and the cost is not paying for itself in the owner-inline benchmark shape
