# 11.605 visible connected-client count fast path

- status: accepted
- before_commit: `2a0915d3fe3002f5d68b7de4d47730e805b9966c`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Remove the mutex + full-map scan from `metrics.connected_client_count()` on the
plain request loop. This count is refreshed on every request in
`connection_handler`, so paying `clients.lock()` plus `count(!killed)` each time
is unnecessary if we can maintain the same visible-client count incrementally.

## Change

- added `visible_connected_clients: AtomicU64` to `ServerMetrics`
- incremented the counter only when `register_client(...)` actually inserts a
  live client entry
- decremented the counter only on the first visible -> killed transition in
  `mark_clients_killed(...)` and `kill_clients(...)`
- decremented on `unregister_client(...)` only when the removed client was still
  visible, so killed clients are not double-counted on disconnect
- changed `connected_client_count()` from `clients.lock() + count(!killed)` to a
  relaxed atomic load
- added exact regressions for
  `register -> mark killed -> unregister` and repeated `kill_clients(...)`
  transitions

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server connected_client_count_tracks_mark_killed_and_unregister_without_double_counting -- --nocapture`
- `cargo test -p garnet-server kill_clients_decrements_connected_client_count_once_per_visible_client -- --nocapture`
- `cargo test -p garnet-server sync_client_appears_in_info_replication_and_client_kill_type_slave_disconnects_it -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `719 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Result

Both benchmark shapes accepted.

- owner-inline `PIPELINE=1`: `SET +3.84%`, `GET +3.09%`, `SET p99 -14.41%`, `GET p99 -16.84%`
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: `SET +0.97%`, `GET +8.09%`, `SET p99 flat`, `GET p99 -3.72%`

Raw medians:

- `PIPELINE=1` baseline: `SET 86143`, `GET 103501`, `p99 0.111/0.095 ms`
- `PIPELINE=1` candidate: `SET 89454`, `GET 106697`, `p99 0.095/0.079 ms`
- `PIPELINE=4` baseline: `SET 233341`, `GET 309795`, `p99 4.735/3.439 ms`
- `PIPELINE=4` candidate: `SET 235596`, `GET 334860`, `p99 4.735/3.311 ms`

## Interpretation

- This is the kind of hot-path bookkeeping cut that matches the framegraph:
  the request loop was paying a lock and a full client-map walk for a value that
  changes only on connection lifecycle transitions.
- The counter update points are straightforward and now explicitly protected
  against double-decrement on repeated kill or kill-then-drop paths.
- The signal stayed positive in both the narrow owner-inline shape and the
  higher-pressure multithread-client shape, so this is a clean keep.

## Artifacts

- `comparison-p1.txt`
- `comparison-p4.txt`
- `diff.patch`
