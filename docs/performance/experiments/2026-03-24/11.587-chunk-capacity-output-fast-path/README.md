# 11.587 chunk-capacity output fast path

- status: accepted
- before_commit: `ff6accef69eb2f61274ee571c93f9e2a6ef67f88`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Take the next small evidence-driven cut in the reply/output bookkeeping path
that is still visible behind the socket leaves in the local framegraph.

On the default hot path, `add_client_output_bytes(...)` was loading reply-buffer
settings on every response even when the client reply buffer was already at the
fixed 16 KiB chunk capacity, where the settings cannot affect resize behavior.

## Change

- added a dedicated `observe_reply_bytes_at_chunk_capacity(...)` helper on
  `ClientRuntimeInfo`
- changed `add_client_output_bytes(...)` to return early for `bytes == 0`
- changed `add_client_output_bytes(...)` to skip
  `reply_buffer_settings_snapshot()` when `reply_buffer_size` is already
  `REPLY_BUFFER_CHUNK_BYTES`, using the new helper instead
- added exact regression
  `add_client_output_bytes_updates_chunk_capacity_reply_buffer_state`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server add_client_output_bytes_updates_chunk_capacity_reply_buffer_state -- --nocapture`
- `cargo test -p garnet-server reply_buffer_limits_match_external_scenario -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `714 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Result

Both identical owner-inline reruns were clean wins:

- run 1: `SET +4.14%`, `GET +0.22%`, `SET p99 -10.13%`, `GET p99 flat`
- run 2: `SET +7.50%`, `GET +0.25%`, `SET p99 flat`, `GET p99 flat`

Raw medians:

- run 1 baseline: `SET 121628`, `GET 151940`, `p99 0.079/0.071 ms`
- run 1 candidate: `SET 126663`, `GET 152267`, `p99 0.071/0.071 ms`
- run 2 baseline: `SET 125983`, `GET 156947`, `p99 0.071/0.071 ms`
- run 2 candidate: `SET 135433`, `GET 157347`, `p99 0.071/0.071 ms`

## Interpretation

- The change is small, local, and aligned with the framegraph suspicion: most
  benchmark clients stay on the default 16 KiB reply-buffer size, so loading
  reply-buffer config on every output event was dead work there.
- The read-path gain is modest, but it stayed non-negative in both reruns while
  the write path improved more clearly, so this is a clean keep.

## Artifacts

- `comparison-run1.txt`
- `comparison-run2.txt`
- `diff.patch`
