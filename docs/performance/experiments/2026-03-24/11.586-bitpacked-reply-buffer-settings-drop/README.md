# 11.586 bitpacked reply-buffer settings

Status: dropped

Goal:
- take another small cut at the `GET`/reply housekeeping path visible in the local framegraph
- reduce `reply_buffer_settings_snapshot()` from three relaxed atomic loads plus per-field decoding to one load
- keep the admin/config surface unchanged while shrinking the common-path snapshot work

Candidate:
- replaced `reply_buffer_peak_reset_time_millis`, `reply_buffer_resizing_enabled`, and `reply_buffer_copy_avoidance_enabled` with one bitpacked `reply_buffer_settings_bits: AtomicU64`
- added encode/decode helpers for `ReplyBufferSettings`
- updated the three setter methods to use a compare-exchange update loop
- added a narrow regression `reply_buffer_settings_snapshot_tracks_independent_updates`

Validation while the candidate was live:
- `cargo test -p garnet-server reply_buffer_settings_snapshot_tracks_independent_updates -- --nocapture`
- `cargo test -p garnet-server reply_buffer_limits_match_external_scenario -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `make fmt`
- `make fmt-check`

Benchmark evidence:
- run 1: [comparison-run1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.586-bitpacked-reply-buffer-settings-drop/comparison-run1.txt)
- run 2: [comparison-run2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.586-bitpacked-reply-buffer-settings-drop/comparison-run2.txt)

Decision:
- drop

Reason:
- the first owner-inline rerun regressed the read path in the exact shape this slice was meant to help: `SET +1.36%`, `GET -5.69%`, `GET p99 +12.70%`
- the identical rerun recovered to a small all-green result (`SET +2.17%`, `GET +1.87%`, flat p99), but that is too weak and too inconsistent to justify adding a CAS loop plus bitpacking complexity to the config path
- because the signal did not converge to a repeatable win, the runtime/test changes were reverted and only this artifact note was kept
