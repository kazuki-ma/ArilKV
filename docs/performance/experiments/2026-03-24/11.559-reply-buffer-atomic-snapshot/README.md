# 11.559 reply-buffer atomic snapshot

- status: accepted
- before_commit: `01becacca4464fc9b4ef7252a6de8211eddb44ea`
- after_commit: unavailable; working-tree accepted change before commit
- revert_commit: not applicable

## Goal

Remove the shared reply-buffer settings mutex from the plain reply path.

## Change

- replaced `ServerMetrics.reply_buffer_settings: Mutex<ReplyBufferSettings>` with three atomics:
  - `reply_buffer_peak_reset_time_millis`
  - `reply_buffer_resizing_enabled`
  - `reply_buffer_copy_avoidance_enabled`
- kept the existing setter surface:
  - `set_reply_buffer_peak_reset_time_millis(...)`
  - `set_reply_buffer_resizing_enabled(...)`
  - `set_reply_copy_avoidance_enabled(...)`
- rebuilt `reply_buffer_settings_snapshot()` from relaxed atomic loads
- left client reply-buffer housekeeping semantics unchanged

## Validation

Targeted Rust test:

- `cargo test -p garnet-server reply_buffer_limits_match_external_scenario -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`

Verified counts:

- `710 passed; 0 failed; 16 ignored`
- plus `26 passed`
- plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.559 \
BASE_BIN=/tmp/garnet-11.558-baseline-30284/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=reply_buffer_mutex_snapshot \
NEW_LABEL=reply_buffer_atomic_snapshot \
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

This was a clear keep.

- `SET ops 103509 -> 123025` (`+18.85%`)
- `GET ops 117876 -> 145589` (`+23.51%`)
- `SET p99 0.079 -> 0.079 ms` (`0.00%`)
- `GET p99 0.071 -> 0.071 ms` (`0.00%`)

The shared settings lock on every reply render was materially more expensive than it looked from the code. Swapping it for atomics moved a real amount of common-path cost without changing reply-buffer behavior.

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
