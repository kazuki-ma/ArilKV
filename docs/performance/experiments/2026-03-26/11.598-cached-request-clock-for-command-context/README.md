## 11.598 Cached Request Clock For Command Context

Before commit: `cb980b93c9`  
After commit: `this commit`

### Goal

Remove per-request hot-path `Instant::now()` / `SystemTime::now()` calls from command snapshot capture by routing `RequestTimeSnapshot::capture()` through the existing cached clock thread.

### Change

- promoted the existing 1ms `garnet-cached-clock` into a shared `CachedRequestClock` in `lib.rs`
- `RequestTimeSnapshot::capture()` now returns cached `{ instant, unix_micros }`
- `current_request_activity_instant()` now uses the same cached source when no request scope is present
- `current_instant()` falls back to the cached request snapshot instead of a fresh `Instant::now()`
- added `command_context_reuses_request_time_snapshot_when_present` to pin the request/command snapshot contract

### Validation

- `cargo test -p garnet-server command_context_reuses_request_time_snapshot_when_present -- --nocapture` PASS
- `make fmt` PASS
- `make fmt-check` PASS
- `make test-server` PASS with verified counts:
  - `717 passed; 0 failed; 16 ignored`
  - `26 passed`
  - `1 passed`

### Benchmark

Owner-inline single-thread, `PIPELINE=1`, run 1:

- `SET +0.52%`
- `GET -2.84%`
- `SET p99 -7.77%`
- `GET p99 +16.84%`

Owner-inline single-thread, `PIPELINE=1`, rerun:

- `SET +6.93%`
- `GET -3.57%`
- `SET p99 -7.77%`
- `GET p99 -7.77%`

Owner-inline multithread client, `THREADS=8 CONNS=16 PIPELINE=4`:

- `SET +5.68%`
- `GET +2.98%`
- `SET p99 -9.40%`
- `GET p99 -3.92%`

### Decision

Keep.

Rationale:

- the multithread pressure shape improved cleanly
- the narrow owner-inline shape stayed mixed, but did not show a stable `SET` regression cliff
- the slice removes two hot request-entry clock syscalls from the command snapshot path while preserving command-scoped frozen time

### Artifacts

- `comparison-owner-inline-p1.txt`
- `comparison-owner-inline-p1-r2.txt`
- `comparison-owner-inline-p4mt.txt`
- `summary.txt`
- `diff.patch`
