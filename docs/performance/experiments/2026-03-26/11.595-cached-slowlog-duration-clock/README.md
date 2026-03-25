# 11.595 cached slowlog duration clock

- status: accepted
- before_commit: `9687b2a7f8`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Take the next evidence-based cut after `11.593`.

The fresh Linux GET perf refresh still showed the clock path as the main
remaining hot spot in the pressure shape that matters here:

- `THREADS=8`
- `CONNS=16`
- `PIPELINE=4`
- owner-inline single-lane server

The actual culprit was narrower than the earlier request-time work:

- `std::time::Instant::elapsed()` inside
  `RequestProcessor::execute_bytes_in_db(...)`
- specifically for the non-blocking slowlog timing path

## Change

- added a process-global `CachedMonotonicClock`
- refreshes cached monotonic micros from a lightweight background thread
- added `cached_monotonic_micros()`
- added `record_slowlog_command_duration_micros(...)`
- changed non-blocking `execute_bytes_in_db(...)` slowlog timing to use cached
  start/end micros instead of `Instant::now()` + `elapsed()`
- kept the blocking slowlog path on the existing exact `Instant::elapsed()`
  code, because that path is not hot and is more compatibility-sensitive

## Why This Scope

This slice intentionally does not try to replace all time reads with a coarse
clock.

- non-blocking slowlog duration was the measured hot path
- blocking/unblock slowlog reporting was not hot
- exact slowlog behavior for blocking commands was already validated and was not
  worth perturbing in the same slice

So the accepted change is the smallest change that removes the measured
`clock_gettime` cliff without broadening risk.

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server slowlog_threshold_and_entry_shape_match_external_scenarios -- --nocapture`
- `cargo test -p garnet-server slowlog_rewritten_and_blocking_commands_match_external_scenarios -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `716 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark Result

Owner-inline single-thread remained a small throughput win, but `GET p99` moved
the wrong way:

- `SET +1.65%`
- `GET +1.63%`
- `SET p99 flat`
- `GET p99 +10.13%`

The multithread-client pressure shape improved cleanly and is the acceptance
bar for this slice:

- `SET +6.43%`
- `GET +2.24%`
- `SET p99 -8.70%`
- `GET p99 -2.06%`

Raw medians:

- owner-inline `PIPELINE=1` baseline: `SET 96090`, `GET 100183`, `p99 0.087/0.079 ms`
- owner-inline `PIPELINE=1` candidate: `SET 97672`, `GET 101814`, `p99 0.087/0.087 ms`
- multithread-client `PIPELINE=4` baseline: `SET 244760`, `GET 337316`, `p99 4.415/3.103 ms`
- multithread-client `PIPELINE=4` candidate: `SET 260505`, `GET 344879`, `p99 4.031/3.039 ms`

## Linux Perf Shift

The confirming Linux perf refresh is the strongest evidence that the slice hit
the intended target.

Before:

- GET top symbol: `__kernel_clock_gettime 20.50%`
- inner hot edge: `Instant::elapsed 16.03%`

After:

- GET top symbol: `cfree 16.89%`
- next symbols: `__wake_up_sync_key 15.82%`, `HashIndex::find_tag_address 13.00%`
- `Instant::elapsed` is no longer near the top of the GET profile

That is the expected pattern if the clock cliff was genuinely removed and the
next-order allocator/storage costs are now exposed.

## Interpretation

- The pressure-shape win plus the perf-hotspot shift makes this a clean keep.
- The slice did not magically fix the whole GET path; it simply exposed the next
  real bottleneck.
- The next likely target is allocator churn on the GET path, not clocking.

## Artifacts

- `summary.txt`
- `comparison-owner-inline-p1.txt`
- `comparison-owner-inline-p4mt.txt`
- `linux-hotspot-shift.txt`
- `diff.patch`
