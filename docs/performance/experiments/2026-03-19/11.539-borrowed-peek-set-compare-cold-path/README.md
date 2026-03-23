# 11.539 Borrowed peek for compare-only `SET` conditions

- Date: 2026-03-19
- Purpose: add a Rust-safe borrowed peek API for compare-only reads, use it in `SET` condition checks, and keep plain owner-inline `SET` fast after validating the design with wallclock and allocation flamegraphs.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 823; borrowed peek API plus cold compare-only helper on top of the same HEAD)`
- Diff note: before/after are two focused measurements on the same `HEAD`. The runtime delta for this slice is limited to `string_commands.rs` and the new borrowed peek plumbing in `tsavorite`.

## Benchmark/profile shape

- Owner-inline single-thread profile, matching the shape used in `11.537` and `11.538`:
  - `THREADS=1`
  - `CONNS=4`
  - `REQUESTS=30000`
  - `PRELOAD_REQUESTS=30000`
  - `PIPELINE=1`
  - `SIZE_RANGE=1-256`
  - `SERVER_CPU_SET=0`
  - `CLIENT_CPU_SET=1`
  - `GARNET_OWNER_THREAD_PINNING=1`
  - `GARNET_OWNER_THREAD_CPU_SET=0`
  - `GARNET_OWNER_EXECUTION_INLINE=1`
- Wallclock flamegraphs came from Linux `perf` on this profile.
- Allocation flamegraphs came from `heaptrack` on a reduced `SET`-only run (`REQUESTS=10000` per client) to keep the trace manageable under heap profiling overhead.
- The baseline for this slice is the corrected `11.538` state, not the earlier reversed local snapshot.

## Design

- Added `TsavoriteSession::peek(...)`, a callback-bounded borrowed read API that pins the epoch only for the duration of the callback.
- The callback receives a borrowed record view and can return a small derived result instead of forcing record materialization.
- `SET GET` intentionally stays on the owned read path because the previous value must outlive the storage callback and be returned on the wire.
- Compare-only `SET` conditions (`IFEQ`, `IFNE`, `IFDEQ`, `IFDNE`) now use the borrowed peek path instead.
- The first direct inline integration made plain `SET` slower. The accepted design therefore keeps the borrowed peek path but isolates it in a `#[cold] #[inline(never)]` helper so the common overwrite path stays compact.
- I did not widen epoch pinning to the whole command. The accepted design keeps the epoch scoped to the peek callback, which is enough for zero-copy compare-only checks and keeps reclamation/lifetime rules narrow and explicit.

## Code change

- `tsavorite` now exposes callback-bounded borrowed record access:
  - `PeekOperationStatus<T>`
  - `RecordView<'a>`
  - `peek(...)`
  - `TsavoriteSession::peek(...)`
- The ordinary owned read path was refactored to reuse the same internal record-view traversal instead of carrying a separate materialization-only path.
- `handle_set` now routes compare-only conditions through `string_write_condition_matches_with_peek(...)`, a cold helper that decodes expiration visibility and compares against the borrowed user-value slice without cloning it.

## Result

- Wallclock owner-inline `SET` improved from `113349.02` to `119340.64 ops/sec` (`+5.29%`).
- Latency moved as follows:
  - `avg 0.03524 -> 0.03352 ms` (`-4.88%`)
  - `p99 0.095 -> 0.087 ms` (`-8.42%`)
  - `p99.9 0.175 -> 0.199 ms` (`+13.71%`)
  - `p99.99 0.551 -> 0.423 ms` (`-23.23%`)
- Heaptrack-run `SET` throughput improved from `1597.03` to `1642.07 ops/sec` (`+2.82%`).
- Heaptrack latency moved as follows:
  - `avg 2.50401 -> 2.43556 ms` (`-2.73%`)
  - `p99 4.191 -> 3.055 ms` (`-27.10%`)
  - `p99.9 7.679 -> 7.167 ms` (`-6.67%`)
  - `p99.99 15.103 -> 17.663 ms` (`+16.95%`)
- Allocation totals stayed effectively flat:
  - calls to allocation functions `2214936 -> 2214888`
  - temporary allocations `473204 -> 473180`
  - peak heap memory consumption `24.81M -> 24.81M`

## Flamegraph readout

- The corrected `11.538` baseline already removed the worst old-value materialization cost, so this slice is more about API cleanliness and targeted compare-only zero-copy than dramatic allocation reduction.
- Wallclock `perf` still shows allocator/free churn under `handle_set`:
  - baseline `cfree 6.25%`
  - final `cfree 8.00%`
- `record_key_access` is no longer a top regression clue in the final capture; it shows up around `1.33%` in the final top excerpt.
- Interpretation: the borrowed peek design is worthwhile and measurably safe when kept off the hot overwrite path, but it does not solve the remaining `SET` allocator/free churn by itself.

## Validation

- Targeted Rust:
  - `cargo test -p tsavorite peek_ -- --nocapture`
  - `cargo test -p garnet-server extended_set_conditions_match_external_string_scenarios -- --nocapture`
  - `cargo test -p garnet-server set_get_option_returns_old_value -- --nocapture`
- Formatting:
  - `make fmt`
  - `make fmt-check`
- Full server gate:
  - `make test-server`
  - Verified counts: `703 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Artifacts

- Summary: [summary.txt](./summary.txt)
- Comparison: [comparison.txt](./comparison.txt)
- Focused patch: [diff.patch](./diff.patch)
- Wallclock flamegraphs:
  - [wallclock-before.svg](./wallclock-before.svg)
  - [wallclock-after.svg](./wallclock-after.svg)
- Allocation flamegraphs:
  - [allocation-before.svg](./allocation-before.svg)
  - [allocation-after.svg](./allocation-after.svg)
- Raw source artifacts remain in:
  - `/tmp/garnet-set-owner-inline-peek-before-corrected-20260319-wallclock/`
  - `/tmp/garnet-set-owner-inline-peek-final-20260319-wallclock/`
  - `/tmp/garnet-set-owner-inline-peek-before-corrected-20260319-heaptrack/`
  - `/tmp/garnet-set-owner-inline-peek-final-20260319-heaptrack/`
