# 11.538 Handle-set owner-inline read avoidance

- Date: 2026-03-19
- Purpose: take wallclock and allocation flamegraphs on the owner-inline `SET` regression path from `11.537`, then reduce the concrete `handle_set` cost instead of only confirming it.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 822; focused handle_set hot-path patch on top of the same HEAD)`
- Diff note: before/after are two focused measurements on the same `HEAD`, not an isolated pair of commits. The runtime delta for this slice is limited to `string_commands.rs` and `string_store.rs`.

## Benchmark/profile shape

- Owner-inline single-thread profile, matching the shape used in `11.537`:
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

## Code change

- `SET` now computes `requires_old_string_value` up front and only materializes the previous string payload when semantics actually need it:
  - `GET`
  - `IFEQ`
  - `IFNE`
  - `IFDEQ`
  - `IFDNE`
- Plain overwrite / `NX` / `XX` cases on the main runtime now use per-shard registry existence checks instead:
  - `tracked_string_key_exists_in_shard(...)`
  - `tracked_object_key_exists_in_shard(...)`
- Successful overwrites also skip redundant `track_string_key_in_shard(...)` reinsertion when the string key already exists.

## Result

- Wallclock owner-inline `SET` improved from `104667.38` to `109014.00 ops/sec` (`+4.15%`).
- Latency also improved:
  - `avg 0.03811 -> 0.03662 ms` (`-3.91%`)
  - `p99 0.103 -> 0.095 ms` (`-7.77%`)
  - `p99.9 0.239 -> 0.159 ms` (`-33.47%`)
  - `p99.99 0.879 -> 0.623 ms` (`-29.12%`)
- Heaptrack allocation capture moved in the right direction too:
  - total allocation calls `1362146 -> 1313807` (`-3.55%`)
  - peak live allocation `427.21K -> 249.12K` (`-41.69%`)
  - heaptrack-run `SET` throughput `1174.27 -> 1277.04 ops/sec` (`+8.75%`)
  - heaptrack-run `SET p99 5.791 -> 4.015 ms` (`-30.67%`)

## Flamegraph readout

- Before the change, wallclock `perf` showed two strong `handle_set` clues:
  - `record_key_access` under `handle_set` at `4.05%`
  - `cfree` under `handle_set` at `3.47%`
- After the change, the standalone `record_key_access` leaf dropped out of the top wallclock excerpt, while `cfree` under `handle_set` remained visible at `4.24%`.
- Interpretation: this slice successfully removed one avoidable old-value materialization cost, but allocator/free churn in the `SET` overwrite path is still a plausible next suspect.

## Validation

- Targeted Rust:
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
- Raw source artifacts remain in `/tmp/garnet-set-owner-inline-before-20260319/` and `/tmp/garnet-set-owner-inline-after-20260319/`.
