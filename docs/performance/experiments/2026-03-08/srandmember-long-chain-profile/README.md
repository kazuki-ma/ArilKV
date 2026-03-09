# SRANDMEMBER Long-Chain Profile

## Objective

Profile the Redis `tests/unit/type/set.tcl` case `SRANDMEMBER with a dict containing long chain`
before/after the owner-thread set hot-state path, using the exact command sequence as the primary
Rust regression test and a standalone external driver for server-only profiling.

## Scenario

- Primary regression test:
  - [tests.rs](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/crates/garnet-server/src/tests.rs)
  - `tests::srandmember_long_chain_external_scenario_runs_as_tcp_integration_test`
- External driver:
  - [driver.py](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/driver.py)
- Workload shape kept from Redis test:
  - `100000` single-member `SADD`
  - `SSCAN` collection + deferred/pipelined `SREM`
  - `BGSAVE`
  - `SRANDMEMBER -10` coverage loop
  - trim-to-30 + positive-count histogram check

## Environment

- Host: macOS
- Profiler: `/usr/bin/sample`
- Flamegraph tools: `/tmp/FlameGraph`
- Server mode:
  - `GARNET_TSAVORITE_STRING_STORE_SHARDS=1`
  - `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=1048576`
  - `GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS=25`

## Findings

### Before

- Artifact:
  - [server.sample.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server.sample.txt)
  - [server.folded](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server.folded)
  - [server.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server.flame.svg)
- Sample captured on the pre-rebuild release binary during `create_set`.
- Dominant stack:
  - `handle_sadd -> load_set_object_payload -> decode_set_object_payload -> BTreeMap::insert`
  - `handle_sadd -> save_set_object -> serialize_set_object_payload`
- Qualitative result:
  - standalone external driver stayed in `phase=create_set` for tens of seconds and had not reached
    `phase=warmup` after roughly 40s.

### After

- Artifacts:
  - [driver-after-profile.log](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/driver-after-profile.log)
  - [server-after.sample.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server-after.sample.txt)
  - [server-after.folded](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server-after.folded)
  - [server-after.flame.svg](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/artifacts/server-after.flame.svg)
- Driver summary:
  - `create_seconds=3.810`
  - `total_seconds=14.012`
  - `chi_square=15.926`
- Dominant stacks shifted to socket I/O and owner-thread handoff:
  - `__sendto`
  - `__recvfrom`
  - `semaphore_signal_trap`
- The old decode/serialize loop is no longer the main hotspot.

## Interpretation

The hotspot was not random sampling itself. It was repeated full set materialization on every
single-member `SADD`/`SREM` in the Redis external workload. Because the same key is always handled
on the same owner thread, keeping a small owner-thread-local mutable set working state is an
effective fix for this scenario.

## Validation

- `make fmt`
- `make test-server`
- `cargo test -p garnet-server srandmember_long_chain_external_scenario_runs_as_tcp_integration_test -- --nocapture`
- `python3 -m py_compile docs/performance/experiments/2026-03-08/srandmember-long-chain-profile/driver.py`

## External Compatibility Note

The full Redis external probe still reports
`SRANDMEMBER with a dict containing long chain in tests/unit/type/set.tcl`, but the current failure
is now due to missing `DEBUG HTSTATS-KEY`, not the `SRANDMEMBER` path itself.
