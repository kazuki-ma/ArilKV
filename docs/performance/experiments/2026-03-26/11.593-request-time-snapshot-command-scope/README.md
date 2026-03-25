# 11.593 request-time snapshot command scope

- status: accepted
- before_commit: `e455417dfe`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Take the next evidence-based cut against the `__kernel_clock_gettime` hotspot
from the multithread-client `PIPELINE=4` flamegraphs without breaking exact
expiration/replication semantics.

The target was not "freeze one timestamp for the whole connection loop". The
real goal was narrower:

- stop hot-path helpers from independently calling `Instant::now()` /
  `SystemTime::now()` during one command execution
- let scripts/functions reuse the same frozen request time when a request
  already has one
- keep lazy-expire and blocked-command replication semantics exact

## Change

- added `RequestTimeSnapshot { instant, unix_micros }`
- added request-local helpers:
  - `current_request_instant()`
  - `current_request_time_snapshot()`
  - `RequestTimeSnapshotScope`
- changed
  `RequestProcessor::execute_with_client_tracking_context_and_effects_in_db(...)`
  to lazily install a request snapshot only when one is not already present
- threaded explicit `RequestTimeSnapshot` values into owner-thread execution so
  routed/inline command execution shares one captured timestamp
- changed script `frozen_time` initialization to reuse the ambient request
  snapshot when one is already active
- changed client/runtime activity bookkeeping to read the request snapshot
  `Instant` instead of independently calling `Instant::now()` in the common
  request path

## Rejected Variant

The first version scoped frozen time much more broadly, effectively around the
outer connection/blocking execution envelope and therefore across more than one
actual command observation boundary.

That variant was rejected because it broke the exact replication regression:

- `sync_replication_stream_exec_preserves_expire_then_del_for_expired_blocked_list_key`

Concretely, the final replicated lazy-expire `DEL` disappeared under the broad
freeze. The accepted design therefore keeps request time bounded to actual
command execution only, while allowing post-command unblock/lazy-expire work to
observe fresh time again.

## Dragonfly Reference

Before landing the slice, I checked Dragonfly's source to confirm that it also
keeps transaction-level time:

- `src/server/transaction.cc`: `Transaction::InitTxTime()`
- `src/server/engine_shard_set.h`: `GetCurrentTimeMs()` /
  `absl::GetCurrentTimeNanos()`

So the accepted Garnet design is directionally aligned with the same idea, but
the exact safe scope in Garnet turned out to be narrower because of
compatibility-sensitive lazy-expire behavior.

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server request_time_snapshot_scope_ -- --nocapture`
- `cargo test -p garnet-server scripting_time_command_uses_cached_time_like_external_scenario -- --nocapture`
- `cargo test -p garnet-server xreadgroup_claim_with_two_blocked_clients_external_scenario_runs_as_tcp_integration_test -- --nocapture`
- `cargo test -p garnet-server sync_replication_stream_exec_preserves_expire_then_del_for_expired_blocked_list_key -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `716 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Result

The narrow owner-inline single-thread shape stayed effectively flat:

- `SET -0.51%`
- `GET -0.05%`
- `SET p99 flat`
- `GET p99 flat`

The multithread-client `PIPELINE=4` shape improved cleanly:

- `SET +3.27%`
- `GET +5.05%`
- `SET p99 -4.32%`
- `GET p99 -4.08%`

Raw medians:

- owner-inline `PIPELINE=1` baseline: `SET 96276`, `GET 104335`, `p99 0.087/0.079 ms`
- owner-inline `PIPELINE=1` candidate: `SET 95788`, `GET 104287`, `p99 0.087/0.079 ms`
- multithread-client `PIPELINE=4` baseline: `SET 246507`, `GET 331905`, `p99 4.447/3.135 ms`
- multithread-client `PIPELINE=4` candidate: `SET 254580`, `GET 348654`, `p99 4.255/3.007 ms`

## Interpretation

- This was not a universal throughput win, and it was never going to be one.
  The owner-inline single-thread shape was already close to the floor for this
  cost.
- It is still worth keeping because it removes redundant per-command time reads
  from the real command-execution path, makes scripting share the same request
  time source, and helps exactly in the multi-request pressure shape where the
  clock hotspot was most visible.
- The key technical result is the scope boundary: request time can be shared
  inside real command execution, but broad request-envelope freezing is not
  exact enough for lazy-expire replication parity.

## Artifacts

- `summary.txt`
- `comparison-owner-inline-p1.txt`
- `comparison-owner-inline-p4mt.txt`
- `diff.patch`
