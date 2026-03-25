## 11.596 Command Context Hot Path First Slice

Before commit: `92f974fd1b`  
After commit: this commit

### Goal

Introduce an explicit `CommandContext` at the dispatch boundary so hot command paths stop depending only on ambient request-time lookups.

First slice scope:

- add `CommandContext { time_snapshot }` in `request_lifecycle`
- pass `ctx` into hot string handlers (`GET`, `SET`, `SETEX`, `SETNX`, `PSETEX`, `GETSET`, `GETDEL`)
- route `record_key_access(...)` through `ctx.instant`
- pass the same `ctx.time_snapshot` into scripting entry so script frozen time is sourced from the command context instead of re-reading ambient request time

This is primarily a design cleanup slice with a small hot-path cut on access metadata bookkeeping.

### Validation

- `make fmt` PASS
- `make fmt-check` PASS
- Targeted Rust PASS:
  - `cargo test -p garnet-server request_time_snapshot_scope_reuses_fixed_time_within_request_helpers -- --nocapture`
  - `cargo test -p garnet-server scripting_time_command_uses_cached_time_like_external_scenario -- --nocapture`
  - `cargo test -p garnet-server sync_replication_stream_exec_preserves_expire_then_del_for_expired_blocked_list_key -- --nocapture`
  - `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `make test-server` PASS with verified counts:
  - `716 passed; 0 failed; 16 ignored`
  - `26 passed`
  - `1 passed`

### Benchmark

Owner-inline single-thread, `PIPELINE=1`:

- `SET +0.84%`
- `GET -1.75%`
- `SET p99 flat`
- `GET p99 +10.13%`

Owner-inline multithread client, `THREADS=8 CONNS=16 PIPELINE=4`:

- `SET +1.05%`
- `GET +3.58%`
- `SET p99 +0.76%`
- `GET p99 -3.08%`

### Decision

Keep.

Rationale:

- the pressure shape that motivated the recent clock/boundary work improved
- the narrow `PIPELINE=1` shape stayed close to flat and did not show a throughput cliff
- the code path is cleaner: command start time is now explicit at the dispatch boundary and scripting frozen time uses the same command-scoped snapshot

### Artifacts

- `comparison-owner-inline-p1.txt`
- `comparison-owner-inline-p4mt.txt`
- `summary.txt`
- `diff.patch`
