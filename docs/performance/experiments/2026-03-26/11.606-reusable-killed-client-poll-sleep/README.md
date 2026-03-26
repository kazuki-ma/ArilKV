# 11.606 reusable killed-client poll sleep

## Status

Accepted.

## Goal

Continue the `__wake_up_sync_key` / request-boundary investigation by removing per-iteration `timeout(...)` future construction from the normal request loop, while preserving the existing killed-client polling semantics.

## Change

- Reused one pinned `tokio::time::Sleep` per connection instead of creating a fresh `tokio::time::timeout(...)` wrapper on every normal request-loop iteration.
- Added a shared helper so both normal and pubsub loops reset the same reusable sleep to `now + KILLED_CLIENT_POLL_INTERVAL`.
- Kept the existing killed-client polling behavior; this is strictly a request-boundary allocation/setup reduction.

## Evidence

Docker/Linux perf was unavailable for this slice because the local Docker daemon was not reachable from the current environment. To keep the investigation moving, local macOS sample-based hotspot captures were taken first:

- pressure shape (`THREADS=8 CONNS=16 PIPELINE=4`) still showed heavy request-loop overhead with leaf hotspots dominated by `__psynch_mutexwait`, `__sendto`, `__recvfrom`, and `ServerMetrics::{add_client_input_bytes_and_last_command,add_client_output_bytes}`
- narrow shape (`PIPELINE=1`) still showed request-boundary inclusive cost, including `tokio::time::timeout`/`Timeout<T>::poll`, `PollEvented::poll_read`, and `TcpStream::read`

That evidence justified a narrow request-boundary experiment rather than another broader wakeup rewrite.

## Benchmark

Before commit: `b911d1c07874bb05b3756ec29934e5db4ec1ec6f`

After commit: pending at authoring time; see `diff.patch`

Harness: owner-inline local A/B with identical parameters per shape.

### Owner-inline `PIPELINE=1`

- first run: `SET -2.67%`, `GET -1.41%`, p99 essentially flat
- rerun: `SET +0.52%`, `GET +0.26%`, p99 flat

### Multithread-client `THREADS=8 CONNS=16 PIPELINE=4`

- `SET +6.96%`
- `GET +9.49%`
- `SET p99 -8.85%`
- `GET p99 -8.08%`

## Acceptance rationale

Kept because the pressure shape we are actively chasing improved clearly, while the narrow owner-inline shape recovered to flat/slightly positive on rerun instead of reproducing a stable regression cliff.

## Validation

- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server tcp_pipeline_executes_basic_crud_commands -- --nocapture`
- `cargo test -p garnet-server tracking_gets_notification_of_expired_keys_like_external_scenario -- --nocapture`
- `make fmt`
- `make fmt-check`
- `make test-server`

Verified final gate counts:

- `719 passed; 0 failed; 16 ignored`
- `26 passed`
- `1 passed`

## Artifacts

- `comparison-p1.txt`
- `comparison-p1-r2.txt`
- `comparison-p4.txt`
- `p1-get.leaf.top20.txt`
- `p1-get.incl.top20.txt`
- `p1-set.leaf.top20.txt`
- `p1-set.incl.top20.txt`
- `p4-get.leaf.top20.txt`
- `p4-get.incl.top20.txt`
- `p4-set.leaf.top20.txt`
- `p4-set.incl.top20.txt`
