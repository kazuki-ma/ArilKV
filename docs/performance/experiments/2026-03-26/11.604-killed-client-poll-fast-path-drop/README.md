# 11.604 conditional killed-client poll fast path

Status: dropped

Goal:
- remove the `25ms` killed-client poll timeout from the common request path when there are no killed clients
- keep idle tracking invalidation and pending-message delivery correct

Candidate:
- added a killed-client count in `ServerMetrics`
- used that count to disable the sleep branch in the connection loop when no client was currently marked killed
- after the first exact failure, also fixed `enqueue_pending_client_message(...)` to notify the waiting connection

Validation while the candidate was live:
- `cargo test -p garnet-server tracking_gets_notification_of_expired_keys_like_external_scenario -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Benchmark evidence:
- owner-inline `PIPELINE=1`: [comparison-p1.txt](comparison-p1.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: [comparison-p4.txt](comparison-p4.txt)

Decision:
- drop

Reason:
- the first exact regression exposed a real hidden dependency on the timeout path, and although the notifier fix closed the correctness gap, the benchmark still regressed
- the remaining sleep-branch overhead is cheaper than the extra killed-client bookkeeping and changed select topology in the common path
