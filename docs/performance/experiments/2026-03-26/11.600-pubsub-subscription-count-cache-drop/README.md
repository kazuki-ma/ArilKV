# 11.600 connection-local pubsub subscription count cache

Status: dropped

Goal:
- remove the hot request-loop and RESP2 subscribed-mode `pubsub_subscription_count(...)` lookup
- carry the current subscription count as a connection-local effect instead

Candidate:
- extended `RequestConnectionEffects` with an optional pubsub subscription count
- subscribe / unsubscribe command handlers updated that effect after successful state changes
- `ClientConnectionState` cached the current count and the connection loop used it for:
  - deciding whether the connection is in pubsub mode
  - RESP2 subscribed-context command gating

Validation while the candidate was live:
- `cargo test -p garnet-server resp2_subscribed_context_clears_after_last_unsubscribe_on_same_connection -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server tcp_pipeline_executes_basic_crud_commands -- --nocapture`

Benchmark evidence:
- owner-inline `PIPELINE=1`: [comparison-p1.txt](comparison-p1.txt)
- owner-inline `PIPELINE=1` rerun: [comparison-p1-r2.txt](comparison-p1-r2.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: [comparison-p4.txt](comparison-p4.txt)

Decision:
- drop

Reason:
- the multithread pressure shape improved, but the narrow common path regressed too hard and did not stabilize on rerun
- the remaining pubsub mutex lookup is cheaper than the extra connection-local bookkeeping this slice introduced on the plain request path
