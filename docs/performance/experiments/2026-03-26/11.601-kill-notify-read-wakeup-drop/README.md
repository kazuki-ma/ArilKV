# 11.601 event-driven killed-client wakeups

Status: dropped

Goal:
- remove the normal client read-loop `25ms` killed-client poll timeout
- wake idle connections only when killed-client state actually changes

Candidate:
- added killed-client count + `Notify` in `ServerMetrics`
- normal client read waits listened for socket readiness or client-kill notification instead of timing out every `25ms`

Validation while the candidate was live:
- `cargo test -p garnet-server idle_normal_client_disconnects_promptly_after_client_kill_id -- --nocapture`
- `cargo test -p garnet-server sync_client_appears_in_info_replication_and_client_kill_type_slave_disconnects_it -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Benchmark evidence:
- owner-inline `PIPELINE=1`: [comparison-p1.txt](comparison-p1.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: [comparison-p4.txt](comparison-p4.txt)

Decision:
- drop

Reason:
- the read-side latency/throughput impact stayed negative, especially on `GET`
- the extra notify/count bookkeeping did not pay for itself relative to the old timeout path
