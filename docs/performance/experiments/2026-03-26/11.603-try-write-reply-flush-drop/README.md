# 11.603 `try_write`-first reply publication

Status: dropped

Goal:
- reduce write-side request-boundary wakeups
- avoid the awaited `write_all(...)` path when the socket is immediately writable

Candidate:
- added `write_all_with_try_write(...)`
- routed reply publication, monitor event flush, and pending pubsub/tracking delivery through that helper

Validation while the candidate was live:
- `cargo test -p garnet-server subscribed_mode_resp2_after_hello2_rejects_regular_commands_like_external_redis_cli_scenario -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server tcp_pipeline_executes_basic_crud_commands -- --nocapture`

Benchmark evidence:
- owner-inline `PIPELINE=1`: [comparison-p1.txt](comparison-p1.txt)
- owner-inline `PIPELINE=1` idle rerun: [comparison-p1-r2.txt](comparison-p1-r2.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: [comparison-p4.txt](comparison-p4.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4` idle rerun: [comparison-p4-r2.txt](comparison-p4-r2.txt)

Decision:
- drop

Reason:
- the narrow shape looked good, but the pressure shape stayed negative overall and got worse on the idle rerun
- the extra `try_write` loop is not paying for itself under the multithread-client shape we are actively chasing
