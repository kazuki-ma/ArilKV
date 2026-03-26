# 11.602 `try_read`-first request-boundary fast path

Status: dropped

Goal:
- reduce read-side request-boundary wakeup cost without changing higher-level routing
- prefer a non-blocking `try_read(...)` before falling back to the awaited `read(...)`

Candidate:
- changed `read_and_drain_available(...)` in `connection_handler.rs` to:
  - call `TcpStream::try_read(...)`
  - fall back to `stream.read(...).await` only on `WouldBlock`

Validation while the candidate was live:
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server tcp_pipeline_executes_basic_crud_commands -- --nocapture`

Benchmark evidence:
- owner-inline `PIPELINE=1`: [comparison-p1.txt](comparison-p1.txt)
- owner-inline `PIPELINE=1` rerun: [comparison-p1-r2.txt](comparison-p1-r2.txt)
- multithread-client `THREADS=8 CONNS=16 PIPELINE=4`: [comparison-p4.txt](comparison-p4.txt)

Decision:
- drop

Reason:
- the pressure shape improved, but the narrow owner-inline shape regressed twice in the same direction on `GET`
- a stable `GET -3%` / `GET p99 +15.53%` cost is too high for a common-path read-loop change
