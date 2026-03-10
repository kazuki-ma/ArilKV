# 11.435 querybuf compat rerender

- Date: 2026-03-10
- Area: isolated `unit/querybuf`
- Before commit: `bf2ac09e8f`
- After commit: working tree

## Goal

Close the isolated Redis `unit/querybuf` residuals by aligning `CLIENT LIST qbuf/qbuf-free` with Redis reusable/private query-buffer semantics.

## Change summary

- Added exact TCP integration regression `query_buffer_resizing_matches_external_scenarios`
- Modeled query-buffer visibility as logical reusable/private state in `ServerMetrics`
- Switched `CLIENT LIST` query-buffer fields away from raw `Vec::capacity()` exposure
- Added RESP declared-bulk-length accounting so fat-argv partial frames surface the expected private qbuf size

## Validation

- Targeted Rust:
  - `cargo test -p garnet-server query_buffer_resizing_matches_external_scenarios -- --nocapture`
- Core gate:
  - `make fmt`
  - `make test-server`
- Targeted external:
  - `cd garnet-rs/tests/interop && RESULT_DIR=... RUNTEXT_EXTRA_ARGS='--single unit/querybuf' ./redis_runtest_external_subset.sh`
- Hot-path smoke benchmark:
  - `binary_ab_local.sh`
  - `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Results

- `make test-server`: `528 + 23 + 1` pass
- targeted external `unit/querybuf`: `ok=4 err=0 timeout=0 ignore=0`
  - result dir: `garnet-rs/tests/interop/results/redis-runtest-querybuf-20260310-1`
- smoke benchmark (`artifacts/bench/comparison.txt`):
  - `SET ops +7.92%`
  - `GET ops +4.57%`
  - `SET p99 -38.32%`
  - `GET p99 -5.93%`

## Artifacts

- `artifacts/bench/base/summary.txt`
- `artifacts/bench/new/summary.txt`
- `artifacts/bench/comparison.txt`
- `diff.patch`
