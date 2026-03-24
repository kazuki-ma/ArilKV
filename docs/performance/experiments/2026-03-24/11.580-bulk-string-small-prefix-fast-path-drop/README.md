# 11.580 bulk-string small-prefix fast path

Status: dropped

Goal:
- shave more work from `GET` response emission after `11.571`
- special-case one-, two-, and three-digit bulk-string lengths inside `append_bulk_string(...)`

Validation while the candidate was live:
- `cargo test -p garnet-server append_bulk_string_emits_expected_prefixes_for_small_lengths -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Benchmark evidence:
- first run: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.580-bulk-string-small-prefix-fast-path-drop/comparison-p1.txt)
- rerun: [comparison-p1-r2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.580-bulk-string-small-prefix-fast-path-drop/comparison-p1-r2.txt)

Decision:
- drop

Reason:
- the first run already traded away `GET p99`
- the rerun regressed both throughput and latency
- this is too small and too noisy a win candidate to justify a hot-path complexity increase
