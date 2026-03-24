# 11.581 borrowed GET lookup key

Status: dropped

Goal:
- remove `RedisKey::from(args[1])` from the main-runtime read-only string path
- let `GET`, `STRLEN`, and `GETRANGE`/`SUBSTR` probe `tsavorite` directly from borrowed key bytes

Candidate:
- added a borrowed-key `peek_bytes(...)` helper in `tsavorite`
- rewired `with_visible_main_runtime_string(...)` to use borrowed `&[u8]` keys
- changed the read-only string handlers to stay on borrowed key bytes longer

Validation while the candidate was live:
- `cargo test -p tsavorite session_peek_bytes_reads_value_with_borrowed_key -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server getrange_and_substr_return_expected_slices -- --nocapture`
- `cargo test -p garnet-server strlen_returns_zero_for_missing_and_length_for_string -- --nocapture`

Benchmark evidence:
- run 1: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.581-borrowed-get-lookup-key-drop/comparison-p1.txt)
- run 2: [comparison-p1-r2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.581-borrowed-get-lookup-key-drop/comparison-p1-r2.txt)
- run 3: [comparison-p1-r3.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.581-borrowed-get-lookup-key-drop/comparison-p1-r3.txt)

Decision:
- drop

Reason:
- the direction did not repeat
- two of three runs regressed plain `GET` throughput
- this is a larger storage-API change than the measured signal justifies right now
