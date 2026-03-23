# 11.552 monotonic access-metadata clock

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted slice on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`

## Goal

Reduce the unconditional timekeeping cost in plain `GET`/`SET` by stopping `record_key_access(...)` and `OBJECT IDLETIME` bookkeeping from converting `SystemTime` to UNIX millis on every touch.

## Scope

- added a processor-local access clock base
- routed `record_key_access(...)`, `key_idle_seconds(...)`, and `set_key_idle_seconds(...)` through a monotonic `current_instant()`-derived helper
- kept TTL/expire semantics on the existing wall-clock path

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server execute_with_client_no_touch_is_scoped_per_request -- --nocapture`
- `cargo test -p garnet-server db_scoped_object_access_metadata_does_not_leak_between_databases -- --nocapture`
- `cargo test -p garnet-server swapdb_zero_and_nonzero_preserves_object_access_metadata -- --nocapture`
- `cargo test -p garnet-server object_idletime_returns_integer_for_existing_key -- --nocapture`

Repository gates:

- `make fmt`
- `make fmt-check`
- `make test-server`
  - `706 passed; 0 failed; 16 ignored`
  - `26 passed; 0 failed`
  - `1 passed; 0 failed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.552 \
BASE_BIN=/tmp/garnet-11.552-baseline.S8MDw6/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=baseline \
NEW_LABEL=monotonic_access_clock \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=30000 \
PRELOAD_REQUESTS=30000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

The monotonic access-metadata clock was a clean keep:

- `SET ops 73062 -> 76165` (`+4.25%`)
- `GET ops 87831 -> 93475` (`+6.43%`)
- `SET p99 0.175 -> 0.175 ms` (`0.00%`)
- `GET p99 0.143 -> 0.127 ms` (`-11.19%`)

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `summary.txt`
- `diff.patch`
