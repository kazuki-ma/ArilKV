# 11.554 slowlog atomic config cache

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted change on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable

## Goal

Remove the `config_overrides` mutex from the plain command hot path when `record_slowlog_command(...)` only needs the current `slowlog-log-slower-than` and `slowlog-max-len` values to decide that nothing should be logged.

## Change

- added `slowlog_threshold_micros: AtomicI64` and `slowlog_max_len: AtomicUsize` to `RequestProcessor`
- made `set_config_value(...)` update those atomics whenever `slowlog-log-slower-than` or `slowlog-max-len` changes
- switched `slowlog_threshold_micros()` and `slowlog_max_len()` to atomic loads instead of locking `config_overrides`
- reordered `record_slowlog_command(...)` so the cheap `threshold/max-len` fast path returns before any of the heavier slowlog work
- added exact regression `slowlog_runtime_config_cache_tracks_config_set_updates`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server slowlog_runtime_config_cache_tracks_config_set_updates -- --nocapture`
- `cargo test -p garnet-server slowlog_surface_validation_matches_external_scenarios -- --nocapture`

Full gate:

- `make fmt-check`
- `make test-server`
- verified counts: `707 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260323-11.554-r2 \
BASE_BIN=/tmp/garnet-11.554-baseline/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=slowlog_mutex_config \
NEW_LABEL=slowlog_atomic_config \
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

The first owner-inline `RUNS=5` sample was much larger (`first-comparison.txt`), so the accepted result is the identical rerun:

- `SET ops 76392 -> 76235` (`-0.21%`)
- `GET ops 80963 -> 85710` (`+5.86%`)
- `SET p99 0.167 -> 0.159 ms` (`-4.79%`)
- `GET p99 0.151 -> 0.143 ms` (`-5.30%`)

That is good enough to keep:

- the change removes hot-path mutex traffic from every command,
- the rerun stays non-regressive on `SET`,
- and `GET` plus both p99s move in the right direction.

## Artifacts

- `comparison.txt`
- `first-comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
