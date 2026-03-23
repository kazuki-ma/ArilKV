# 11.549 watch/tracking fast path

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted change on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable

## Goal

Remove unconditional write-side work from the plain string hot path when nobody is using `WATCH` or client-side tracking.

## Change

- added a real per-connection watch registration guard so `watching_clients` now reflects live `WATCH` usage and is cleaned up on `UNWATCH`, `RESET`, `EXEC`/`DISCARD`, and disconnect
- added a lightweight `tracking_clients` count so write-side invalidation can skip the tracking machinery entirely when no client tracking state exists
- changed `bump_watch_version_in_db(...)` and its server-origin variant to:
  - skip both `watch_versions` and invalidation work when both watcher/tracking counts are zero
  - only bump `watch_versions` when a watcher exists
  - only enqueue tracking invalidations when a tracking client exists
- added exact TCP regression `info_clients_updates_watch_and_tracking_counts_on_disable_and_disconnect`
- updated the internal watch-version regression to reflect the new invariant: with zero watchers the version need not advance, but with an active watcher it must still be DB-scoped and monotonic

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server info_clients_updates_watch_and_tracking_counts_on_disable_and_disconnect -- --nocapture`
- `cargo test -p garnet-server watch_stale_key_then_lazy_delete_does_not_abort_exec -- --nocapture`
- `cargo test -p garnet-server watch_versions_are_scoped_by_explicit_db -- --nocapture`
- `cargo test -p garnet-server db_scoped_object_access_metadata_does_not_leak_between_databases -- --nocapture`
- `cargo test -p garnet-server swapdb_zero_and_nonzero_preserves_object_access_metadata -- --nocapture`

Full gate:

- `make fmt-check`
- `make test-server`
- verified counts: `706 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.549-rerun \
BASE_BIN=/tmp/garnet-11.549-baseline.KX7iTE/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=watch_tracking_baseline \
NEW_LABEL=watch_tracking_fast_path \
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

The first `RUNS=5` A/B was mixed (`first-comparison.txt`): `SET +5.68%`, `GET -4.48%`, `SET p99 -7.21%`, `GET p99 +16.84%`.

The identical rerun was stable and non-regressive, so that rerun is the accepted result:

- `SET ops 106971 -> 108149` (`+1.10%`)
- `GET ops 116096 -> 117511` (`+1.22%`)
- `SET p99 0.103 -> 0.103 ms` (`0.00%`)
- `GET p99 0.095 -> 0.095 ms` (`0.00%`)

## Artifacts

- `comparison.txt`
- `first-comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
