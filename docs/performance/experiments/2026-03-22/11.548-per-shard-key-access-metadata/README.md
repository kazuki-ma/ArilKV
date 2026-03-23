# 11.548 per-shard key access metadata

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted change on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable

## Goal

Remove the global `record_key_access(...)` and LFU/LRU metadata mutexes from the plain string hot path by sharding the access side-state with the same key-to-shard function used by the string store.

## Change

- changed `DbCatalogSideState.key_lru_access_millis` from one global `Mutex<DbKeyMapState<u64>>` to `PerShard<Mutex<DbKeyMapState<u64>>>`
- changed `DbCatalogSideState.key_lfu_frequency` from one global `Mutex<DbKeyMapState<u8>>` to `PerShard<Mutex<DbKeyMapState<u8>>>`
- routed `record_key_access`, `clear_key_access`, `key_lru_millis`, `set_key_idle_seconds`, `set_key_frequency`, and `key_frequency` through `string_store_shard_index_for_key(...)`
- updated `SWAPDB` metadata swap to iterate all shard-local access maps

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server db_scoped_object_access_metadata_does_not_leak_between_databases -- --nocapture`
- `cargo test -p garnet-server swapdb_zero_and_nonzero_preserves_object_access_metadata -- --nocapture`

Full gate:

- `make test-server`
- verified counts: `705 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.548 \
BASE_BIN=/tmp/garnet-11.548-baseline.hi8h3W/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=global_key_access \
NEW_LABEL=per_shard_key_access \
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

The shard-local metadata change is a clean win:

- `SET ops 99930 -> 102007` (`+2.08%`)
- `GET ops 107305 -> 112488` (`+4.83%`)
- `SET p99 0.127 -> 0.103 ms` (`-18.90%`)
- `GET p99 0.103 -> 0.095 ms` (`-7.77%`)

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
