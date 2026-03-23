# 11.558 tracking zero-client fast path

- status: dropped
- before_commit: `01becacca4464fc9b4ef7252a6de8211eddb44ea`
- after_commit: unavailable; reverted working-tree experiment
- revert_commit: not applicable

## Goal

Skip the request-level tracking invalidation/read TLS stack work when no tracking clients are registered.

## Change

- gated `begin_tracking_invalidation_collection()` / `begin_tracking_read_collection()` behind `has_tracking_clients()`
- gated the `tracking_reads_enabled` request-context toggle behind the same snapshot
- skipped deferred read application, tracking caching override clear, and invalidation emission cleanup when the request started with `tracking_clients == 0`
- applied the same gate to `begin_tracking_invalidation_batch()` / `finish_tracking_invalidation_batch()`

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server flush_commands_emit_global_tracking_invalidation_even_without_tracked_keys -- --nocapture`
- `cargo test -p garnet-server swapdb_emits_global_tracking_invalidation -- --nocapture`
- `cargo test -p garnet-server client_info_kill_caching_reply_and_config_rewrite_stubs -- --nocapture`

All three passed.

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.558-r2 \
BASE_BIN=/tmp/garnet-11.558-baseline-30284/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=tracking_stacks_always_on \
NEW_LABEL=tracking_zero_client_fast_path \
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

This did not pay for itself.

First run:

- `SET ops 122578 -> 119487` (`-2.52%`)
- `GET ops 136525 -> 136838` (`+0.23%`)
- `SET p99 0.071 -> 0.079 ms` (`+11.27%`)
- `GET p99 0.079 -> 0.071 ms` (`-10.13%`)

Identical rerun:

- `SET ops 119724 -> 118031` (`-1.41%`)
- `GET ops 137731 -> 139368` (`+1.19%`)
- `SET p99 0.079 -> 0.079 ms` (`0.00%`)
- `GET p99 0.079 -> 0.071 ms` (`-10.13%`)

The request-level stack/TLS work looked cheap enough that the extra branching/gating did not win on plain owner-inline `GET/SET`. The slice was reverted.

## Artifacts

- `first-comparison.txt`
- `first-before-summary.txt`
- `first-before-runs.csv`
- `first-after-summary.txt`
- `first-after-runs.csv`
- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
