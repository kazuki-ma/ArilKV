# 11.570 tracking-read stack gate

- status: dropped
- before_commit: `8e90713c70ddf00e55705bc7b58cbf5380635d0a`
- after_commit: `59bee7571f333819161364c126b3e0aa32b1609d`
- revert_commit: `0899056cb0ab4847500870e5c81fd5c493f9fd10`

## Goal

Skip deferred tracking-read TLS stack setup/teardown when the current request already knows that read tracking is disabled, so plain writes and tracking-off reads avoid the extra `Vec` push/pop path.

## Change

- gated `begin_tracking_read_collection()` / `finish_tracking_read_collection()` in transaction invalidation batches on `current_request_tracking_reads_enabled()`
- computed `tracking_reads_enabled` once in `execute_bytes_in_db(...)`
- skipped request-level deferred read stack setup, teardown, and apply when the request-local flag was false

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server client_tracking_optin_only_tracks_reads_after_caching_yes -- --nocapture`
- `cargo test -p garnet-server tracking_gets_notification_of_expired_keys_like_external_scenario -- --nocapture`
- `cargo test -p garnet-server execute_owned_frame_args_via_processor_matches_direct_execution -- --nocapture`

## Benchmark commands

Current binary:

```bash
CARGO_TARGET_DIR=/private/tmp/garnet-11.570-current-target cargo build -p garnet-server --release
```

Owner-inline `PIPELINE=1`:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.570-p1 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.570-current-target/release/garnet-server \
BASE_LABEL=tracking_read_stack_always \
NEW_LABEL=tracking_read_stack_gated \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

Owner-inline `PIPELINE=1` rerun:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.570-p1-r2 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.570-current-target/release/garnet-server \
BASE_LABEL=tracking_read_stack_always \
NEW_LABEL=tracking_read_stack_gated \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=1 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

Owner-inline `PIPELINE=16`:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.570-p16 \
BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.570-current-target/release/garnet-server \
BASE_LABEL=tracking_read_stack_always \
NEW_LABEL=tracking_read_stack_gated \
RUNS=5 \
THREADS=1 \
CONNS=4 \
REQUESTS=50000 \
PRELOAD_REQUESTS=50000 \
PIPELINE=16 \
SIZE_RANGE=1-256 \
OWNER_THREADS=1 \
GARNET_OWNER_THREAD_PINNING=1 \
GARNET_OWNER_THREAD_CPU_SET=0 \
GARNET_OWNER_EXECUTION_INLINE=1 \
./garnet-rs/benches/binary_ab_local.sh
```

## Result

- `PIPELINE=1` run 1: `SET +0.94%`, `GET -2.93%`, `SET p99 -20.17%`, `GET p99 +11.27%`
- `PIPELINE=1` run 2: `SET -0.42%`, `GET -1.69%`, `SET p99 -10.13%`, `GET p99 +11.27%`
- `PIPELINE=16`: `SET -1.68%`, `GET +0.54%`, `SET p99 -18.56%`, `GET p99 -4.02%`

## Interpretation

- The queue-heavy shape did not collapse, but the plain `PIPELINE=1` path regressed twice in the same direction on `GET` throughput and `GET p99`.
- That makes the tradeoff wrong for a common-path cleanup: the code is small, but it still adds request-level branching without a stable plain-read win.
- The candidate was reverted and only the experiment record was kept.

## Artifacts

- `comparison-p1.txt`
- `comparison-p1-r2.txt`
- `comparison-p16.txt`
- `summary.txt`
- `diff.patch`
