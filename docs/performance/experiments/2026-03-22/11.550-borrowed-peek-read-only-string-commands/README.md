# 11.550 borrowed peek for read-only string commands

- status: accepted
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only accepted change on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable

## Goal

Retry the earlier borrowed read idea in a much narrower shape: keep the generic `read_string_value(...)` path alone, and only optimize main-runtime `GET`, `STRLEN`, and `GETRANGE`/`SUBSTR` so they can inspect the stored value through `TsavoriteSession::peek(...)` without first materializing an owned `Vec<u8>`.

## Change

- added `visible_string_user_value(...)` in `string_commands.rs` so borrowed reads use the same `decode_stored_value(...)` and expiration visibility rules as the existing reader path
- added `with_visible_main_runtime_string(...)` as a callback-bounded helper around `TsavoriteSession::peek(...)`
- changed only the main-runtime branches of:
  - `GET`
  - `STRLEN`
  - `GETRANGE` / `SUBSTR`
- kept auxiliary-db reads, wrongtype checks, read tracking, and generic string helpers unchanged
- reused the same borrowed visibility helper for compare-only `SET` conditions so the expiration visibility logic is not duplicated

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server strlen_returns_zero_for_missing_and_length_for_string -- --nocapture`
- `cargo test -p garnet-server getrange_and_substr_return_expected_slices -- --nocapture`
- `cargo test -p garnet-server debug_sync_points_can_force_get_set_ordering_between_threads -- --nocapture`

Full gate:

- `make fmt-check`
- `make test-server`
- verified counts: `706 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.550 \
BASE_BIN=/tmp/garnet-11.550-baseline.4ZuLK1/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=borrowed_peek_read_baseline \
NEW_LABEL=borrowed_peek_read_path \
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

The narrower borrowed-peek shape was a clean keep:

- `SET ops 97469 -> 107032` (`+9.81%`)
- `GET ops 104751 -> 111228` (`+6.18%`)
- `SET p99 0.119 -> 0.103 ms` (`-13.45%`)
- `GET p99 0.111 -> 0.095 ms` (`-14.41%`)

The earlier direct read-to-RESP experiment regressed because it widened the change too far. This version keeps the optimization local to three read-only commands and avoids perturbing the broader read/write helpers, which was enough to make the borrowed path a net win.

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch`
