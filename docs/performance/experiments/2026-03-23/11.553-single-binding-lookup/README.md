# 11.553 single binding lookup

- status: rejected
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only experiment on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable; the experiment was reverted in the working tree before acceptance

## Goal

Test whether the string hot path could benefit from resolving `logical_db_uses_main_runtime(selected_db)` only once per command instead of rechecking it inside both the handler and expiration helper.

## Scope

- added a dedicated main-runtime expiration helper
- cached the selected-db binding inside `GET`, `STRLEN`, `GETRANGE`/`SUBSTR`, and `SET`
- kept the experiment local to the working tree because it needed benchmark proof before acceptance

## Validation

Targeted Rust tests passed during the experiment:

- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
- `cargo test -p garnet-server getrange_and_substr_return_expected_slices -- --nocapture`
- `cargo test -p garnet-server strlen_returns_zero_for_missing_and_length_for_string -- --nocapture`

After reverting the slice, the tree was checked again with:

- `make fmt-check`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260323-11.553-r2 \
BASE_BIN=/tmp/garnet-11.553-baseline.2eYO45/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=baseline \
NEW_LABEL=single_binding_lookup \
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

The experiment was dropped after two A/B runs.

First run:

- `SET ops 100421 -> 95208` (`-5.19%`)
- `GET ops 114797 -> 108923` (`-5.12%`)
- `SET p99 0.103 -> 0.127 ms` (`+23.30%`)
- `GET p99 0.095 -> 0.103 ms` (`+8.42%`)

Second run:

- `SET ops 104448 -> 101487` (`-2.83%`)
- `GET ops 109159 -> 110223` (`+0.97%`)
- `SET p99 0.111 -> 0.127 ms` (`+14.41%`)
- `GET p99 0.119 -> 0.111 ms` (`-6.72%`)

## Artifacts

- `comparison.txt` is the second run
- `first-comparison.txt` is the first run
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `summary.txt`
- `diff.patch`
