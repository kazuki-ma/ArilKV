# 11.551 borrowed-key read path

- status: rejected
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only experiment on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable; the experiment was reverted in the working tree before acceptance

## Goal

Test whether the main-runtime string read path could go one step beyond `11.550` by avoiding `RedisKey::from(args[1])` and probing `tsavorite` directly from borrowed key bytes.

## Scope

- added a borrowed-key `peek` entrypoint in `tsavorite`
- rewired the main-runtime `GET`, `STRLEN`, and `GETRANGE`/`SUBSTR` handlers to probe from `&[u8]`
- kept the experiment local to the working tree because it needed benchmark proof before acceptance

## Validation

Targeted Rust tests passed during the experiment and again after the revert:

- `cargo test -p garnet-server strlen_returns_zero_for_missing_and_length_for_string -- --nocapture`
- `cargo test -p garnet-server getrange_and_substr_return_expected_slices -- --nocapture`
- `cargo test -p garnet-server extended_set_conditions_match_external_string_scenarios -- --nocapture`

After reverting the slice, the main gate was green:

- `make fmt-check`
- `make test-server`
  - `706 passed; 0 failed; 16 ignored`
  - `26 passed; 0 failed`
  - `1 passed; 0 failed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.551-idle \
BASE_BIN=<baseline binary> \
NEW_BIN=<experiment binary> \
BASE_LABEL=borrowed_key_baseline \
NEW_LABEL=borrowed_key_read_path \
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

The experiment regressed on both the first run and the idle-host rerun, so it was dropped.

First run:

- `SET ops 61321 -> 41705` (`-31.99%`)
- `GET ops 61659 -> 52060` (`-15.57%`)
- `SET p99 0.231 -> 0.319 ms` (`+38.10%`)
- `GET p99 0.207 -> 0.255 ms` (`+23.19%`)

Idle-host rerun:

- `SET ops 102224 -> 83317` (`-18.50%`)
- `GET ops 110274 -> 91518` (`-17.01%`)
- `SET p99 0.111 -> 0.135 ms` (`+21.62%`)
- `GET p99 0.103 -> 0.119 ms` (`+15.53%`)

## Artifacts

- `comparison.txt` is the final idle-host rerun
- `first-comparison.txt` is the initial noisy-host run
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `summary.txt`
- `diff.patch` records why no retained standalone patch was kept
