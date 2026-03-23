# 11.547 direct read-to-RESP string read

- status: rejected
- before_commit: `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- after_commit: unavailable; working-tree-only experiment on top of `fa4106fd5c65901a6058febc10609e7ca699fbc2`
- revert_commit: not applicable; the experiment was reverted in the working tree before acceptance

## Goal

Test whether a borrowed `peek`-style read path for string reads could bypass the usual `record -> Vec<u8> -> RESP output buffer` flow and improve plain owner-inline `GET`/`SET` workloads.

## Scope

- rewired string read helpers to support borrowed `peek`-based string access
- tested the change against `GET`, `GETRANGE`, and `STRLEN`-adjacent paths
- kept the experiment local to the working tree because it needed benchmark proof before acceptance

## Validation

Targeted Rust tests passed before the benchmark:

- `cargo test -p garnet-server debug_sync_points_can_force_get_set_ordering_between_threads -- --nocapture`
- `cargo test -p garnet-server strlen_returns_zero_for_missing_and_length_for_string -- --nocapture`
- `cargo test -p garnet-server getrange_and_substr_return_expected_slices -- --nocapture`
- `cargo test -p garnet-server setnx_sets_only_when_key_absent -- --nocapture`

The broader suite was green during the trial run, but the experiment was rejected on benchmark results before acceptance, so the retained evidence is the targeted regression set plus the A/B report.

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260322-11.547 \
BASE_BIN=<baseline binary> \
NEW_BIN=<experiment binary> \
BASE_LABEL=baseline \
NEW_LABEL=current \
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

The experiment regressed the hot path and was dropped:

- `SET ops 105472 -> 97929` (`-7.15%`)
- `GET ops 113859 -> 104069` (`-8.60%`)
- `SET p99 0.119 -> 0.127 ms` (`+6.72%`)
- `GET p99 0.103 -> 0.135 ms` (`+31.07%`)

## Artifacts

- `comparison.txt`
- `before-summary.txt`
- `before-runs.csv`
- `after-summary.txt`
- `after-runs.csv`
- `diff.patch` contains the explicit reason a retained patch is unavailable
