# 11.562 commandstats atomic top-level fast path

- status: dropped
- before_commit: `a60cd5a56042941992f731745379deca4c37f5f0`
- after_commit: unavailable; reverted working-tree experiment
- revert_commit: not applicable

## Goal

Remove the per-command `command_calls` mutex from the plain dispatched `GET`/`SET` path by counting top-level commands in atomics and leaving only subcommands/internal ad-hoc names on the existing map path.

## Change

- added a dispatched-command `commandstats` fast path that counted top-level commands in an atomic array keyed by `CommandId`
- kept `record_command_call(...)` and subcommand/internal names on the existing lowercase `HashMap<Vec<u8>, u64>` path
- merged atomic top-level counts back into `INFO COMMANDSTATS` snapshots and added id-aware undo handling for the blocking ACL rejection path

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server commandstats_snapshot_merges_atomic_top_level_and_map_counts -- --nocapture`
- `cargo test -p garnet-server undo_recorded_dispatched_command_call_reverts_atomic_top_level_counts -- --nocapture`

Both passed.

Full gate while the slice was applied:

- `make fmt`
- `make fmt-check`
- `make test-server`

Verified counts:

- `712 passed; 0 failed; 16 ignored`
- plus `26 passed`
- plus `1 passed`

## Benchmark command

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.562-r3 \
BASE_BIN=/tmp/garnet-11.562-baseline-76087/garnet-rs/target/release/garnet-server \
NEW_BIN=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/garnet-rs/target/release/garnet-server \
BASE_LABEL=commandstats_mutex \
NEW_LABEL=commandstats_atomic_top_level \
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

This was dropped because three identical owner-inline reruns did not converge to a clean win.

First run:

- `SET ops 116009 -> 108529` (`-6.45%`)
- `GET ops 142862 -> 148767` (`+4.13%`)
- `SET p99 0.079 -> 0.087 ms` (`+10.13%`)
- `GET p99 0.071 -> 0.071 ms` (`0.00%`)

Second run:

- `SET ops 132689 -> 133856` (`+0.88%`)
- `GET ops 150577 -> 152531` (`+1.30%`)
- flat `SET/GET p99` (`0.071/0.071 ms`)

Third run:

- `SET ops 123200 -> 130140` (`+5.63%`)
- `GET ops 151410 -> 145197` (`-4.10%`)
- `SET p99 0.079 -> 0.071 ms` (`-10.13%`)
- `GET p99 0.063 -> 0.071 ms` (`+12.70%`)

The direction kept flipping between `SET` and `GET`, so this is not solid enough to keep in the hot path. The code was reverted.

## Artifacts

- `first-comparison.txt`
- `second-comparison.txt`
- `third-comparison.txt`
- `diff.patch`
