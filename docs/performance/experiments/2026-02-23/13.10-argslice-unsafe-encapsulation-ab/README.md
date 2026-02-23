# 13.10 ArgSlice Unsafe Encapsulation A/B

- date: 2026-02-23
- todo: 11.124
- before_commit: `dc959cb2ce` (`perf: reduce SET allocation churn and capture alloc evidence`)
- after_commit: `THIS_COMMIT` (unsafe encapsulation change-set)
- revert_commit: `N/A`

## Goal

Encapsulate `ArgSlice::as_slice` unsafety behind shared helpers and remove scattered direct `unsafe { ...as_slice() }` usage from command handlers and command-path modules, then verify no obvious hot-path regression.

## Code Scope

- `garnet-rs/crates/garnet-server/src/request_lifecycle*.rs` and handler submodules
- `garnet-rs/crates/garnet-server/src/connection_handler.rs`
- `garnet-rs/crates/garnet-server/src/connection_routing.rs`
- `garnet-rs/crates/garnet-server/src/redis_replication.rs`
- `garnet-rs/crates/garnet-server/src/command_dispatch.rs`
- `garnet-rs/crates/garnet-server/src/tests.rs`

## Method

Built two binaries and compared with `garnet-rs/benches/binary_ab_local.sh`.

```bash
cd garnet-rs
BASE_BIN=/tmp/garnet-ab-unsafe/garnet-base-unsafe \
NEW_BIN=/tmp/garnet-ab-unsafe/garnet-new-unsafe \
BASE_LABEL=before NEW_LABEL=after \
RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 PRELOAD_REQUESTS=5000 \
PIPELINE=1 SIZE_RANGE=1-1024 \
OUTDIR=/tmp/garnet-ab-unsafe/results \
./benches/binary_ab_local.sh
```

## Result (Median)

- SET ops/s: `155079 -> 168360` (`+8.56%`)
- GET ops/s: `173206 -> 169718` (`-2.01%`)
- SET p99 ms: `0.487 -> 0.375` (`-23.00%`)
- GET p99 ms: `0.319 -> 0.335` (`+5.02%`)

Interpretation: mixed but small trade-off profile on this local shape; no catastrophic regression observed.

## Artifacts

- `comparison.txt`
- `summary-before.txt`
- `summary-after.txt`
- `runs-before.csv`
- `runs-after.csv`
- `diff.patch`
