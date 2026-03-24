# 11.576 DbKeyMap borrowed update

- status: accepted
- before_commit: `6f2ea5fbfca4963fba597a253f334bc2e084a8b5`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Take the next evidence-driven cut at `record_key_access(...)`.

The fresh local flamegraph still showed `record_key_access` and
`DbKeyMapState::insert` in both plain `GET` and `SET`. The hot metadata path was
still constructing a fresh owned `RedisKey` on every access, even when the key
was already present in the per-shard map and the operation was just overwriting
its timestamp/frequency value.

## Change

- changed `DbKeyMapState::insert(...)` to update an existing borrowed-key entry
  in place when the key is already present
- fall back to cloning `RedisKey` only on the first insert for that key

This keeps the data model and locking unchanged while removing a per-access key
allocation/copy from the common metadata update path.

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server db_scoped_object_access_metadata_does_not_leak_between_databases -- --nocapture`
- `cargo test -p garnet-server swapdb_zero_and_nonzero_preserves_object_access_metadata -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `712 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark commands

Baseline binary:

```bash
git worktree add --detach /private/tmp/garnet-11.576-base-wt HEAD
CARGO_TARGET_DIR=/private/tmp/garnet-11.576-baseline-target cargo build -p garnet-server --release
```

Candidate binary:

```bash
CARGO_TARGET_DIR=/private/tmp/garnet-11.576-current-target cargo build -p garnet-server --release
```

Owner-inline `PIPELINE=1` run 1:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.576-p1 \
BASE_BIN=/private/tmp/garnet-11.576-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.576-current-target/release/garnet-server \
BASE_LABEL=baseline_metadata_key_clone \
NEW_LABEL=borrowed_metadata_update \
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
OUTDIR=/tmp/garnet-binary-ab-20260324-11.576-p1-r2 \
BASE_BIN=/private/tmp/garnet-11.576-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.576-current-target/release/garnet-server \
BASE_LABEL=baseline_metadata_key_clone \
NEW_LABEL=borrowed_metadata_update \
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

## Result

Two identical reruns both landed strongly positive:

- run 1: `SET +72.30%`, `GET +51.60%`, `SET p99 -57.79%`, `GET p99 -54.11%`
- run 2: `SET +100.99%`, `GET +88.96%`, `SET p99 -64.11%`, `GET p99 -55.81%`

Raw medians:

- run 1 baseline: `SET 59357`, `GET 70057`, `p99 0.263/0.207 ms`
- run 1 candidate: `SET 102271`, `GET 106205`, `p99 0.111/0.095 ms`
- run 2 baseline: `SET 51840`, `GET 64457`, `p99 0.287/0.215 ms`
- run 2 candidate: `SET 104195`, `GET 121800`, `p99 0.103/0.095 ms`

## Interpretation

- The magnitude is large, but the direction repeated twice and matches the
  flamegraph suspicion: metadata key cloning was far more expensive than it
  looked from code inspection alone.
- This slice keeps the locking model and metadata semantics intact, so the gain
  is coming from removing a pure hot-path allocation/copy rather than from a
  semantic shortcut.
- With this accepted, the next pass should stay on the adjacent client
  metrics/time bookkeeping lane before moving to the larger `GET` response-path
  work.

## Artifacts

- `comparison-p1.txt`
- `comparison-p1-rerun.txt`
- `summary.txt`
- `diff.patch`
