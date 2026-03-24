# 11.583 combined input and last-command metrics

- status: accepted
- before_commit: `5ae2d535d76967f8edfe4f9d2cc794fdc58959b0`
- after_commit: pending in this commit
- revert_commit: not applicable

## Goal

Take another small evidence-driven cut at the client-metrics bookkeeping still
visible in the plain request flamegraph.

The hot request loop still updated input bytes and last command through two
separate client-table lock acquisitions even though both writes touch the same
`ClientInfo` entry on every request.

## Change

- added `add_client_input_bytes_and_last_command(...)` on `ServerMetrics`
- updated the request loop to coalesce input-byte accounting and normalized
  last-command bookkeeping under one `clients` lock
- kept the existing repeated-command fast path, so unchanged `GET` / `SET`
  traffic still skips the lowercasing/buffer rewrite work

## Validation

Targeted Rust tests:

- `cargo test -p garnet-server combined_input_and_last_command_updates_counters_activity_and_lowercased_command -- --nocapture`
- `cargo test -p garnet-server client_last_activity_only_tracks_input_and_output_not_internal_bookkeeping -- --nocapture`
- `cargo test -p garnet-server client_last_command_stores_lowercased_command_and_subcommand -- --nocapture`
- `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`

Full gate:

- `make fmt`
- `make fmt-check`
- `make test-server`
- verified counts: `713 passed; 0 failed; 16 ignored`, plus `26 passed`, plus `1 passed`

## Benchmark commands

Baseline binary:

```bash
git worktree add --detach /private/tmp/garnet-11.582-base-wt.DdpM5p HEAD
CARGO_TARGET_DIR=/private/tmp/garnet-11.582-baseline-target cargo build -p garnet-server --release
```

Candidate binary:

```bash
CARGO_TARGET_DIR=/private/tmp/garnet-11.583-current-target cargo build -p garnet-server --release
```

Owner-inline `PIPELINE=1` run 1:

```bash
OUTDIR=/tmp/garnet-binary-ab-20260324-11.583-p1 \
BASE_BIN=/private/tmp/garnet-11.582-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.583-current-target/release/garnet-server \
BASE_LABEL=baseline_split_input_last_command \
NEW_LABEL=combined_input_last_command \
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
OUTDIR=/tmp/garnet-binary-ab-20260324-11.583-p1-r2 \
BASE_BIN=/private/tmp/garnet-11.582-baseline-target/release/garnet-server \
NEW_BIN=/private/tmp/garnet-11.583-current-target/release/garnet-server \
BASE_LABEL=baseline_split_input_last_command \
NEW_LABEL=combined_input_last_command \
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

Both identical owner-inline reruns were clean wins:

- run 1: `SET +1.29%`, `GET +4.84%`, `SET p99 flat`, `GET p99 -10.13%`
- run 2: `SET +2.90%`, `GET +3.34%`, `SET p99 flat`, `GET p99 flat`

Raw medians:

- run 1 baseline: `SET 127985`, `GET 147268`, `p99 0.071/0.079 ms`
- run 1 candidate: `SET 129637`, `GET 154391`, `p99 0.071/0.071 ms`
- run 2 baseline: `SET 126790`, `GET 150672`, `p99 0.071/0.071 ms`
- run 2 candidate: `SET 130470`, `GET 155711`, `p99 0.071/0.071 ms`

## Interpretation

- The gain is modest, but it repeated twice and stays aligned with the flamegraph
  suspicion: this request-loop bookkeeping is still worth cleaning up when the
  change is small and local.
- The merged helper keeps the behavior readable and does not widen any
  cross-cutting API surface beyond the existing `ServerMetrics` boundary.

## Artifacts

- `comparison-run1.txt`
- `comparison-run2.txt`
- `summary.txt`
- `diff.patch`
