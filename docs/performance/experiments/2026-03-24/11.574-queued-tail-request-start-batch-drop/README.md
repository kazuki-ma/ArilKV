# Experiment - Queued Tail Request Start Batch (Dropped)

## Metadata

- date: `2026-03-24`
- status: `REJECTED`
- before_commit: `4691331b251bfecce3a43e09e980a1c98e829e0a`
- after_commit: `N/A (working-tree-only experiment)`
- revert_commit: `N/A (working-tree revert before commit)`
- baseline_binary: `/private/tmp/garnet-11.574-baseline-target/release/garnet-server`
- candidate_binary: `/private/tmp/garnet-11.574-current-target/release/garnet-server`

## Goal

Retry the earlier rejected request-burst clock idea in a much narrower form.

The intended semantics for this pass were:

- keep the first request in a socket burst on its own `Instant::now()`
- once a request has already started and more parsed requests are queued behind it,
  capture one more `Instant` and reuse it for that queued tail
- thread that request-start instant through owner-thread execution and client
  input/output accounting instead of reading another clock per dequeued tail item

This matches the intended consistency argument: already-buffered tail requests
have already entered execution order, so sharing one start timestamp across the
queued tail should not violate linearization.

## Candidate Design

- add `RequestExecutionContext.request_started_at`
- add `RequestProcessor::execute_with_client_tracking_context_and_effects_started_at_in_db(...)`
- thread `request_started_at` through owner-thread routing helpers
- add `ServerMetrics::{add_client_input_bytes_at, add_client_output_bytes_at}`
- in `connection_handler`, keep request 1 on its own `Instant::now()`, then let
  request 2+ in the same buffered burst reuse one queued-tail timestamp

Process note:

- the first broader attempt reused one timestamp for the whole burst and was
  immediately bad
- the final decision below is based on the narrowed queued-tail-only design
- `diff.patch` in this directory captures the initial working-tree candidate;
  the queued-tail narrowing was a small follow-up change in the connection
  handler before the experiment was reverted

## Validation

- targeted revert verification:
  - `cargo test -p garnet-server executes_set_then_get_roundtrip -- --nocapture`
  - PASS after reverting the working tree to baseline behavior

## Benchmark Parameters

Common A/B harness:

- script: `garnet-rs/benches/binary_ab_local.sh`
- `BASE_BIN=/private/tmp/garnet-11.574-baseline-target/release/garnet-server`
- `NEW_BIN=/private/tmp/garnet-11.574-current-target/release/garnet-server`
- `RUNS=5`
- `THREADS=1`
- `CONNS=4`
- `REQUESTS=50000`
- `PRELOAD_REQUESTS=50000`
- `SIZE_RANGE=1-256`
- `OWNER_THREADS=1`
- `GARNET_OWNER_THREAD_PINNING=1`
- `GARNET_OWNER_THREAD_CPU_SET=0`
- `GARNET_OWNER_EXECUTION_INLINE=1`

Variant-specific parameter:

- `PIPELINE=1`
- `PIPELINE=16`

## Evidence

### Broad first attempt (`PIPELINE=1`)

- artifact: [comparison-p1-broad.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.574-queued-tail-request-start-batch-drop/comparison-p1-broad.txt)
- `SET ops 122703 -> 114797` (`-6.44%`)
- `GET ops 154822 -> 151386` (`-2.22%`)
- `SET p99 0.079 -> 0.095 ms` (`+20.25%`)
- `GET p99 0.087 -> 0.079 ms` (`-9.20%`)

This was the “entire burst shares one timestamp” shape and was rejected
immediately.

### Narrowed queued-tail-only design (`PIPELINE=1`, run 1)

- artifact: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.574-queued-tail-request-start-batch-drop/comparison-p1.txt)
- `SET ops 125180 -> 131101` (`+4.73%`)
- `GET ops 149172 -> 134806` (`-9.63%`)
- `SET p99 0.071 -> 0.071 ms` (`0.00%`)
- `GET p99 0.071 -> 0.079 ms` (`+11.27%`)

### Narrowed queued-tail-only design (`PIPELINE=1`, rerun)

- artifact: [comparison-p1-rerun.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.574-queued-tail-request-start-batch-drop/comparison-p1-rerun.txt)
- `SET ops 121116 -> 132148` (`+9.11%`)
- `GET ops 153594 -> 134051` (`-12.72%`)
- `SET p99 0.079 -> 0.071 ms` (`-10.13%`)
- `GET p99 0.071 -> 0.087 ms` (`+22.54%`)

### Narrowed queued-tail-only design (`PIPELINE=16`)

- artifact: [comparison-p16.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.574-queued-tail-request-start-batch-drop/comparison-p16.txt)
- `SET ops 455179 -> 504877` (`+10.92%`)
- `GET ops 689966 -> 699914` (`+1.44%`)
- `SET p99 0.223 -> 0.215 ms` (`-3.59%`)
- `GET p99 0.159 -> 0.167 ms` (`+5.03%`)

## Decision

Drop the slice.

Reason:

- the narrowed version did help the queue-heavy write shape
- but the common `PIPELINE=1` read path regressed twice in the same direction
- `GET p99` also worsened in the `PIPELINE=16` queue-heavy shape
- that is not a good trade for a common-path change in connection execution

## Follow-up Learning

- the user-level consistency argument still looks sound
- the performance trade does not justify this implementation shape
- if we revisit this idea, the next version should target one concrete metadata
  path instead of coupling request execution context plus reply accounting to the
  same shared start instant
