# Experiment - Request Burst Time Snapshot (Dropped)

## Metadata

- date: `2026-03-24`
- status: `REJECTED`
- before_commit: `c9ff8f777d`
- after_commit: `N/A (working-tree-only experiment)`
- revert_commit: `N/A (working-tree revert before commit)`
- baseline_binary: `/private/tmp/garnet-11.567-baseline-target/release/garnet-server`
- candidate_binary: `/private/tmp/garnet-11.567-current-target/release/garnet-server`

## Goal

Try fixing request time once for already-queued work instead of reading `now`
for each dequeued request.

The intended semantics were:

- transaction queues: when `EXEC` drains multiple queued commands, share one
  request-start time across those queued commands
- socket receive bursts: when multiple RESP frames are already buffered, let the
  already-queued tail reuse one captured time snapshot instead of reading wall
  clock per dequeue

## Process Note

This was explored as a working-tree-only experiment and reverted before commit
because benchmark evidence never stabilized into a clean keep signal. That does
not match the newer one-experiment-one-commit policy, so the missing
`after_commit` / `revert_commit` hashes are recorded explicitly here.

## Candidate Design

- Add a request-scoped `TimeSnapshot` carrying both `unix_micros` and
  monotonic `Instant`
- Thread an optional snapshot through owner-thread execution helpers
- Use one captured snapshot for queued transaction items
- For socket bursts, eventually narrow the scope so only frames already queued
  behind the current frame reused the snapshot
- Preserve script frozen-time precedence

During the experiment, one regress root-cause did become clear: the first
version made `current_*time*()` consult script state via an extra common-path
TLS/lock indirection even when no script was running. That part was fixed, but
the overall slice still did not benchmark well enough.

## Benchmark Parameters

Common A/B harness:

- script: `garnet-rs/benches/binary_ab_local.sh`
- `BASE_BIN=/private/tmp/garnet-11.567-baseline-target/release/garnet-server`
- `NEW_BIN=/private/tmp/garnet-11.567-current-target/release/garnet-server`
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

## Final Evidence

The final narrowed design still failed the keep bar.

### `PIPELINE=1`

- artifact: [comparison-p1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.567-request-burst-time-snapshot-drop/comparison-p1.txt)
- `SET ops 130538 -> 133293` (`+2.11%`)
- `GET ops 149189 -> 145259` (`-2.63%`)
- `SET p99 0.071 -> 0.071 ms` (`0.00%`)
- `GET p99 0.071 -> 0.071 ms` (`0.00%`)

### `PIPELINE=16`

- artifact: [comparison-p16.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.567-request-burst-time-snapshot-drop/comparison-p16.txt)
- `SET ops 485286 -> 484062` (`-0.25%`)
- `GET ops 698707 -> 634352` (`-9.21%`)
- `SET p99 0.239 -> 0.239 ms` (`0.00%`)
- `GET p99 0.151 -> 0.183 ms` (`+21.19%`)

## Decision

Drop the slice.

Reason:

- The intended queue-heavy shape did not produce a stable win.
- The final burst-only narrowing still regressed `GET`, especially on
  `PIPELINE=16`.
- Keeping the extra plumbing without clear benchmark upside would add
  complexity to a sensitive execution path.

## Follow-up Learning

- The core idea is still semantically sound.
- If we revisit it, the next version should be narrower:
  - avoid global `current_*time*()` indirection
  - target one concrete metadata path first
  - prove the win with a workload that actually spends meaningful CPU in
    timekeeping before widening the scope
