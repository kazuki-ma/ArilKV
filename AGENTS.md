# AGENTS.md

## Scope

This file defines default collaboration and execution rules for repository work, especially `garnet-rs` implementation/performance tasks.

## Canonical Working Files

- Tracker: `TODO_AND_STATUS.md`
- Experiment policy: `docs/performance/EXPERIMENT_REPORTING_POLICY.md`
- DeepResearch request policy: `docs/performance/deepresearch-request-guidelines-2026-02-21.md`
- Latest actor/locking guidance notes: `docs/performance/actor-lock-minimization-notes-2026-02-21.md`

## Non-Negotiable Workflow

1. Update `TODO_AND_STATUS.md` every iteration.
2. Keep commits small and logically scoped.
3. Do not merge performance-sensitive changes without benchmark evidence.
4. Do not rely on command exit status alone for tests; verify reported test counts and failed-case absence in output.
5. Keep compatibility-sensitive behavior changes backed by explicit tests.

## Required Validation (Default)

- Core test gate:
  - `cd garnet-rs && cargo test -p garnet-server`
  - Verify displayed test-case counts and pass/fail summary.
- If command-path or concurrency code changed:
  - Run hot-path benchmark comparison with identical parameters before/after.
  - Preferred harness: `garnet-rs/benches/binary_ab_local.sh`
- If a benchmark script/test script is edited:
  - Re-run that script in a smoke configuration and verify integrity checks.

## Performance Regression Policy

- For hot-path changes, run before/after comparison using fixed parameters.
- Record throughput and p99 deltas.
- If regression appears, either:
  - revert the change, or
  - keep it only with explicit rationale and follow-up task.

## Experiment Reporting Policy

- Store each experiment under:
  - `docs/performance/experiments/<YYYY-MM-DD>/<experiment-slug>/`
- Include:
  - `README.md`
  - lightweight artifacts (`summary.txt`, `runs.csv`, `comparison.txt`, log excerpts)
  - `diff.patch`
  - before/after commit hashes

## DeepResearch Operations

- Trigger DeepResearch when:
  - architecture/concurrency model changes are being considered,
  - compatibility and performance goals conflict,
  - benchmark outcomes are noisy or ambiguous,
  - specialized prior-art comparison is required.
- Track all requests in `TODO_AND_STATUS.md` Phase 11B.
- Status lifecycle:
  - `REQUESTED_WAITING` -> `RECEIVED`
- On return:
  - import report into `docs/performance/`
  - add concise actionable notes
  - update tracker and iteration log

## Current Directional Guidance

- Prefer owner-thread actor serialization for mutation paths.
- Limit optimistic shared-read techniques to read-mostly metadata/snapshots.
- Keep deterministic fallback paths for optimistic reads.
- Handle cross-shard ordering needs with explicit coordinator/barrier logic, not coarse global data locks.
