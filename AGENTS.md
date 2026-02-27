# AGENTS.md

## Scope

This file defines default collaboration and execution rules for repository work, especially `garnet-rs` implementation/performance tasks.

## Canonical Working Files

- Tracker: `TODO_AND_STATUS.md`
- Experiment policy: `docs/performance/EXPERIMENT_REPORTING_POLICY.md`
- DeepResearch request policy: `docs/performance/deepresearch-request-guidelines-2026-02-21.md`
- Latest actor/locking guidance notes: `docs/performance/actor-lock-minimization-notes-2026-02-21.md`
- TLA+ local workflow quickstart: `formal/tla/README.md`

## Rust Toolchain And Make Workflow

- Use `make` targets from repo root as the default entrypoint for `garnet-rs`.
- Build/test runs use the stable release pinned in `rust-toolchain.toml`.
- Formatting uses pinned nightly rustfmt through `Makefile` (`RUSTFMT_NIGHTLY`).
- Avoid ad-hoc direct `cargo +toolchain ...` commands in normal workflow; prefer:
  - `make fmt`
  - `make fmt-check`
  - `make check`
  - `make test`
  - `make test-server`
  - `make clippy`

## Non-Negotiable Workflow

1. Update `TODO_AND_STATUS.md` every iteration.
2. Keep commits small and logically scoped.
3. Enforce granularity: `1 TODO = 1 iteration = 1 commit` by default.
4. If a commit must cover multiple TODOs (for mechanical coupling only), record explicit rationale in `TODO_AND_STATUS.md`.
5. Do not merge performance-sensitive changes without benchmark evidence.
6. Do not rely on command exit status alone for tests; verify reported test counts and failed-case absence in output.
7. Keep compatibility-sensitive behavior changes backed by explicit tests.

## Code Quality Baseline

- Always target production-quality code.
- "High quality" means practical, maintainable, and readable implementation quality, not clever or flashy code.
- Do not intentionally keep known-correctness or known-design gaps just to make the diff look small.
- Do not keep required structural changes out of scope only to preserve a "minimal diff" appearance.
- Use temporary/partial steps only when they are technically necessary, and record explicit rationale plus follow-up TODO.
- Avoid casual "for now" / "first" staging language unless there is a concrete technical dependency that enforces staged delivery.
- Avoid `impl`/`.as_`-style conversion helpers in internal APIs; use them only at true boundaries (serialization and external I/O), and keep internal APIs unified on concrete domain types.
- For performance-sensitive work: make the implementation clean first; once a path is proven performance-critical, keep the code readable/beautiful first, then optimize with measurement-backed changes.
- Spending more time to reach a robust implementation is preferred over fast-but-fragile patches.

## Required Validation (Default)

- Core test gate:
  - `make test-server`
  - Verify displayed test-case counts and pass/fail summary.
- If command-path or concurrency code changed:
  - Run hot-path benchmark comparison with identical parameters before/after.
  - Preferred harness: `garnet-rs/benches/binary_ab_local.sh`
- If a benchmark script/test script is edited:
  - Re-run that script in a smoke configuration and verify integrity checks.

## Command Coverage Workflow

When command behavior or command declarations change (`request_lifecycle`,
`command_spec`, `command_dispatch`), run this sequence:

1. `make test-server` (or `cd garnet-rs && cargo test -p garnet-server -- --nocapture` when stdout detail is required)
2. `cd garnet-rs/tests/interop && REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis ./build_compatibility_report.sh`

Commit the generated compatibility matrix files when status changes:

- `docs/compatibility/redis-command-status.csv`
- `docs/compatibility/redis-command-status-summary.md`
- `docs/compatibility/redis-command-maturity.csv`
- `docs/compatibility/redis-command-maturity-summary.md`
- `docs/compatibility/compatibility-report.md`

When a compatibility artifact changes, commit it in the same commit as the
related code change (do not carry compatibility-report-only drift across
multiple subsequent code commits).

## Implementation Reference Policy

For future command implementation work, use this source-of-truth order:

1. Command semantics and edge-case behavior:
   - Prefer `Valkey` source as primary reference.
   - Local path: `/Users/kazuki-matsuda/dev/src/github.com/valkey-io/valkey`
2. This repository's architecture and integration constraints:
   - Prefer `garnet-rs` existing patterns in this repo.
3. Additional implementation patterns (when needed):
   - Use `.NET Garnet` code in this repo as secondary guidance.

Rationale:

- Valkey is BSD-3-Clause and avoids Redis post-license-change ambiguity.
- `garnet-rs` must still follow its own threading/locking/testing conventions.

Guardrails:

- Do not copy large code blocks verbatim from external projects.
- Always validate behavior via the command workflow above and keep matrix files updated.

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
