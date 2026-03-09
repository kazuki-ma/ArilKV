# AGENTS.md

## Scope

This file defines default collaboration and execution rules for repository work, especially `garnet-rs` implementation/performance tasks.

## Canonical Working Files

- Tracker: `TODO_AND_STATUS.md`
- Experiment policy: `docs/performance/EXPERIMENT_REPORTING_POLICY.md`
- DeepResearch request policy: `docs/performance/deepresearch-request-guidelines-2026-02-21.md`
- Latest actor/locking guidance notes: `docs/performance/actor-lock-minimization-notes-2026-02-21.md`
- TLA+ local workflow quickstart: `formal/tla/README.md`
- Codex CLI execution runbook (MCP timeout fallback): `docs/operations/codex-cli-execution.md`

## Codex Execution Path Policy

- Do not run long-lived repository work through Codex MCP server calls.
- If MCP-based execution times out, switch immediately to local `codex exec` CLI.
- For this repository, treat `docs/operations/codex-cli-execution.md` as the default operational guide for Codex task execution.

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
8. When an external interop test fails, first add a Rust unit/integration test that reproduces the external scenario itself before or together with the fix; do not replace it with a smaller synthetic repro as the primary regression test.

## Code Quality Baseline

- Always target production-quality code.
- "High quality" means practical, maintainable, and readable implementation quality, not clever or flashy code.
- Do not intentionally keep known-correctness or known-design gaps just to make the diff look small.
- Do not keep required structural changes out of scope only to preserve a "minimal diff" appearance.
- Use temporary/partial steps only when they are technically necessary, and record explicit rationale plus follow-up TODO.
- Avoid casual "for now" / "first" staging language unless there is a concrete technical dependency that enforces staged delivery.
- Avoid `impl`/`.as_`-style conversion helpers in internal APIs; use them only at true boundaries (serialization and external I/O), and keep internal APIs unified on concrete domain types.
- Prefer explicit, step-by-step control flow over dense chained combinators when code is on a critical path or has branching behavior.
- Break complex expressions into named intermediate variables/helpers when that improves intent readability; do not compress multiple semantic steps into one expression.
- Keep parse/validate/state-mutation/response-emission phases separated when practical, so behavior can be reasoned about locally.
- If readability and micro-optimization conflict and no benchmark evidence exists, choose readability first and optimize later with measurement.
- "Code cleanliness" is judged by post-change readability of the whole code path, not by keeping a local patch minimal.
- Do not optimize for a small or pretty-looking diff if it leaves the resulting code path dirty, inconsistent, or non-beautiful.
- If better final readability requires broader edits, do the needed refactor up front; otherwise record explicit follow-up TODOs and do not silently leave structural debt.
- For performance-sensitive work: make the implementation clean first; once a path is proven performance-critical, keep the code readable/beautiful first, then optimize with measurement-backed changes.
- Spending more time to reach a robust implementation is preferred over fast-but-fragile patches.

## TLA+ Annotation Policy

- Some critical concurrency/command paths are formally modeled with TLA+.
- For files that implement a modeled path:
  - add a file-level comment indicating the related TLA+ model/spec.
  - annotate each modeled critical section with exact action labels:
    - `// TLA+ : <ActionName>`
- Keep action labels in code comments synchronized with names in `formal/tla/specs/*.tla`.

## Required Validation (Default)

- Core test gate:
  - `make test-server`
  - Verify displayed test-case counts and pass/fail summary.
- During active compatibility backlog burn, use targeted-first validation for command behavior changes:
  - First add or update exact Rust regression tests that mirror the touched external scenario.
  - Run targeted Rust test selections for those regressions.
  - Run targeted external verification only for the affected Redis unit/case/profile.
  - Defer `redis-runtest-external-full` / `build_compatibility_report.sh` to milestone boundaries or explicit escalation cases listed below.
- If command-path or concurrency code changed:
  - Run hot-path benchmark comparison with identical parameters before/after.
  - Preferred harness: `garnet-rs/benches/binary_ab_local.sh`
- If a benchmark script/test script is edited:
  - Re-run that script in a smoke configuration and verify integrity checks.

## Command Coverage Workflow

When command behavior or command declarations change (`request_lifecycle`,
`command_spec`, `command_dispatch`), run this sequence:

1. Add or update exact Rust regression tests for the touched external scenario.
2. Run the narrowest relevant Rust test selection for those regressions.
3. `make test-server` (or `cd garnet-rs && cargo test -p garnet-server -- --nocapture` when stdout detail is required)
4. Run targeted external verification only for the affected Redis test scope (single case, isolated unit file, or focused profile).
5. Run `cd garnet-rs/tests/interop && REDIS_REPO_ROOT=/Users/kazuki-matsuda/dev/src/github.com/redis/redis ./build_compatibility_report.sh` only when one of these is true:
   - a task family or matrix slice is being closed,
   - shared command infrastructure changed (`command_spec`, `command_dispatch`, broad parser/routing surfaces, replication rewrite plumbing),
   - targeted probes suggest cross-test contamination or unknown regressions,
   - compatibility artifacts must be refreshed for merge/review/release,
   - the user explicitly asks for a full rerun.

Commit the generated compatibility matrix files when a full rerun is performed and status changes:

- `docs/compatibility/redis-command-status.csv`
- `docs/compatibility/redis-command-status-summary.md`
- `docs/compatibility/redis-command-maturity.csv`
- `docs/compatibility/redis-command-maturity-summary.md`
- `docs/compatibility/compatibility-report.md`

When a compatibility artifact changes from a full rerun, commit it in the same
commit as the related code change (do not carry compatibility-report-only
drift across multiple subsequent code commits).

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
- Always validate behavior via the command workflow above and keep matrix files updated at full-rerun milestones.

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
