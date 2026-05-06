# AGENTS.md

Repository rule: actively consume the remaining TODO backlog. When a tracked TODO is completed and verified, remove it from the active TODO list instead of leaving completed backlog items in place.

## Scope

This file defines default collaboration and execution rules for repository work, especially Garnet-rs implementation and performance tasks.

## Canonical Working Files

- Tracker: `TODO_AND_STATUS.md`
- TLA+ local workflow quickstart: `formal/tla/README.md`

## Rust Toolchain And Make Workflow

- Use `make` targets from repo root as the default entrypoint.
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
4. If a commit must cover multiple TODOs, record explicit rationale in `TODO_AND_STATUS.md`.
5. Do not merge performance-sensitive changes without benchmark evidence.
6. Do not rely on command exit status alone for tests; verify reported test counts and failed-case absence in output.
7. Keep compatibility-sensitive behavior changes backed by explicit tests.
8. When an external interop test fails, first add a Rust unit/integration test that reproduces the external scenario itself before or together with the fix.

## Code Quality Baseline

- Always target production-quality code.
- Prefer explicit, step-by-step control flow over dense chained combinators when code is on a critical path or has branching behavior.
- Keep parse/validate/state-mutation/response-emission phases separated when practical.
- If readability and micro-optimization conflict and no benchmark evidence exists, choose readability first and optimize later with measurement.
- Do not optimize for a small diff if it leaves the resulting code path dirty or inconsistent.
- For performance-sensitive work: make the implementation clean first, then optimize with measurement-backed changes.

## TLA+ Annotation Policy

- Some critical concurrency/command paths are formally modeled with TLA+.
- For files that implement a modeled path:
  - Add a file-level comment indicating the related TLA+ model/spec.
  - Annotate each modeled critical section with exact action labels: `// TLA+ : <ActionName>`.
- Keep action labels in code comments synchronized with names in `formal/tla/specs/*.tla`.

## Required Validation

- Core test gate: `make test-server`.
- Verify displayed test-case counts and pass/fail summary.
- If command-path or concurrency code changed, run hot-path benchmark comparison with identical parameters before/after.
- Preferred benchmark harness: `benches/binary_ab_local.sh`.
- If a benchmark script/test script is edited, rerun that script in a smoke configuration and verify integrity checks.

## Command Coverage Workflow

When command behavior or command declarations change (`request_lifecycle`, `command_spec`, `command_dispatch`), run this sequence:

1. Add or update exact Rust regression tests for the touched external scenario.
2. Run the narrowest relevant Rust test selection for those regressions.
3. Run `make test-server` or `cargo test -p garnet-server -- --nocapture` when stdout detail is required.
4. Run targeted external verification only for the affected Redis test scope.
5. Run `tests/interop/build_compatibility_report.sh` only at milestone boundaries or when shared command infrastructure changed.

## Implementation Reference Policy

For future command implementation work, use this source-of-truth order:

1. Command semantics and edge-case behavior: prefer Valkey source as primary reference.
2. This repository's architecture and integration constraints: prefer existing Garnet-rs patterns.
3. Additional implementation patterns when needed: use other BSD/MIT-compatible sources.

Guardrails:

- Do not copy large code blocks verbatim from external projects.
- Always validate behavior via the command workflow above.

## Current Directional Guidance

- Prefer owner-thread actor serialization for mutation paths.
- Limit optimistic shared-read techniques to read-mostly metadata/snapshots.
- Keep deterministic fallback paths for optimistic reads.
- Handle cross-shard ordering needs with explicit coordinator/barrier logic, not coarse global data locks.
