# TLA+ Onboarding Notes (2026-02-22)

## Source

- DeepResearch report (imported):
  - `docs/performance/tlaplus-onboarding-model-checking-deepresearch-2026-02-22.md`
- Instruction used:
  - `docs/performance/DEEPRESEARCH_TLAPLUS_MODELING_INSTRUCTION_2026-02-22.md`

## Key Findings (Actionable)

- Start with TLC-centered bounded models and keep Toolbox/CLI dual workflow:
  - Toolbox for local authoring/debug.
  - CLI TLC for reproducible automation.
- Scope first model to concurrency invariants only:
  - owner-thread routing/serialization
  - blocking wakeup/fairness/timeout semantics
  - replication apply-order prefix guarantees
- Prioritize safety invariants first, then add liveness/fairness assumptions incrementally.
- Keep disconnect/cancel/unblock interleavings explicit in blocking models.
- Treat state-space control as first-class:
  - small cardinalities
  - bounded steps
  - symmetry/model values where possible
- Standardize counterexample-to-test pipeline:
  - preserve TLC trace metadata
  - convert to deterministic async schedule (barriers/channels)
  - add provenance links in Rust tests

## Assessment

- The report quality is good for current project direction:
  - recommendations are practical and align with existing owner-thread architecture work.
- Most valuable guidance for immediate use:
  - strict model scoping, bounded TLC loops, and deterministic test conversion workflow.

## Proposed Repository-level Rules

- Rule 1: Any blocking/disconnect behavior change must update corresponding TLA+ model(s) and run TLC.
- Rule 2: New concurrency invariants should be encoded in TLA+ first (or in the same PR) before non-trivial refactors.
- Rule 3: If TLC produces a counterexample, add a deterministic Rust regression test before merge.
- Rule 4: Keep baseline TLA+ models bounded to "minutes, not hours" runtime by default.

## Candidate Work Items

- Add replication-order focused TLA+ model (`append/apply prefix` safety).
- Add multi-key blocking selection model (key-order + waiter FIFO interaction).
- Add CLIENT UNBLOCK interaction model for blocked-client lifecycle.
- Add one script to run all baseline TLA+ configs and print PASS/FAIL summary.
