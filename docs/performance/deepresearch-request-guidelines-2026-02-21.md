# DeepResearch Request Guidelines (2026-02-21)

## Purpose

Use ChatGPT DeepResearch at high-leverage points where local experimentation alone is likely to miss design space, prior art, or failure modes.

## When To Request DeepResearch

- Before committing to a non-trivial architecture shift (threading model, consistency model, storage/layout model).
- When introducing concurrency primitives that are hard to validate by tests alone (optimistic reads, RCU/epoch, lock-free reclamation).
- When compatibility semantics may conflict with performance goals (for example transaction/replication/expiration ordering).
- When benchmark results are ambiguous or noisy and we need external methodology references to avoid false conclusions.
- When we need portability guidance from proven systems (Redis/Dragonfly/Linux kernel/JVM/Seastar) rather than ad-hoc implementation guesses.

## When Not To Request

- Straightforward coding tasks with clear acceptance tests.
- Mechanical refactors that do not change invariants.
- Small performance tweaks that can be decided quickly with existing benchmark scripts.

## Request Quality Checklist

- State our current architecture and constraints explicitly (assume report author cannot read repo).
- Ask for actionable outputs: decision matrix, phased plan, risks, rollback criteria, benchmark plan.
- Require citations or source-classification (production-proven vs experimental).
- Define expected deliverables location in repo (report + note digest).

## Repository Workflow

1. Add instruction file in repo (`docs/performance/...INSTRUCTION...md`).
2. Register request in `TODO_AND_STATUS.md` Phase 11B as `REQUESTED_WAITING`.
3. On return, import report under `docs/performance/...deepresearch...md`.
4. Create or update concise actionable notes (`...-notes-...md`).
5. Mark tracker status `RECEIVED` and record iteration log entry.

