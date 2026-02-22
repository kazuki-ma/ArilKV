# 2026-02-21 Experiment Set

## Included Experiments

1. `12.30-actor-debt-cleanup-regression-check`
2. `12.31-command-catalog-sso`

## Policy

- See `docs/performance/EXPERIMENT_REPORTING_POLICY.md`.
- This date set follows the commit-tracked workflow (before/after hashes + diff artifact).

## Patterns Observed In This Set

- Refactors affecting hot paths need explicit no-regression evidence.
  - measure throughput and p99 before proceeding with feature additions.
- Command metadata centralization should be validated as near-neutral.
  - keep hot-path dispatch exceptions explicit and benchmarked.
- Experiment notes are most useful when they include copy-pastable commands.
  - preserve full command lines and output artifact paths.
