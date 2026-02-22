# 2026-02-23 Experiment Set

## Included Experiments

1. `13.00-evidence-gate-read-drain-and-client-metrics`
2. `13.01-owner-thread-count-sweep`
3. `13.02-docker-linux-median-owner-thread-count`

## Policy

- See `docs/performance/EXPERIMENT_REPORTING_POLICY.md`.
- This date set follows evidence-first optimization acceptance.

## Patterns Observed In This Set

- Candidate optimizations must be measured before acceptance.
- If isolated gain is unclear, reject the change and keep only measured wins.
- Avoid global default flips when load-shape results are mixed.
