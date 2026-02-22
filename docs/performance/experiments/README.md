# Experiment Reports

- Date: `2026-02-20`
- Base directory: `docs/performance/experiments/2026-02-20/`
- Policy: `docs/performance/EXPERIMENT_REPORTING_POLICY.md`
- Date: `2026-02-21`
- Base directory: `docs/performance/experiments/2026-02-21/`
- Date: `2026-02-23`
- Base directory: `docs/performance/experiments/2026-02-23/`

## Reports

1. `docs/performance/experiments/2026-02-20/11.37-binary-ab-harness-and-failfast/`
2. `docs/performance/experiments/2026-02-20/11.38-owner-routing-no-frame-reparse/`
3. `docs/performance/experiments/2026-02-20/11.39-hotspot-default-shards2/`
4. `docs/performance/experiments/2026-02-20/11.40-packed-owner-routing-rejected/`
5. `docs/performance/experiments/2026-02-20/11.45-stepwise-single-vs-multiport-benchmark/`
6. `docs/performance/experiments/2026-02-20/rxbuf-copy-rewrite-rejected/`
7. `docs/performance/experiments/2026-02-21/12.30-actor-debt-cleanup-regression-check/`
8. `docs/performance/experiments/2026-02-21/12.31-command-catalog-sso/`
9. `docs/performance/experiments/2026-02-23/13.00-evidence-gate-read-drain-and-client-metrics/`
10. `docs/performance/experiments/2026-02-23/13.01-owner-thread-count-sweep/`
11. `docs/performance/experiments/2026-02-23/13.02-docker-linux-median-owner-thread-count/`

## Patterns To Re-check For New Experiments

- One experiment should map to one commit when possible.
  - include before/after commit hashes and keep `diff.patch`.
- Always capture exact benchmark parameters.
  - threads, clients, requests, pipeline, key range, payload size.
- Regressions must be judged from repeated runs.
  - compare medians and preserve raw run tables (`runs.csv`).
- Guard against false green outcomes.
  - verify test/benchmark case counts and fail on server error lines.
- Store artifacts in the experiment directory, not temporary-only locations.
  - at minimum: `README.md`, `diff.patch`, summary artifacts.
