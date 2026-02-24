# 2026-02-24 Experiment Set

## Included Experiments

1. `13.11-argslice-boundary-dispatch-ab-accepted`

## Policy

- See `docs/performance/EXPERIMENT_REPORTING_POLICY.md`.
- This date set follows commit-tracked A/B validation with preserved raw artifacts.

## Patterns Observed In This Set

- Boundary-only unsafe-to-safe refactors can be net-positive on 1-thread hot paths while remaining near-neutral on higher-concurrency shapes.
- Adoption decisions should use at least two representative shapes instead of one benchmark profile.
- Median-of-N + strict integrity checks remains mandatory even when the change looks mechanically simple.
