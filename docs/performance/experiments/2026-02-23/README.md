# 2026-02-23 Experiment Set

## Included Experiments

1. `13.00-evidence-gate-read-drain-and-client-metrics`
2. `13.01-owner-thread-count-sweep`
3. `13.02-docker-linux-median-owner-thread-count`
4. `13.03-single-port-current-thread-runtime-rejected`
5. `13.04-owner-result-channel-mode-rejected`
6. `13.05-listener-owner-colocation-accepted`
7. `13.06-linux-hotspot-refresh-owner-pinned`
8. `13.07-owner-pinned-dragonfly-comparison-milestone` (**MILESTONE**)

## Policy

- See `docs/performance/EXPERIMENT_REPORTING_POLICY.md`.
- This date set follows evidence-first optimization acceptance.

## Patterns Observed In This Set

- Candidate optimizations must be measured before acceptance.
- If isolated gain is unclear, reject the change and keep only measured wins.
- Avoid global default flips when load-shape results are mixed.
- Treat runtime/scheduler model changes as high-risk; accept only with clear win.
- When a design-intent mismatch is hotspot-dominant, validate and prioritize structural fixes first.
- Use median-of-N comparisons before declaring milestone wins against external systems.
