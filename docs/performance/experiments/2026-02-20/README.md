# 2026-02-20 Experiment Set

## Included Experiments

1. `11.37-binary-ab-harness-and-failfast`
2. `11.38-owner-routing-no-frame-reparse`
3. `11.39-hotspot-default-shards2`
4. `11.40-packed-owner-routing-rejected`
5. `11.45-stepwise-single-vs-multiport-benchmark`
6. `rxbuf-copy-rewrite-rejected`

## Policy

- See `docs/performance/EXPERIMENT_REPORTING_POLICY.md`.
- This date set includes both compliant (commit-tracked) and historical gap reports.

## Patterns Observed In This Set

- Historical working-tree-only experiments made traceability difficult.
  - reason: missing before/after commit hashes and revert hashes.
- Harness integrity checks were necessary to avoid false pass.
  - benchmark scripts must fail on server-side error output.
- Local hotspot defaults can distort interpretation.
  - keep defaults aligned to current runtime policy (for example shard count).
