# 11.455 multidb blocking wait db scope

- Date: 2026-03-11
- Base commit: `cb40c1adc8`
- New commit: worktree for iteration `11.455`
- Scope: DB-scoped blocking wait registration/readiness for no-singledb blocked list/zset/stream paths.

## Why this exists

The functional change is in blocking wake visibility under auxiliary DBs (`SELECT 9` in Redis external no-singledb tests). The primary evidence for correctness is the exact regression set and targeted external probes, not throughput numbers.

## Benchmark note

The host machine was under concurrent user workload during this iteration. The `binary_ab_local.sh` outputs in `artifacts/` are kept only as noisy smoke references and were not used as a release gate for this slice.

- First run: `artifacts/comparison.txt`
- Rerun: `artifacts/comparison-rerun.txt`

Because the two runs disagree materially, treat them as non-actionable noise.

## Validation used for the iteration

- TLA+:
  - `formal/tla/specs/BlockingCountVisibility_bug.cfg` expected counterexample
  - `formal/tla/specs/BlockingCountVisibility_fixed.cfg` pass
- Rust:
  - `linked_blmove_chain_is_observable_without_intermediate_residue`
  - `xreadgroup_claim_blocking_wait_detection_supports_claim_and_multiple_streams`
  - `make test-server`
- External:
  - `Linked LMOVEs` single-case probe
  - isolated `unit/type/list` no-singledb rerun
