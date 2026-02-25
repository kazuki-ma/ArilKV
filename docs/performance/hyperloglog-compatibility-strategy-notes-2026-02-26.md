# HyperLogLog Compatibility Strategy Notes (2026-02-26)

## Source

- DeepResearch report (imported):
  - `docs/performance/hyperloglog-compatibility-strategy-deepresearch-2026-02-26.md`
- Instruction used:
  - `docs/performance/DEEPRESEARCH_HLL_COMPAT_STRATEGY_INSTRUCTION_2026-02-25.md`

## Key Findings (Actionable)

- For Redis/Valkey parity, prioritize a native Redis-compatible HLL representation, not a generic HLL crate.
  - Keep String payload compatibility (`GET`/`SET` round-trip on HLL payloads).
  - Implement sparse/dense dual encoding and 16-byte header semantics (including cardinality-cache invalidation behavior).
- Treat corruption validation as a first-class requirement.
  - Add strict payload parser bounds checks for malformed sparse/dense payloads.
  - Ensure malformed payloads fail safely (no panic/abort/OOB behavior).
- Keep `PFCOUNT` side effects explicit in design.
  - Cardinality cache updates can make `PFCOUNT` write-like for replication/persistence decisions.
- Avoid adopting current external Rust HLL crates as the primary compatibility path.
  - They can be useful for experimentation, but they do not provide Redis byte-level format compatibility.

## Migration Guidance

- Migrate from `garnet-pf-v1` placeholder format with write-on-touch conversion during `PF*` operations.
- Add operational guardrails for migration:
  - conversion metrics,
  - conversion failure counters,
  - kill-switch for conversion rollback behavior.

## Follow-up TODO Focus (11.168)

- Implement Redis-compatible HLL payload representation and parser/validator.
- Rework `PFADD`/`PFCOUNT`/`PFMERGE` to operate on compatible payloads.
- Implement `PFDEBUG` compatibility subcommands required by external tests.
- Add fuzz + malformed-payload regression coverage for HLL parser safety.
