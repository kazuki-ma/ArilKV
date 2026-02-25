# DeepResearch Instruction (No Repo Access): HyperLogLog Compatibility Strategy and Library Decision

## Important Constraints

- Assume you **cannot access this repository**.
- Build your answer from public primary/official sources.
- Prioritize production-credible guidance over novelty.

## Project Context (Provided Manually)

We are building a Redis-compatible server in Rust.

Current HyperLogLog (`PF*`) state:

- Commands exist: `PFADD`, `PFCOUNT`, `PFMERGE`, `PFDEBUG`, `PFSELFTEST`.
- Current implementation is a **placeholder**, not a Redis-compatible HLL engine:
  - Stores exact members in a set-backed payload with custom prefix (`garnet-pf-v1`).
  - `PFCOUNT` currently computes exact union cardinality over stored sets.
  - `PFDEBUG ENCODING` returns static `sparse`; `PFDEBUG TODENSE` is effectively a no-op.
- External compatibility currently shows `tests/unit/hyperloglog.tcl` failures (`14` cases).

Observed failure themes:

- sparse/dense transition semantics,
- corruption detection/robustness behavior,
- `PFDEBUG GETREG`-style introspection behavior,
- merge/count/cache invalidation edge semantics.

## Research Goals

1. Decide whether to:
   - adopt an external Rust HLL crate,
   - implement/port a Redis/Valkey-compatible internal HLL representation,
   - or use a hybrid approach.
2. Produce a practical plan to reach Redis/Valkey behavior parity for `PF*`.
3. Minimize long-term maintenance and compatibility risk.

## Questions To Answer

1. What credible Rust options exist today for HyperLogLog implementations suitable for server-side use?
   - Include candidate crates (if any), maintenance activity, license, and ecosystem maturity.
2. Can existing crates realistically satisfy Redis/Valkey compatibility expectations?
   - Especially sparse/dense encoding behavior, merge/count semantics, and corruption handling paths.
3. If crate adoption is not sufficient, what is the best implementation strategy?
   - Reimplement Redis/Valkey-compatible encoding semantics from spec/source behavior,
   - versus using a crate for registers while implementing compatibility translation layer.
4. What are the key risks and tradeoffs for each option?
   - Correctness/compatibility risk,
   - performance and memory profile,
   - operational safety under malformed payloads,
   - maintenance burden and future drift.
5. What migration strategy should be used from current placeholder data format?
   - Backward compatibility expectations,
   - online migration vs write-on-touch conversion,
   - rollback/kill-switch strategy.
6. What minimum test matrix should gate acceptance?
   - Unit invariants,
   - Redis/Valkey compatibility tests,
   - malformed/corrupted payload resilience,
   - deterministic before/after verification criteria.

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary` (10 bullets max)
2. `Candidate Matrix` table:
   - Candidate/Approach
   - Maintenance status
   - License
   - Compatibility feasibility (Redis/Valkey `PF*`)
   - Performance/memory expectations
   - Operational risk
   - Adopt now / pilot / avoid
3. `Recommended Direction`
   - Primary recommendation
   - Fallback recommendation
   - Why
4. `Implementation Blueprint`
   - Data representation strategy
   - Command semantics mapping (`PFADD`/`PFCOUNT`/`PFMERGE`/`PFDEBUG`)
   - Corruption handling and validation model
5. `Migration Plan`
   - Placeholder-format transition steps
   - Backward-compat policy
   - Rollback strategy
6. `Acceptance Test Plan`
   - Must-pass compatibility and robustness checks
   - Performance guardrails
7. `References`
   - Primary sources only (official crate docs/repos, Redis/Valkey docs/source)

## Decision Criteria We Will Apply

- Redis/Valkey behavior parity is prioritized over shortest implementation time.
- Security/robustness under malformed inputs is non-negotiable.
- Any external dependency must be actively maintained and license-compatible.
- Adoption must allow incremental rollout with clear rollback path.
