# Lua Engine Crate Selection Notes (2026-02-24)

## Source

- DeepResearch report (imported):
  - `docs/performance/lua-engine-crate-selection-deepresearch-2026-02-24.md`
- Instruction used:
  - `docs/performance/DEEPRESEARCH_LUA_ENGINE_CRATE_SELECTION_INSTRUCTION_2026-02-24.md`

## Key Findings (Actionable)

- Default runtime choice should be `mlua` with `Lua 5.1` and `vendored` build for Redis-compatible behavior and deployment stability.
- `rlua` should not be newly adopted (archived/deprecated path), and `piccolo` should stay research-only until maturity rises.
- Security baseline must be explicit from day one: `require`/dynamic module loading disabled, strict sandboxed library surface, and feature-gated scripting kill switch.
- Rollout should be phased: plumbing first, then read-only scripting (`EVAL_RO`/`EVALSHA_RO`), then cache semantics (`SCRIPT *` + `NOSCRIPT`), then function API (`FUNCTION`/`FCALL*`), then hardening.
- Execution model should align with current owner-thread architecture: one Lua VM per owner thread to keep execution/ownership rules simple.
- Replication strategy should target command-effects replication for scripting writes (not verbatim script-text replication).

## Assessment

- Report quality is high and directly usable in this repository: it includes concrete crate selection, security constraints, and a phased compatibility plan.
- Highest-value guidance is consistent with current codebase direction: owner-thread execution, explicit compatibility gating, and opt-in rollout for risky surfaces.

## Follow-up TODO (Mapped to Tracker)

- `11.133`: Add `mlua` plumbing behind feature flag and keep scripting disabled by default (`ERR scripting is disabled` path unchanged unless enabled).
- `11.134`: Introduce script cache fundamentals (SHA1 map, metrics, size limit, volatile semantics) without enabling arbitrary write scripts.
- `11.135`: Implement minimal read-only scripting path (`EVAL_RO`, `EVALSHA_RO`) with `KEYS`/`ARGV` and `redis.call`/`redis.pcall` read-only enforcement.
- `11.136`: Implement `SCRIPT LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` and correct `NOSCRIPT` behavior for `EVALSHA*`.
- `11.137`: Implement `FUNCTION LOAD` + `FCALL_RO` minimal compatibility path and key-argument validation rules.
- `11.138`: Add hardening and observability gates (timeout/memory limits, script-cache eviction policy, replication/AOF compatibility tests).
