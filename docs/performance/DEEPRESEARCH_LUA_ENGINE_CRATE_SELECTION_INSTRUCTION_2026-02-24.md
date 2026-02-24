# DeepResearch Instruction (No Repo Access): Rust Lua Engine Crate Selection for Redis Scripting

## Important Constraints

- Assume you **cannot access this repository**.
- Build your answer only from public primary/official sources.
- Prioritize production reality over novelty (maintenance status, CVE posture, ABI risk, ops burden).

## Project Context (Provided Manually)

We are implementing a Redis-compatible server in Rust.

Current state of scripting-related command surface:

- `EVAL`, `EVAL_RO`, `EVALSHA`, `EVALSHA_RO`, `FCALL`, `FCALL_RO` exist and validate `numkeys`.
- Actual script execution is not implemented yet (currently returns `ERR scripting is disabled in this server`).
- `SCRIPT`/`FUNCTION` currently have only minimal `FLUSH`-style compatibility behavior.

Non-functional requirements:

- We do **not** want to reimplement Lua runtime ourselves.
- We want maintainable, production-grade integration.
- Performance matters, but correctness/security/operability come first for initial rollout.
- We prefer phased delivery with explicit fallback/kill-switch options.

## Research Goals

1. Select the most practical Rust crate/runtime strategy for Redis-style Lua scripting.
2. Identify integration architecture that supports deterministic behavior and safe execution boundaries.
3. Produce a phased rollout plan from “disabled” to “usable in production”.

## Questions To Answer

1. Which Rust options are credible today for embedding Lua semantics needed by Redis scripting?
   - Include at least: `mlua`, `rlua` (if relevant), pure-Rust alternatives, and non-Lua substitution options.
2. Which options are actively maintained and production-credible?
   - Release activity, issue/PR health, platform support, license compatibility.
3. What are the key technical tradeoffs?
   - FFI/C dependency vs pure Rust
   - Lua version coverage (`5.1/5.2/5.3/5.4`, LuaJIT, Luau if applicable)
   - Thread-safety and async interaction model
   - Sandboxing/isolation capability and resource limits
   - Determinism concerns for distributed/server use
4. For Redis compatibility scope, what is minimum viable scripting behavior for first implementation?
   - Script load/cache (`EVAL` vs `EVALSHA`)
   - `KEYS`/`ARGV` mapping
   - Error propagation semantics
   - Script time limits / interruption policy
5. What security model is recommended?
   - Unsafe standard libs to disable
   - File/system/network access controls
   - Timeout/instruction-count/memory ceilings
   - Abuse/failure isolation strategy
6. What integration architecture is best for owner-thread server models?
   - Per-thread VM vs shared VM
   - Script cache ownership/invalidation
   - Replication/AOF implications (execute-result replication vs script text replication)
7. What are the major failure modes and mitigations?
   - VM crash/abort, long-running scripts, memory blow-up, non-deterministic behavior.

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary` (10 bullets max)
2. `Candidate Matrix` table:
   - Candidate
   - Maintenance status
   - License
   - Runtime dependency model
   - Redis-compat feasibility
   - Security/sandbox controls
   - Expected performance envelope
   - Adopt now / pilot / avoid
3. `Recommended Default`
   - One primary choice
   - One fallback choice
   - Why
4. `Integration Blueprint`
   - VM lifecycle model
   - Script cache model
   - Command execution flow (`EVAL*` / `FCALL*`)
   - Error/timeout handling
5. `Phased Delivery Plan`
   - Phase 0: disabled + plumbing
   - Phase 1: `EVAL` minimal
   - Phase 2: `EVALSHA` + cache semantics
   - Phase 3: function API (`FCALL*`) parity
   - Phase 4: hardening/perf
6. `Risk Register`
   - Top risks and explicit mitigation for each
7. `References`
   - Primary sources only (official crate docs/repos, Lua manuals, Redis/Valkey scripting docs)

## Decision Criteria We Will Apply

- Must be actively maintained and license-compatible.
- Must support safe operational controls (timeouts/limits/sandboxing).
- Must fit a server architecture with strict latency constraints.
- Must allow an incremental rollout with clear rollback path.
- “Fast but fragile” is unacceptable for first adoption.
