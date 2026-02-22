# DeepResearch Instruction (No Repo Access): TLA+ Onboarding + Model-Checking Plan

Date: 2026-02-22  
Target system: Redis-compatible Rust server (owner-thread actor architecture)  
Constraint: **You cannot read repository code or local docs.**  
You must work only from the architecture description below plus external sources.

## 1. Mission

Produce a practical report for introducing **TLA+** into a Redis-like server project that is actively changing concurrency architecture.

Primary goal:

- define a small but meaningful TLA+ modeling scope we can run now
- catch concurrency/correctness bugs early (before implementation drift)
- keep the model maintainable and useful for engineers who are not formal-methods experts

This is not an academic exercise. We need a workflow that engineers can actually use week-to-week.

## 2. System Description (Self-Contained)

Assume this architecture:

1. **Protocol/connection layer**
   - TCP server handles many client connections.
   - RESP commands are parsed per connection.
   - Commands are dispatched by command name + args.

2. **Owner-thread actor model**
   - Keyed operations route to an owner thread (single owner per key space partition).
   - Some deployments use multi-port mode where each listen port maps to one owner runtime.
   - Objective is to avoid global coarse locks on hot paths.

3. **Command behavior classes**
   - Fast path: `GET`/`SET`-like immediate operations.
   - Blocking operations: list/zset blocking pops (`BLPOP`/`BRPOP`/`BLMPOP`/`BZMPOP` etc.) with timeout behavior and wakeup order constraints.
   - Admin/control paths: `CLIENT`, replication control, basic cluster routing behavior (`MOVED` style semantics in cluster mode).

4. **Background and ordering-sensitive paths**
   - Expiration processing (periodic task).
   - Replication stream apply/order constraints.
   - Transaction-like replay/queued execution behavior.

5. **Current concern**
   - We want owner-thread serialization semantics to be clear and verifiable.
   - Blocking wakeup/fairness, timeout behavior, and replication ordering are easy to regress.
   - We need a minimal formal model that can expose invalid interleavings.

## 3. What We Need From You

Given the architecture above, provide an actionable TLA+ adoption package:

1. How to start with TLA+ quickly (tooling + workflow).
2. What exact model boundaries to choose first.
3. Which safety/liveness properties to encode first.
4. How to avoid state explosion while still finding real bugs.
5. How to convert counterexamples into deterministic Rust tests.

## 4. Research Questions

1. What is the best practical split between:
   - TLA+ Toolbox + TLC
   - CLI-first TLC
   - Apalache (if useful for this workload)
2. How should we model owner-thread routing semantics without over-modeling implementation details?
3. How should we model blocking commands and wakeups:
   - queue/fairness assumptions
   - timeout semantics
   - cancellation/unblock behavior
4. What fairness assumptions are appropriate (weak vs strong fairness) for this type of server?
5. How should replication-order properties be modeled (single source stream, apply order, failover/switch)?
6. What are proven patterns to control state explosion in protocol/server models of this shape?
7. Which properties should stay out of TLA+ and remain test/benchmark-only?

## 5. Required Deliverables

Return report in this exact order:

1. Executive summary (max 10 bullets)
2. TLA+ quickstart for engineers (install, run, iterate) on macOS/Linux
3. Minimal model architecture (modules, variables, actions) for this system
4. Property catalog:
   - Safety invariants
   - Liveness properties
   - Fairness assumptions (explicit)
5. Incremental modeling roadmap:
   - M0: tiny key/value core
   - M1: owner-thread routing
   - M2: blocking command semantics
   - M3: replication order
   - M4: optional cluster/failover extension
6. State-space control plan (bounds, symmetry, abstractions, decomposition)
7. Counterexample-to-test workflow:
   - how to extract failing interleaving
   - how to encode deterministic reproduction tests in Rust
8. Risks/anti-patterns:
   - over-modeling
   - wrong fairness assumptions
   - specs that pass but are useless for implementation
9. “Do this week” checklist (max 12 items)

## 6. Quality Bar

- Must be practical and implementation-facing.
- Separate:
  - production-proven advice
  - experimental/advanced recommendations
- Provide explicit assumptions for every major recommendation.
- Prefer simpler modeling choices when bug-finding power is similar.
- Include a recommended initial bounded model size that runs in minutes, not hours.

## 7. Important Constraints

- Do not ask to browse our repository.
- Treat architecture details above as the only system context.
- If a recommendation depends on unknown internals, mark it conditional and provide fallback.
- Keep terminology consistent with Redis-like server behavior and owner-thread actor execution.
