# Review Triage -- 2026-03-04 (`.review_by_claude/`)

This document classifies major findings from `.review_by_claude/*.md` into:

- `ACTION`: implement/planned backlog work
- `NO_ACTION`: intentionally kept by design
- `HOLD`: deferred (low ROI now, or needs stronger evidence first)

Related ADRs:

- ADR-0002 through ADR-0007 under `docs/adr/`

## 1. ACTION Backlog

| ID | Finding | Source | Classification | Why | Next Step |
|---|---|---|---|---|---|
| `RT-001` | Object commands do full ser/deser per operation (`O(N)` updates) | `00_EXECUTIVE_SUMMARY`, `01_PERFORMANCE_CRITICAL`, `04_DATA_STRUCTURES`, `13_CSHARP_COMPARISON` | `ACTION` | Top systemic cost and broad impact on hash/list/set/zset/stream command families. | Baseline/design `11.396`, hash single-field slice `11.397`, hash multi-field slice `11.398`, and `HMSET`/`HGETDEL` slice `11.399` are complete. `11.399` kept touched paths non-regressing and maintained prior hash-path gains in guard runs. Next: `11.400` (`HSETEX`/`HGETEX` mutation-heavy branch fast-path evaluation + revalidation). |
| `RT-002` | GET/SET and replication path still allocation-heavy | `00_EXECUTIVE_SUMMARY`, `01_PERFORMANCE_CRITICAL`, `05_MEMORY_ALLOCATION`, `12_GET_SET_PATH_TRACE` | `ACTION` | Hot path issue with direct throughput/latency impact. | Iterations `11.390` (RESP integer/length formatting), `11.391` (replication frame rewrite/emit allocation cuts), `11.392` (replication-heavy validation), `11.393` (LMPOP parser-side allocation cut), `11.394` (controlled-noise revalidation), and `11.395` (path-aligned LMPOP replication gate) are complete. `11.395` stayed within same-binary noise bounds, so this `RT-002` slice is currently accepted while active optimization focus remains `RT-001` (`11.399` -> `11.400`). |
| `RT-003` | SCAN family uses full snapshot (`O(N)` per call) | `08_COMMAND_LIFECYCLE`, `09_CORRECTNESS`, `11_MISSING_FEATURES`, `13_CSHARP_COMPARISON` | `ACTION` | High compatibility/perf gap for keyspace iteration behavior. | Introduce incremental cursor strategy and validate with compat + complexity-focused benchmark. |
| `RT-004` | Blocking commands rely on polling loops (1ms/5ms style waits) | `08_COMMAND_LIFECYCLE`, `09_CORRECTNESS`, `13_CSHARP_COMPARISON` | `ACTION` | Correctness is maintained, but CPU efficiency and wakeup behavior can be improved. | Evaluate event-driven wake path preserving existing wait-order invariants and TLA+ assumptions. |
| `RT-005` | Eviction policy (`maxmemory-policy`) not implemented | `11_MISSING_FEATURES`, `13_CSHARP_COMPARISON` | `ACTION` | Critical production feature gap. | Add phased eviction implementation with explicit policy coverage and compatibility tests. |
| `RT-006` | ACL/Auth are still stub-level for production parity | `11_MISSING_FEATURES`, `13_CSHARP_COMPARISON` | `ACTION` | Critical security/production gap. | Define minimal secure ACL phase (user model + command gating) and stage rollout with tests. |
| `RT-007` | RDB snapshot support missing | `11_MISSING_FEATURES` | `ACTION` | High-priority durability/ops feature gap. | Scope snapshot format + lifecycle integration plan and compatibility test matrix. |
| `RT-008` | `BGREWRITEAOF` missing | `11_MISSING_FEATURES` | `ACTION` | High-priority operational durability gap. | Implement background rewrite path with safety checks and smoke/recovery validation. |

## 2. NO_ACTION (By-Design)

| ID | Finding | Source | Classification | Decision Record |
|---|---|---|---|---|
| `RT-101` | Owner-thread shard serialization + ordered lock discipline limits concurrency | `00_EXECUTIVE_SUMMARY`, `06_CONCURRENCY` | `NO_ACTION` | ADR-0002 |
| `RT-102` | String values keep inline expiration prefix overhead | `04_DATA_STRUCTURES` | `NO_ACTION` | ADR-0003 |
| `RT-103` | `string_expiration_counts` keeps relaxed hint semantics | `06_CONCURRENCY` | `NO_ACTION` | ADR-0004 |

## 3. HOLD (Deferred / Re-evaluate with Evidence)

| ID | Finding | Source | Classification | Decision Record | Re-open Condition |
|---|---|---|---|---|---|
| `RT-201` | Replace non-hot dispatch lookup with PHF/HashMap | `01_PERFORMANCE_CRITICAL`, `13_CSHARP_COMPARISON` | `HOLD` | ADR-0005 | Reopen if profiler shows non-hot dispatch as sustained hotspot. |
| `RT-202` | Reuse/pool Lua VM instead of per-call `Lua::new()` | `11_MISSING_FEATURES`, `13_CSHARP_COMPARISON` | `HOLD` | ADR-0006 | Reopen if script-heavy profiling makes VM creation dominant. |
| `RT-203` | Large readability-only module splits (`connection_handler.rs`, `request_lifecycle.rs`) | `10_READABILITY` | `HOLD` | ADR-0007 | Reopen with incident-driven or feature-scoped extraction plan. |
| `RT-204` | Global refactor from BTree* to hash-based collections before object-path redesign | `04_DATA_STRUCTURES`, `13_CSHARP_COMPARISON` | `HOLD` | Linked to `RT-001` | Re-evaluate after object update model redesign, not before. |
| `RT-205` | Broad atomic-ordering micro-tuning without hotspot/correctness evidence | `06_CONCURRENCY` | `HOLD` | Linked to ADR-0004 | Reopen only when tied to measured bottleneck or proven correctness issue. |

## 4. Notes

- This triage intentionally tracks major/systemic findings first.
- Individual low-level style observations remain in source review documents and
  are not all elevated into immediate backlog items.
- Any `HOLD` item can move to `ACTION` when revisit triggers are satisfied.
