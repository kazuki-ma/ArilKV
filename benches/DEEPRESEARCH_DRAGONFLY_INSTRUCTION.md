# DeepResearch Instruction: Dragonfly Performance Gap Analysis

## Goal
Identify evidence-backed reasons Dragonfly outperforms `garnet-rs` under cache workloads, then map them to concrete, prioritized optimizations for this repository.

## Scope
- Workload family: Redis-compatible GET/SET hot paths.
- Baseline benchmark harness:
  - `benches/cache_benchmarks_garnet_wrapper.sh`
  - `benches/cache_benchmarks_dragonfly_docker_wrapper.sh`
  - `tidwall/cache-benchmarks` (`./bench ...`)
- Treat current local benchmarking integration as known context.

## Known Context (Do Not Re-discover)
- `cache-benchmarks` integration already exists for `garnet-rs`.
- Dragonfly can be run on macOS through Docker wrapper for comparison.
- Recent storage failure root cause in variable-size writes was fixed in `garnet-rs`.
- Current need is *not* basic setup; it is gap analysis and optimization prioritization.

## Unknowns To Resolve
1. What are the top architectural causes of Dragonfly throughput/latency advantage?
2. How much of the gap is network/RESP parsing vs storage path vs allocator behavior?
3. Which Dragonfly techniques are transferable to `garnet-rs` with reasonable risk?
4. What benchmark settings are required for fair apples-to-apples comparison?
5. Which optimizations should be done first for maximum ROI on this codebase?

## Research Requirements
- Prefer primary sources:
  - Dragonfly official docs/design notes/source code/PR discussions.
  - Redis/Valkey docs where relevant for baseline behavior.
  - Linux perf and flamegraph methodology references.
- Every claim must include source links.
- Separate evidence from inference:
  - Tag each conclusion as `EVIDENCE` or `INFERENCE`.
- Avoid marketing claims unless supported by methodology and measurements.

## Required Deliverables
1. **Evidence Table**
   - columns: `claim`, `evidence`, `source`, `confidence (high/med/low)`.
2. **Bottleneck Model**
   - estimated contribution of `network`, `parser`, `storage`, `allocator`, `sync`.
3. **Transferability Matrix**
   - columns: `technique`, `expected impact`, `implementation complexity`, `risk`, `fit for garnet-rs`.
4. **Prioritized Backlog**
   - 10 items max, sorted by impact/effort.
   - each item must include measurable success criteria (`ops/sec`, `p99`, CPU metrics).
5. **Experiment Plan**
   - exact commands for Linux-based `perf record` + flamegraph for both systems.
   - at least 3-run median policy and fairness checklist.

## Output Format
- Start with a one-page executive summary.
- Then provide the 5 deliverables above.
- End with an explicit “first 3 actions to execute this week”.

## Copy/Paste Prompt For ChatGPT DeepResearch
Use the following prompt as-is:

```text
You are performing a deep technical performance investigation for a Redis-compatible Rust server (`garnet-rs`) compared with Dragonfly.

Goal:
- Identify evidence-backed reasons Dragonfly is faster on GET/SET cache workloads.
- Convert findings into a prioritized, implementable optimization roadmap for `garnet-rs`.

Known context:
- Benchmark harness exists via `tidwall/cache-benchmarks`.
- `garnet-rs` wrapper exists: `benches/cache_benchmarks_garnet_wrapper.sh`.
- Dragonfly wrapper exists for macOS Docker: `benches/cache_benchmarks_dragonfly_docker_wrapper.sh`.
- Setup is not the task. Root-cause performance gap analysis is the task.

Unknowns to resolve:
1) top architectural reasons for Dragonfly advantage,
2) breakdown of gap by network/parser/storage/allocator/synchronization,
3) what is transferable to Rust `garnet-rs`,
4) fair-comparison benchmark settings,
5) highest-ROI optimization order.

Requirements:
- Use primary sources whenever possible (official docs, code, design docs, PR discussions, talks).
- Provide source links for every key claim.
- Mark each conclusion as EVIDENCE or INFERENCE.
- Avoid unsupported marketing claims.

Deliverables:
1) Evidence table (claim/evidence/source/confidence),
2) Bottleneck model (network/parser/storage/allocator/sync),
3) Transferability matrix (impact/complexity/risk/fit),
4) Prioritized backlog (max 10 items, each with measurable success criteria),
5) Linux experiment plan with concrete commands (`perf stat`, `perf record`, flamegraph), fairness checklist, and 3-run median policy.

Output:
- Start with a short executive summary.
- Then provide the 5 deliverables.
- End with the first 3 actions to execute this week.
```
