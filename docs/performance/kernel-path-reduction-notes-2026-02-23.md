# Kernel-Path Reduction Notes (2026-02-23)

## Source

- DeepResearch report (imported):
  - `docs/performance/kernel-path-reduction-deepresearch-2026-02-23.md`
- Instruction used:
  - `docs/performance/DEEPRESEARCH_KERNEL_PATH_REDUCTION_INSTRUCTION_2026-02-21.md`

## Key Findings (Actionable)

- For tiny GET/SET, kernel-bound cost (`recv/send` + wakeups/scheduling) dominating profiles is expected.
- Priority should be reducing boundary/syscall count before major architecture changes:
  - aggregate writes (`writev`/fewer small sends),
  - read-drain loops (consume multiple commands per readable event),
  - reduce avoidable wakeups.
- Keep `TCP_NODELAY` decisions tied to write-aggregation behavior; small fragmented writes can still hurt throughput/CPU.
- `MSG_ZEROCOPY` is usually a poor fit for tiny responses; treat as deferred/not-default.
- `io_uring` can help when used with batching/completion model changes (not just epoll-like substitution).
- `SQPOLL`, busy polling, and similar options are candidate later-phase optimizations with explicit CPU/ops tradeoffs.
- Kernel-bypass approaches (AF_XDP/DPDK) are high complexity/risk; treat as last-stage options.

## Recommended Phase Order

1. Low-risk: syscall/wakeup reduction in current model (write/read aggregation, socket/tuning checks).
2. Medium-risk: selective Linux tuning experiments (busy poll, affinity-style options) with strict A/B evidence.
3. Higher-risk: `io_uring` backend trial behind feature flag with fallback path.
4. Last resort: kernel-bypass track only if prior phases are insufficient.

## Candidate Work Items

- Add explicit benchmark counters for syscall-per-request and wakeup-related events.
- Add write-aggregation checks in hot response paths (avoid split tiny writes).
- Add bounded experiment plan for busy-poll options in dedicated environment.
- Define an optional `io_uring` trial plan with clear rollback criteria.
