# RDMA / Special-Hardware Replication Note (Deferred, 2026-03-16)

Scope: whether RDMA or similar special hardware should materially change the
current Garnet replication/full-sync design direction.

Related local notes:

- `docs/performance/replication-fullsync-cutover-notes-2026-03-16.md`
- `docs/performance/replication-fullsync-idea-hunt-2026-03-16.md`
- `docs/performance/design-tiered-local-wal-and-object-store-replication-2026-03-16.md`

## Decision

Forget RDMA for the current design cycle.

This is an explicit defer decision, not a claim that RDMA is useless forever.

## Why We Are Deferring It

RDMA mostly changes transport and data-movement cost.

It can help with:

- kernel/TCP bypass
- copy reduction
- remote CPU involvement on the replica/backup side
- lower-overhead log shipping or overlap-buffer transport

But it does **not** remove the core design work we still need anyway:

- correct `snapshot + suffix` cutover
- explicit replay boundaries
- replication-unit classification
- coordination for still-unsplit multi-partition/global units
- retention / replay / manifest design
- durability semantics

So RDMA is not a substitute for the architecture we are still defining.

## Why It Is A Bad Fit Right Now

For the current phase, RDMA would likely distract more than it helps because:

- it optimizes a path whose final protocol/record model is not settled yet
- it adds deployment constraints and operational complexity
- it can bias design decisions toward hardware-specific shortcuts before the
  correctness model is stable
- the current open questions are still mostly about protocol boundaries and
  replay structure, not raw transport cost

Put differently:

- if we do not yet know the correct replay/session structure, accelerating the
  wrong structure is not a good trade

## Current Working Rule

Assume ordinary network + ordinary local storage.

Design the replication/full-sync path so it is correct and operationally sound
without requiring:

- RDMA
- remote persistent memory
- SmartNIC/DPU offload
- any other special fabric guarantee

If the design is good, special hardware can be evaluated later as an optional
transport optimization layer.

## Revisit Conditions

Reopen RDMA only if all of the following become true:

1. the replay/session/boundary model is already stable
2. profiling shows transport or kernel-path overhead is the dominant remaining
   bottleneck
3. the intended deployment environment can actually guarantee RDMA-capable
   hardware and operations

Until then, RDMA stays out of scope.

## Bottom Line

RDMA may eventually speed up parts of replication.

It does not change the core correctness problem, and it is not the right lever
for the current phase.

So the repository-level conclusion for now is:

- do not design around RDMA
- do not keep it in active solution search
- revisit only after the ordinary-network design is correct and measured
