# Replication Full-Sync Idea Hunt (2026-03-16)

This note captures the first manual "idea hunt" pass for Redis/Valkey
compatible full-sync replication before `DR-008`, `DR-009`, and `DR-010`
return.

The goal is not to compare implementations mechanically. The goal is to find
mechanisms that materially change the design space for Garnet.

Related local discussion snapshot:

- `docs/performance/replication-fullsync-cutover-notes-2026-03-16.md`

## Current Garnet Constraint Anchor

The starting constraints are unchanged:

- downstream export is still one externally visible stream with one global
  `master_repl_offset`
- real downstream Redis replicas currently receive an empty Redis RDB payload,
  not a real non-empty exported snapshot
- upstream `FULLRESYNC` still discards the incoming RDB payload instead of
  ingesting it into local state
- current downstream handoff still has the dangerous order
  `build snapshot -> subscribe -> stream live`

So the immediate challenge is still the same: how to construct a correct
`snapshot + suffix` splice without paying Redis-style `fork()` + Copy-on-Write
costs.

## Highest-Value Ideas So Far

### 1. Replica-Owned Overlap Buffering, Not Primary-Side CoW

Most relevant sources:

- Valkey 8.0 RC1 blog:
  `https://valkey.io/blog/valkey-8-0-0-rc1/`
- Valkey dual-channel replication slide deck:
  `https://valkey.io/events/keyspace-beijing-2025/slides/Valkey-Dual-Channel-Replication.pdf`
- Redis 8.0-M03 blog:
  `https://redis.io/blog/redis-8-0-m03-generally-available/`

What changed in my view:

- dual-channel replication is no longer a speculative idea; Redis and Valkey
  have now both converged on the same family of solution
- the important mechanism is not merely "two sockets"
- the important mechanism is that the replica owns the transient overlap buffer
  while the snapshot is in flight, so the primary does not need to hold an
  ever-growing CoW penalty or giant one-off buffer

Why this matters for Garnet:

- the core insight survives even when the wire protocol falls back to legacy
  single-channel Redis compatibility
- Garnet can still adopt the same internal architecture:
  - establish a replay boundary first
  - begin capturing suffix traffic immediately
  - materialize snapshot in parallel
  - hand the suffix to the replica after the boundary
- for capable peers, that can become true overlapped transfer
- for legacy peers, it still gives the right architecture for a serialized
  `snapshot then suffix` export

This is the strongest current signal that "avoid `fork()`/CoW" does not imply
"accept a global stop-the-world snapshot".

### 2. Cutover Should Be A Replay Boundary, Not A Magical Global Epoch

Most relevant sources:

- Valkey dual-channel replication slide deck:
  `https://valkey.io/events/keyspace-beijing-2025/slides/Valkey-Dual-Channel-Replication.pdf`
- CPR:
  `https://www.microsoft.com/en-us/research/publication/concurrent-prefix-recovery-performing-cpr-on-a-database/`
- DPR:
  `https://www.microsoft.com/en-us/research/publication/asynchronous-prefix-recoverability-for-fast-distributed-stores/`

The useful abstraction is:

- identify exactly what prefix of history the snapshot represents
- identify exactly what suffix must be replayed after that prefix

That sounds obvious, but it shifts the design away from "find one perfect
global epoch" and toward "make the replay boundary explicit and machine-checkable".

Why this matters for Garnet:

- if export remains externally one logical dataset, the external contract still
  wants a crisp replay boundary
- but internally, the snapshot boundary does not have to be represented as one
  global monolith
- it can instead be encoded as:
  - one explicit coordination-lane boundary
  - plus one boundary per partition-local lane

That aligns much better with the current discussion around replication units.
It also gives a cleaner target for TLA+:

- `NoGap`
- `NoDup`
- `SnapshotThenSuffix`
- `UnsplitUnit`

CPR/DPR are especially useful because they frame correctness in terms of
prefixes, not just timestamps.

### 3. Replicate Ahead, Apply Later

Most relevant sources:

- Rosé:
  `https://vldb.org/cidrdb/2026/rose-flexible-replication-with-strong-semantics-for-partitioned-databases.html`
  and PDF
  `https://www.vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf`
- Dragonfly "Scaling Dragonfly to the Sky":
  `https://www.dragonflydb.io/blog/scaling-dragonfly-to-the-sky`

This is the most interesting design idea from the literature pass.

Rosé's key move is to separate:

- replication of writes to the backup
- application of those writes into the backup's visible key-value state

That is much closer to our discussion than a generic "backup lag" story.

Why this matters for Garnet:

- it suggests that per-partition snapshot skew is often acceptable internally
- what needs coordination is the externally visible apply frontier, not
  necessarily every internal capture step
- this is the cleanest bridge I have seen between:
  - "partition-local replication units can move independently"
  - and
  - "the externally visible dataset still needs a legal history"

In practical Garnet terms, this suggests a design where:

- partition-local lanes capture and replay eagerly
- an explicit coordination lane carries the still-unsplit units
- the session switches to true live mode only when every required lane has
  crossed the session boundary

This is a much stronger formulation than "global epoch or bust".

### 4. Backpressure Should Be Lane-Local When Possible

Most relevant sources:

- Rosé:
  `https://www.vldb.org/cidrdb/papers/2026/p8-zarkadas.pdf`
- Valkey dual-channel replication slide deck:
  `https://valkey.io/events/keyspace-beijing-2025/slides/Valkey-Dual-Channel-Replication.pdf`

Rosé explicitly argues for push-based replication with lag tracking and
backpressure targeted only at the overloaded partition.

Why this matters for Garnet:

- owner-thread architecture is a much better fit for lane-local pressure than
  for a coarse global stall
- if one shard's full-sync session or replay path falls behind, the default
  design should be "slow that lane and the coordination lane that depends on
  it", not "globally throttle everything"

This is not the first design step, but it is a strong shaping constraint for
whatever backlog/session-buffer design we choose.

### 5. Keep The Redundant/Export Path Separate From The Hot Store Path

Most relevant source:

- DEPART:
  `https://www.usenix.org/system/files/fast22-zhang-qiang.pdf`

DEPART is not a full-sync paper, but its core idea is still relevant:

- primary copies and redundant copies should not automatically pay the same
  storage/indexing costs

Why this matters for Garnet:

- full-sync/export should probably not be modeled as "serialize directly out of
  the hot in-memory store and whatever broadcast channel already exists"
- a dedicated replay/export structure can legitimately be optimized for:
  - suffix capture
  - replay indexing
  - eventual durability / `WAIT` / `WAITAOF` integration
  - bounded session fan-out

This is a medium-confidence idea, not the leading architecture. But it is a
good warning against overloading the current broadcast-only replication path
with every future requirement.

## Emerging Garnet-Specific Direction

The best current synthesis is not "copy Valkey dual-channel as-is".

It is:

1. classify replication units aggressively
2. keep partition-local units partition-local
3. isolate the remaining unsplit units into a small explicit coordination lane
4. make full-sync correctness about replay boundaries and visible apply
   frontiers
5. treat overlap buffering as a first-class session concern, ideally owned by
   the replica/session rather than by a `fork()` snapshot on the primary

That suggests this provisional design shape:

1. Introduce a real full-sync session object.
2. Start suffix capture before snapshot materialization.
3. Record explicit per-lane boundaries for the session.
4. Materialize partition-local snapshot slices in parallel.
5. Replay suffix lanes after those boundaries.
6. Gate the externally visible live switch on a session-wide frontier.

This direction keeps the good part of fine-grained cutover without pretending
that every command family is already safely decomposed.

## What Looks Less Attractive After The Hunt

- A pure "one giant global cutover epoch for everything" design now looks more
  like a fallback than the best architecture.
- A design that depends on Redis-style `fork()` + CoW for correctness also
  looks weak for Garnet's owner-thread direction.
- A design that keeps all replication units at top-level command granularity
  looks increasingly incompatible with fine-grained export correctness.

## Immediate Follow-Up While DR Is Still Pending

- inspect Redis 8 / Valkey capability negotiation details more closely and
  decide what can be used directly vs only copied as an internal mechanism
- classify today's unsplit replication units in `garnet-rs` with a concrete
  inventory
- sketch the smallest viable session buffer API:
  - append
  - boundary capture
  - per-session replay cursor
  - lane completion / apply frontier tracking
- start a small TLA+ model around replay boundaries and coordination-lane
  gating instead of around a single global epoch

## Current Bottom Line

The best new idea from this pass is not simply "dual-channel replication".

It is the combination of:

- explicit replay boundaries
- replica/session-owned overlap buffering
- replicate-ahead / coordinated-apply-later
- and a small explicit coordination lane for unsplit units

That combination looks materially better than either:

- global epoch absolutism
- or
- naive per-shard cutover with no explicit coordination model
