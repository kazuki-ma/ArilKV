# Replication Cutover Formal-Methods Notes (2026-03-16)

Source report:

- `docs/performance/replication-cutover-formal-methods-and-literature-deepresearch-2026-03-16.md`

## Core Correctness Model To Adopt

- Model full-sync cutover as **prefix equivalence** over one external logical
  log:
  - snapshot `S(c)` represents log prefix `<= c`
  - suffix replay represents log `> c`
- This is the cleanest shared model across:
  - Redis/Valkey full sync
  - Raft snapshot install
  - WAL/base-backup recovery
  - modern CDC snapshot+log systems

## Practical Judgment On Shards

- The design statement
  "one global logical cutover, shards may materialize later if pinned to that
  cutover"
  is **sound only if** each shard can expose a read view pinned to that same
  logical cutover.
- Without that pinned read view, wall-clock skew becomes an inconsistent cut and
  can produce snapshot/suffix duplication or omission.

## TLA+ Blueprint To Use

- Minimal variables:
  - `CommittedLog`
  - `Cutover`
  - `Backlog`
  - `ReplicaStore`
  - `ReplicaApplied`
  - `Session/ReplId`
  - shard-local apply progress
  - in-flight cross-shard command state
- Minimal actions:
  - `StartFullSync`
  - `SendSnapshot`
  - `SendSuffixEntry`
  - `ReplicaBufferSuffix`
  - `ReplicaInstallSnapshot`
  - `ReplicaDrainBuffer`
  - `PSYNCRequest`
  - `BacklogEvict`
- First properties:
  - `TypeOK`
  - `NoGap`
  - `NoDup`
  - `SnapshotThenSuffix`
  - `AtomicCutover`
  - `CrossShardAtomic`

## First Counterexamples Worth Checking

- snapshot starts before suffix capture is protected
- off-by-one at `cutover` / `PSYNC offset+1`
- cross-shard command split across snapshot and suffix
- reconnect causes duplicate replay
- live serving begins before backlog drain completes
- stale cutover/session metadata is reused

## Runtime Validation Complement

- exact integration tests around snapshot+during-write overlap
- randomized schedule tests for owner-thread/shard interleavings
- crash/reconnect simulation with backlog exhaustion
- differential Redis/Valkey interop at the cutover boundary

## Recommended Next Actions

- Add a new local spec, likely `formal/tla/specs/ReplicationCutover.tla`, using
  the prefix-log abstraction first and only then layering the concrete
  full-sync/session mechanics.
- Keep the first model small enough to catch the six bug classes above before
  adding full protocol detail.
- When runtime work starts, annotate the eventual critical sections with TLA+
  action labels that match the spec.
