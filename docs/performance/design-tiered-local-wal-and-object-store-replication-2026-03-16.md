# Tiered Local WAL + Object Store Replication Design (2026-03-16)

Scope: replication/export architecture for Garnet when we want cloud-native
durability and catch-up without putting object storage on the write hot path.

Related local notes:

- `docs/performance/replication-fullsync-cutover-notes-2026-03-16.md`
- `docs/performance/replication-fullsync-idea-hunt-2026-03-16.md`
- `docs/performance/design-typed-mutation-oplog-and-durability-ledger-2026-03-12.md`
- `docs/performance/design-live-aof-durability-tracking-2026-03-14.md`

## 1. Bottom Line

Object storage should not be the primary write-path log.

The right shape is:

1. append every mutation to a local ordered WAL
2. acknowledge based on local append / local fsync policy
3. seal local WAL segments asynchronously
4. upload sealed segments and snapshots to object storage
5. use object storage as:
   - long-tail catch-up source
   - new-replica bootstrap source
   - durable archive
   - cross-region transfer tier

So the right mental model is:

- local disk/NVMe = hot WAL
- object storage = warm/cold segment tier

That is very different from "write every command directly to S3/Blob".

## 2. Why Direct Object-Store WAL Is The Wrong Default

Writing each mutation directly to S3/Blob looks attractive from a durability
story, but it is a poor fit for the hot path.

The bottlenecks are not mainly raw bandwidth. They are:

- per-request RTT
- object commit overhead
- poor economics for tiny writes
- append-unfriendly object semantics
- high tail latency if durability waits on remote object completion

So a design like:

- command arrives
- write one object or tiny multipart chunk
- wait for remote durability
- ACK client

is not the shape we want.

Even if throughput were acceptable in some cases, the latency profile and tail
behavior are the wrong contract for Garnet's hot path.

## 3. Why Local WAL First Is Reasonable

Local append-only logging is not automatically the bottleneck.

The usual split is:

- `append-only write` is often cheap enough
- `force a durable barrier for every write` is what becomes expensive

So the high-performance path is:

- append ordered records locally
- batch fsync according to policy
- let replication, `WAIT`, `WAITAOF`, full-sync suffix capture, and archive all
  consume the same ordered stream

This matches the existing direction in the local durability notes:

- ordered mutation stream
- explicit durability frontiers
- command semantics derived from ledger/frontier state instead of ad-hoc side
  channels

## 4. Proposed Tiered Architecture

### 4.1 Hot Tier: Local Ordered WAL

The primary owns a local append-only log with explicit offsets.

This log is the source of truth for:

- short-tail replica catch-up
- full-sync suffix capture
- `WAIT` / `WAITAOF` frontiers
- crash-local recovery

Preferred shape:

- one ordered runtime log with typed mutation records or another stable replay
  unit
- enough metadata to derive:
  - replication order
  - local durability frontier
  - per-session replay cursors
  - eventual segment boundaries

This is a better fit than trying to stretch the current broadcast-only
downstream channel into a durable replay substrate.

### 4.2 Warm Tier: Sealed Local Segments

The live WAL is periodically cut into immutable segments.

Each sealed segment should have:

- `segment_id`
- `start_offset`
- `end_offset`
- `record_count`
- checksum / digest
- format version
- optional per-lane index metadata

The act of sealing is important because it gives a stable object to upload and
reference in manifests.

### 4.3 Cold/Wide Tier: Object Storage

Sealed segments and snapshots are uploaded asynchronously to object storage.

Object storage is then used for:

- replica bootstrap from snapshot + manifest
- old-gap catch-up after local retention is exceeded
- archive / disaster-recovery retention
- cross-AZ or cross-region transfer

This makes object storage part of the replication system without making it part
of the hot client write latency.

## 5. Snapshot + Suffix Under This Model

This model fits the current full-sync problem quite well.

For a new or far-behind replica:

1. create a full-sync session
2. capture a replay boundary in the local WAL
3. start suffix capture from that boundary
4. materialize the snapshot
5. deliver snapshot
6. replay the suffix after the captured boundary
7. switch to live streaming

The useful extension is:

- if the session runs long or the suffix exceeds local in-memory/session
  buffers, the source of replay does not need to remain purely in RAM
- the replay source can spill or already exist in local WAL segments
- if the replica is *very* far behind, recovery can resume from object-store
  segments instead of forcing the primary to retain an unbounded hot replay
  window

So tiering gives us a natural answer for slow-replica and long-full-sync cases.

## 6. The Right Division Of Responsibility

The clean split is:

- live replication path:
  - low latency
  - local WAL backed
  - local memory/session cursors for the hot tail
- archival/cold catch-up path:
  - object-store backed
  - optimized for throughput and retention, not immediate ACK

This avoids mixing two very different QoS profiles into one mechanism.

## 7. Interaction With `WAIT` And `WAITAOF`

This design becomes more compelling when viewed together with the live
durability notes.

Local WAL frontiers can naturally expose:

- append frontier
- local fsync frontier
- downstream replica applied frontier
- downstream replica durable frontier, if/when replicas report one

That leads to a clean decomposition:

- `WAIT`
  - wait on replication/applied frontier
- `WAITAOF numlocal`
  - wait on local fsync frontier
- future durable-replica wait
  - wait on replica durable frontier

Object storage does **not** need to sit in these synchronous commands.

It helps by extending replay retention and recovery range, not by becoming the
per-command durability barrier.

## 8. Manifest Design

Object storage becomes usable only if we have a clean manifest contract.

The minimum useful manifest likely needs:

- dataset or stream id
- generation / epoch / term
- snapshot object reference
- snapshot boundary offset
- ordered list of WAL segments after the snapshot boundary
- integrity metadata
- format version

That gives a replica enough information to do:

- bootstrap from snapshot
- replay suffix from ordered sealed segments
- verify continuity

For Garnet specifically, the key property is continuity:

- `snapshot_boundary_offset`
- first replay segment must begin strictly after or exactly at the required next
  record depending on record semantics
- no gaps
- no duplicates

This is the same core correctness story as the local full-sync discussion, only
persisted into object storage.

## 9. Record Format Implications

This design works best if the replay unit is not just an opaque ad-hoc network
frame.

The further Garnet moves toward a stable typed mutation log, the more value we
get:

- easier replay
- better indexing
- easier per-lane classification
- easier durability accounting
- easier segment integrity validation
- easier future object-store catch-up

If we stay entirely on command-rewrite frames, the tiered model still works,
but:

- indexing is weaker
- cross-partition unit tracking is harder
- replay correctness is harder to reason about

So this design is another argument in favor of a more principled replay record
type over time.

## 10. Retention And Garbage Collection

Tiering only helps if retention is explicit.

Local retention policy should likely be based on:

- current live tail needs
- slowest active replica/session cursor
- object-store upload completion
- checkpoint/snapshot compaction boundary

Object-store retention policy should likely be based on:

- latest retained snapshot generations
- disaster-recovery retention goals
- maximum acceptable catch-up age
- compliance/cost policy

This creates two independent but related GC decisions:

- when may local hot segments be deleted?
- when may remote archived segments be deleted?

## 11. Failure Model

The intended failure behavior is:

- local crash:
  - recover from local durable WAL + latest local snapshot/checkpoint
- replica falls behind local retention:
  - catch up from object-store manifest + sealed segments
- region/network event:
  - object-store tier remains a portable recovery/bootstrap source

This is strictly stronger operationally than relying on RAM-only suffix buffers
for full-sync and slow replicas.

## 12. Costs And Risks

Main costs:

- more moving parts than local-only replay
- manifest/versioning complexity
- retention/GC mistakes can create hard-to-debug holes
- object-store eventual consistency assumptions must be handled carefully
- typed replay-record work becomes more valuable, which may widen the design
  scope

Main trap to avoid:

- sneaking object-store completion back into the synchronous client ACK path

If we do that, we lose the whole point of the tiered architecture.

## 13. Suggested Garnet Adoption Order

### Stage 1: Make Local WAL The Real Replay Substrate

- stop thinking of the downstream broadcast stream as the durable replay system
- introduce explicit replay cursors / boundaries on the local ordered log
- use that for full-sync suffix correctness

### Stage 2: Cut Immutable Local Segments

- formalize segment boundaries
- add segment metadata and integrity checks
- make slow-session replay able to read from sealed local segments

### Stage 3: Add Object-Store Upload + Manifest

- upload sealed segments asynchronously
- publish manifest format
- support replica bootstrap/catch-up from remote segments

### Stage 4: Unify With Snapshot Export

- publish snapshot + segment manifest as a stable bootstrap package
- make far-behind replicas prefer the package over forcing a primary-local giant
  retention window

## 14. Recommendation

For Garnet, the best object-storage design is:

- not direct remote-WAL durability on every write
- but a tiered local-WAL-first architecture

That gives us:

- better hot-path latency
- a clean answer for slow replicas
- a real place for cloud-native archive/bootstrap
- better alignment with `WAIT` / `WAITAOF`
- and a much stronger foundation for future full-sync correctness

So if the question is "can cloud storage be part of the design?", the answer is
yes.

If the question is "should S3/Blob be the write hot path?", the default answer
should be no.
