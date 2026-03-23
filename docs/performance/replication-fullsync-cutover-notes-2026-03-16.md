# Replication Full-Sync / Cutover Discussion Notes (2026-03-16)

This note captures the current design discussion around Redis/Valkey-compatible
full-sync replication in `garnet-rs` before the new DeepResearch results return.

It is intentionally a discussion snapshot, not a final architecture decision.

## Current Runtime Facts

- Downstream replication is still modeled as one externally visible replication stream with one global replication offset.
  - `publish_write_frame` advances a single `master_repl_offset` and publishes one frame into one downstream broadcast channel.
- Real downstream Redis replicas currently receive a valid empty Redis RDB payload for `SYNC` / `FULLRESYNC`, then rely on subsequent command propagation.
  - `REPLCONF rdb-only 1` remains a separate debug-snapshot lane for `redis-cli --rdb` / `--functions-rdb`.
- Current downstream `SYNC` / `PSYNC` ordering is:
  1. build snapshot payload
  2. subscribe downstream
  3. switch to replica stream
  - This is sufficient for the current empty-RDB compatibility slice, but would create a snapshot/live gap once we start exporting a real non-empty snapshot.
- Current upstream replica mode still discards the incoming `FULLRESYNC` RDB payload rather than ingesting it into local state.
  - After discarding the payload, Garnet continues applying live replication frames from the upstream primary.

## Main Reframe From The Discussion

The important atomicity boundary is not the command name itself.

The right boundary is the **replication unit**:

- if a mutation is already represented as a partition-local replication unit, it can tolerate per-partition cutover skew
- if a mutation is still represented as one unsplit cross-partition replication unit, splitting it across snapshot and suffix replay is dangerous

This reframing is more useful than asking whether "global epoch vs per-shard epoch" is always right or always wrong.

## What Seems Safe In Principle

Per-partition cutover skew looks sound for a large class of cases, assuming:

- each partition's snapshot is a prefix of that partition's logical history
- each partition's stream replay is the suffix after that partition's cutover
- per-partition stream order is preserved
- any remaining global records have their own explicit serialization lane
- intermediate pre-live-cutover visibility is not itself required to look globally consistent

Under those assumptions:

- single-partition updates like `SET`, `INCR`, single-key expiry-driven `DEL`, and similar partition-local effects can tolerate different cutover points across partitions
- a script/function that reads one key and emits multiple partition-local write effects can also tolerate this skew, as long as the replication path ships the resulting effects rather than requiring the replica to re-run one cross-partition script atom

## Why Scripts/Functions Are Not Automatically Global

Current mutating script/function replication is already fairly effect-oriented:

- scripting code enqueues replication effects after successful `redis.call(...)`
- connection handling consumes those effect frames and publishes them instead of always relying on the top-level `EVAL` / `FCALL` request as the replication record

So the safe/unsafe line is not "script vs non-script".

It is:

- **safe-ish** if the script/function has already been lowered into partition-local effect frames
- **unsafe** if it still survives as one unsplit cross-partition replication unit

## Cases That Still Look Dangerous Today

The following families still look risky for any fine-grained per-partition cutover scheme because they remain command-level, transaction-level, or otherwise unsplit replication units:

- `MSET` / other multi-key top-level frames that are still replicated as one unit
- `MULTI` / `EXEC` bundles, where the replication path still wraps multiple frames with `MULTI` + `EXEC`
- `LMOVE` / `RPOPLPUSH` and similar source/destination operations that remain one cross-key replication frame
- command families that move or derive state across keys or DBs but are not yet lowered into per-partition effects
  - `RENAME`
  - `COPY`
  - `MOVE`
  - `SMOVE`
  - `SUNIONSTORE`
  - `SINTERSTORE`
  - `ZUNIONSTORE`
  - `ZINTERSTORE`
  - `PFMERGE`
  - `SORT ... STORE`
  - `SWAPDB`
  - `MIGRATE`
- global-state mutations that are not naturally tied to one partition
  - `FUNCTION LOAD`
  - `ACL`
  - `CONFIG`
  - `FLUSHALL`-style operations

This list is a design-risk inventory, not a claim that every item must remain global forever.

## Current Working Hypothesis

The likely long-term design shape is:

- partition-local replication units should be allowed to use partition-local cutover points
- remaining cross-partition or global units should be isolated into a separate explicit coordination lane
- the export problem is therefore not "everything must share one epoch"
- it is "we must classify and preserve the atomicity of every replication unit we expose to replicas"

That is a much more promising direction than forcing a universal global cutover for all state.

## Why This Still Looks Hard

Even if the above direction is correct, it is still difficult in practice because:

- the current downstream path exposes one global replication stream / offset
- the export side still needs a correct `snapshot + suffix` splice with no gap
- the current downstream handoff order builds the snapshot before subscription, which is incompatible with a real non-empty snapshot export
- many Redis-compatible commands are still replicated as coarse command-level units instead of partition-local physical/effect records
- downstream Redis/Valkey replicas expect a standard RDB + command-stream protocol, so we cannot arbitrarily invent a private partitioned suffix format for compatibility mode

So "fine-grained epoching might be correct" does not mean "it is cheap or mechanically obvious to implement".

## Questions Still Open Pending DeepResearch

- Is the best export architecture still a single logical global cutover plus backlog replay, even if internal materialization is partition-local?
- Or is a partition-local cutover design with a small explicit global lane the better fit for an owner-thread server?
- What is the best backlog / replay structure for the downstream side?
  - per-session buffer
  - shared offset-indexed ring
  - hybrid with AOF-backed replay
  - dual-channel replication
- What is the best whole-RDB ingest/export strategy in Rust?
  - in-tree implementation
  - `redis/librdb` FFI
  - some hybrid path
- What is the minimal TLA+ model that captures:
  - no-gap
  - no-dup
  - snapshot-then-suffix ordering
  - cross-partition unit indivisibility

## Non-Decisions

This note does **not** decide:

- that per-partition epochs are the final design
- that one global epoch is required
- that every command family must be lowered to effect-level replication
- the final backlog or RDB implementation choice

Those remain active design questions, now tracked by `DR-008`, `DR-009`, and `DR-010`.
