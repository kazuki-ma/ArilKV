# Replication Full-Sync Architecture Notes (2026-03-16)

Source report:

- `docs/performance/replication-fullsync-architecture-deepresearch-2026-03-16.md`

## What This Report Confirms

- The safest external contract for one exported Redis-compatible instance is
  still one **global cutover offset** on one logical replication stream.
- The recommended downstream shape is still:
  - define cutover once
  - generate a point-in-time RDB for that cutover
  - retain all `> cutover` writes in a replayable backlog
  - drain that suffix before joining the normal live stream
- Start with **single-channel correctness first** and treat Valkey
  dual-channel replication as a later capability-gated optimization, not the
  first correctness mechanism.

## What This Tightens

- The report takes a more conservative line than our local "replication unit"
  exploration: for wire-compatible export, **per-shard independent cutover
  epochs remain unsafe** while the server is exported as one logical Redis
  instance with one offset space.
- The acceptable form of shard skew is narrower:
  - shards may materialize at different wall-clock times
  - but only if each shard is pinned to the same logical cutover / barrier
  - and the snapshot still denotes one prefix of the external history
- Backlog design should not be "best effort". It needs explicit **session
  protection** so a full-sync session cannot lose its suffix mid-transfer.

## Immediate Implementation Guidance

- Keep the current primary design target:
  - global cutover offset
  - shard barrier
  - online snapshot view
  - shared backlog
  - per-session suffix protection
- Keep the conservative fallback available:
  - short write stop
  - snapshot
  - backlog replay
  This is acceptable for the first minimum-correct production slice if online
  snapshotting is not ready.
- For upstream ingest, use:
  - `stage -> validate -> publish`
  - plus buffering/spooling of post-RDB commands during load
  Merely "stop reading while loading" is not a robust design because upstream
  output-buffer pressure can turn that into disconnects.

## How This Interacts With Local Notes

- This report supports the current local direction in
  `replication-fullsync-cutover-notes-2026-03-16.md` and
  `replication-fullsync-idea-hunt-2026-03-16.md`, but it clearly biases the
  near-term implementation toward the **more defensive global-cutover model**.
- The earlier "replication unit" idea should therefore be treated as:
  - a possible future internal optimization strategy
  - not the first wire-compatibility contract

## Recommended Next Actions

- Build the real `FullSyncSession` around:
  - `cutover_offset`
  - session-held replay protection
  - explicit snapshot completion and backlog-drain states
- Replace the current `build snapshot -> subscribe -> stream live` handoff with
  `capture boundary/protection -> build snapshot -> drain suffix -> join live`.
- Add bounded disconnect/retry behavior explicitly instead of assuming backlog
  retention will always be sufficient.
- Defer dual-channel export until single-channel correctness and observability
  are complete.
