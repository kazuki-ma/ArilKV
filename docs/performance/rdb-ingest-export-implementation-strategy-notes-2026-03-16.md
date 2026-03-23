# RDB Ingest/Export Implementation Strategy Notes (2026-03-16)

Source report:

- `docs/performance/rdb-ingest-export-implementation-strategy-deepresearch-2026-03-16.md`

## Primary Decision

- The default end-state should be **in-tree pure Rust** for both:
  - whole-RDB ingest
  - whole-RDB export
- `redis/librdb` is a credible **ingest-only fallback/pilot**, not the preferred
  long-term core path.

## Scope To Lock First

- Treat **RDB11** as the initial compatibility target:
  - Redis OSS up to `7.2.x`
  - Valkey `7.2.x` / `8.0`
- Treat newer non-OSS Redis CE RDB drift and Valkey `RDB80+` as later explicit
  work, not day-one scope.

## Why This Fits Garnet

- The report agrees that the existing DUMP decode work is the best bootstrap
  asset for RDB ingest because value encodings are shared.
- There is still no convincing Rust ecosystem choice that cleanly solves both:
  - parser quality for production ingest
  - real writer support for downstream `FULLRESYNC`
- That means writer ownership lands in-tree anyway, so a split
  "foreign parser + local writer" design only makes sense as a schedule-driven
  temporary compromise.

## Concrete Design Guidance

- Structure ingest as:
  - container parser for RDB framing/opcodes
  - value decoder with version-aware object decoding
  - `stage -> validate -> publish` swap model
- Fail closed on:
  - unknown required opcode
  - unknown object type
  - checksum mismatch
  - unsupported module/function payload
- Ignore unknown `AUX` fields for forward compatibility.
- Prefer export as:
  - streaming RDB writer
  - `RDB11` output first
  - simple/non-compressed encodings first
  - `EOF` marker transport as the default streaming path
  - length-known mode only as a fallback

## Important Local Adjustment

- The generic report says streams/functions/modules can be deferred in the first
  slice if unsupported cases fail closed.
- For Garnet specifically, this should be read narrowly:
  - fail-closed is acceptable for the very first attach-time RDB slice
  - but stream support will likely need to move up quickly because stream
    persistence/compatibility is already a real product surface in this repo

## Recommended Next Actions

- Build an `rdb` container layer around the existing DUMP-value decode logic
  instead of creating a second object-decoding stack.
- Lock export to `RDB11` and make the support window explicit in docs/tests.
- Add staged ingest publish semantics before attempting broad type coverage.
- Add a dedicated validation lane:
  - golden corpora
  - parser fuzzing
  - Redis/Valkey differential load tests
  - `redis-cli --rdb` / `valkey-cli --rdb` interop probes
