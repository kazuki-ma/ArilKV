# DeepResearch Instruction (No Repo Access): RDB Ingest/Export Implementation Strategy for a Redis-Compatible Server

## Important Constraints

- Assume you **cannot access this repository**.
- Build the answer from public sources only.
- Prefer primary/official sources for product behavior and library status.
- If you use source code or crate metadata, cite exact repositories/pages and classify the source.
- We want the best implementation strategy today, not just the easiest one.

## Project Context (Provided Manually)

We are building a Redis-compatible server in Rust.

Current serialization state:

- We already have custom in-tree snapshot/debug serialization used for internal reload/testing flows.
- We already decode Redis `DUMP` payloads for some object types, including enough Redis RDB object-encoding logic to restore selected value forms.
- We do **not** yet have a complete whole-RDB ingest path for replication `FULLRESYNC`.
- We do **not** yet have a complete whole-RDB export path that can encode a true dataset snapshot for downstream Redis/Valkey replicas.

Current replication goal:

- Upstream `FULLRESYNC`: ingest Redis/Valkey RDB into Garnet state.
- Downstream `FULLRESYNC`: emit a valid Redis-compatible RDB snapshot from Garnet state.

Current constraints:

- The server is in Rust.
- We care about long-term maintenance and security posture.
- We are willing to use a library or FFI if that is clearly the best choice.
- We do not want to pick an abandoned or weakly maintained dependency for such a core path.
- We need explicit guidance for a phased rollout if the best end-state is larger than the first safe slice.

## Research Goals

1. Determine the best strategy for whole-RDB ingest and export in Rust today.
2. Decide whether we should:
   - build/extend an in-tree pure-Rust implementation,
   - adopt a Rust crate,
   - use FFI to an official C library such as `redis/librdb`,
   - or use some hybrid approach.
3. Produce a phased, testable implementation plan with clear risk tradeoffs.

## Questions To Answer

1. What are the credible implementation options **today** for RDB parsing and generation?
   - Evaluate at least:
     - in-tree pure Rust implementation
     - `redis/librdb` via FFI if appropriate
     - the current Rust `rdb` ecosystem and any newer credible alternatives
     - sidecar/process-based approaches if you believe one is surprisingly better

2. Which option is the best fit for a Redis-compatible Rust server?
   - Consider:
     - maintenance status
     - license
     - version drift risk
     - performance
     - ability to support both ingest and export
     - safety and fuzzability
     - difficulty of interop testing

3. What object/type coverage should the first production slice include?
   - strings
   - expiries
   - logical databases / `SELECT`
   - streams and stream metadata
   - function libraries if applicable
   - hashes / lists / sets / zsets
   - AUX fields / metadata
   - checksum and compression behavior
   - unsupported module payloads or future version fields

4. What are the best strategies for whole-RDB **ingest**?
   - direct decode into live structures
   - decode into a staging model, then publish
   - decode to canonical logical objects and reuse existing restore paths
   - mixed fast path / fallback path

5. What are the best strategies for whole-RDB **export**?
   - direct encoder over live pinned snapshot views
   - staged intermediate object model
   - library-assisted writer
   - streaming writer vs full materialization in memory

6. How should versioning and compatibility be handled?
   - Redis vs Valkey version drift
   - supported RDB version window
   - handling unknown AUX fields
   - handling unsupported object/module encodings
   - forward-compat policy

7. What should the test strategy be?
   - cross-version golden corpora
   - round-trip tests
   - fuzzing
   - differential testing against Redis/Valkey
   - compatibility probes using real `redis-server`, `valkey-server`, and `redis-cli --rdb`

8. What are the main failure modes and mitigations?
   - malformed or malicious RDB payloads
   - memory blow-ups during import/export
   - partial ingest publication
   - checksum/compression bugs
   - abandoned dependency risk
   - FFI boundary risk

## Required Output Format

Please structure the answer exactly as:

1. `Executive Summary`
   - no more than 10 bullets
2. `Candidate Matrix`
   - Candidate
   - Maintenance status
   - License
   - Ingest support fit
   - Export support fit
   - Safety / security posture
   - Operational burden
   - Adopt now / pilot / avoid
3. `Recommended Default`
   - one primary choice
   - one fallback choice
   - why
4. `Minimum Type Coverage`
   - what the first production slice must support
   - what can be explicitly deferred
5. `Ingest Design`
   - decoding model
   - staging/publish model
   - error policy
6. `Export Design`
   - encoding model
   - streaming/materialization policy
   - versioning policy
7. `Compatibility Strategy`
   - Redis / Valkey coverage plan
   - future version handling
8. `Validation Blueprint`
   - corpus
   - fuzzing
   - differential tests
   - real-server interop
9. `Phased Delivery Plan`
   - Phase 1
   - Phase 2
   - Phase 3
10. `Risk Register`
11. `References`
   - URLs and source classification

## Specific Things To Investigate

- Current state of `redis/librdb`
- Current state of Rust `rdb` crates and whether any are abandoned or clearly insufficient
- Whether Valkey has diverged enough to matter for parser/writer strategy
- Whether export should target a narrow RDB version initially or multi-version support immediately
- Whether using an official C parser/writer for first correctness and then replacing pieces later is a good or bad long-term bet

## Decision Criteria We Will Apply

- Core-path maintainability matters as much as raw correctness.
- We prefer a recommendation that supports both ingest and export with a coherent testing story.
- Active maintenance and production credibility beat cleverness.
- If the best answer is "extend in-tree Rust and avoid abandoned crates", say that directly.
