# Experiment 11.39 - Align Local Hotspot Default to 2 Shards

## Metadata

- experiment_id: `11.39`
- date: `2026-02-20`
- before_commit: `7438375d5749b5f72281b77301dd701ee76c7fa4`
- after_commit: `f518a4084fe990c148ecffecf682ac4bdbe6e1d1`
- commit_message: `bench: default local hotspot captures to 2 shards`
- full_diff: `docs/performance/experiments/2026-02-20/11.39-hotspot-default-shards2/diff.patch`

## Goal

Make default local hotspot captures reflect current runtime policy (`STRING_STORE_SHARDS=2`) instead of a lock-heavy single-shard profile (`1`).

## Measured Before/After Workloads (pre-change measurement basis)

1. Baseline-style capture (`shards=1`)
   - command:
     ```bash
     cd garnet-rs
     PIPELINE=32 THREADS=8 CONNS=16 REQ_PER_CLIENT=30000 \
     OUTDIR=/tmp/garnet-hotspots-p32-20260220-143101 \
     ./benches/local_hotspot_framegraph_macos.sh
     ```
2. Comparison capture (`shards=2`)
   - command:
     ```bash
     cd garnet-rs
     STRING_STORE_SHARDS=2 PIPELINE=32 THREADS=8 CONNS=16 REQ_PER_CLIENT=30000 \
     OUTDIR=/tmp/garnet-hotspots-p32-s2-20260220-143214 \
     ./benches/local_hotspot_framegraph_macos.sh
     ```
3. Script syntax sanity after update
   - command: `bash -n garnet-rs/benches/local_hotspot_framegraph_macos.sh`
   - result: `exit 0`

## Results Snapshot

Source: `artifacts/p32-totals-shards1-vs-2.txt`

| Condition | GET Totals Ops/sec | SET Totals Ops/sec |
|---|---:|---:|
| shards=1 | 270667.28 | 273006.74 |
| shards=2 | 396638.12 | 427012.59 |

Selected hotspot leaf evidence (counts from sampled stacks):

- `__psynch_mutexwait` GET leaf count
  - shards=1: `84265`
  - shards=2: `55044`
- `__psynch_mutexwait` SET leaf count
  - shards=1: `91355`
  - shards=2: `52026`

Interpretation: defaulting local hotspot script to shard=2 avoids over-emphasizing single-shard lock contention.

## Diff Excerpt

```diff
-STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-1}"
+STRING_STORE_SHARDS="${STRING_STORE_SHARDS:-2}"
```

```diff
+The default shard setting for this script is `STRING_STORE_SHARDS=2` to match
+current server-side default policy.
```

## Artifacts

- `artifacts/p32-shards1-summary.txt`
- `artifacts/p32-shards2-summary.txt`
- `artifacts/p32-totals-shards1-vs-2.txt`
- `artifacts/p32-shards1-get-leaf-top20.txt`
- `artifacts/p32-shards1-set-leaf-top20.txt`
- `artifacts/p32-shards2-get-leaf-top20.txt`
- `artifacts/p32-shards2-set-leaf-top20.txt`
- `artifacts/p32-shards1-get-incl-top20.txt`
- `artifacts/p32-shards1-set-incl-top20.txt`
- `artifacts/p32-shards2-get-incl-top20.txt`
- `artifacts/p32-shards2-set-incl-top20.txt`
- `diff.patch`
