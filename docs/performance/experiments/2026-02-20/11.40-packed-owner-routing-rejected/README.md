# Experiment 11.40 - Packed Owner-Routing Variant (Rejected)

## Metadata

- experiment_id: `11.40`
- date: `2026-02-20`
- baseline_commit_for_comparison: `f518a4084fe990c148ecffecf682ac4bdbe6e1d1`
- documentation_commit: `5156aee45d309a64a76aec43b3d48d6310db12f4`
- revert_commit: `N/A (working-tree revert before commit)`
- status: `REJECTED`

## Important Process Note

This prototype was tested as a working-tree-only change and reverted before commit.
Because of that, there is no committed code hash for the prototype diff.
This is explicitly recorded in `prototype-diff-unavailable.txt`.

From this point forward, the mandatory workflow is defined in:
`docs/performance/EXPERIMENT_REPORTING_POLICY.md`.

## Goal

Test a more aggressive owner-routing optimization:

- replace `Vec<Vec<u8>>` arg copies with a packed contiguous buffer + prebuilt `ArgSlice` array
- reduce allocations and conversions inside routed closures

## Benchmark Command

```bash
cd garnet-rs
BASE_BIN=/tmp/garnet-server-shardfnv \
NEW_BIN=/tmp/garnet-server-ownedargs-packed \
BASE_LABEL=frame NEW_LABEL=packed \
RUNS=3 THREADS=8 CONNS=16 REQUESTS=10000 PRELOAD_REQUESTS=10000 \
PIPELINE=32 STRING_STORE_SHARDS=2 OWNER_THREADS=2 \
PORT=16480 OUTDIR=/tmp/garnet-owner2-ab-packed-20260220-145542 \
./benches/binary_ab_local.sh
```

## Results (Rejected)

Source: `artifacts/owner2-p32-packed-comparison.txt`

| Metric | Frame | Packed | Delta |
|---|---:|---:|---:|
| median_set_ops | 380699.000 | 346706.000 | -8.93% |
| median_get_ops | 391904.000 | 212387.000 | -45.81% |
| median_set_p99_ms | 52.22300 | 59.64700 | +14.22% |
| median_get_p99_ms | 21.50300 | 47.35900 | +120.24% |

Decision:

- Prototype is not acceptable.
- Code was reverted immediately.
- Existing `11.38` implementation is retained.

## Artifacts

- `artifacts/owner2-p32-packed-comparison.txt`
- `artifacts/owner2-p32-frame-summary.txt`
- `artifacts/owner2-p32-packed-summary.txt`
- `artifacts/owner2-p32-frame-runs.csv`
- `artifacts/owner2-p32-packed-runs.csv`
- `prototype-diff-unavailable.txt`
