# 11.454 MultiDB INFO KEYSIZES selected-db benchmark

- Date: 2026-03-11
- Before: `a334a5b760` (`fix: complete multidb transaction watch semantics`)
- After: working tree before commit `11.454`
- Scope:
  - `INFO KEYSIZES` now always computes the default histogram under `db0`, independent of the current client `SELECT` state
  - exact regressions for Redis `tests/unit/info-keysizes.tcl` `Test SWAPDB` and `DEBUG RELOAD reset keysizes`
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters:
  - smoke: `RUNS=1 THREADS=4 CONNS=8 REQUESTS=3000 PRELOAD_REQUESTS=3000 PIPELINE=1 SIZE_RANGE=1-64`

## Result

- Smoke (`artifacts/binary-ab/comparison.txt`)
  - `SET ops`: `118842 -> 140243` (`+18.01%`)
  - `GET ops`: `138427 -> 148011` (`+6.92%`)
  - `SET p99`: `0.647ms -> 0.471ms` (`-27.20%`)
  - `GET p99`: `0.471ms -> 0.415ms` (`-11.89%`)

## Notes

- This slice changes only admin/introspection behavior (`INFO KEYSIZES`) plus tests; it does not alter DB0 read/write fast paths.
- The smoke run is used as a guardrail only. It shows no regression signal.
- External compatibility target for this slice was `RUNTEXT_SINGLEDB=0 RUNTEXT_RUN_ONLY_ISOLATED_UNIT=unit/info-keysizes`; that probe now passes at `ok=18 err=0 timeout=0 ignore=8`.

## Artifacts

- `artifacts/base_commit.txt`
- `artifacts/binary-ab/comparison.txt`
- `artifacts/binary-ab.stdout.txt`
- `diff.patch`
