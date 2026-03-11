# 11.451 MultiDB keyspace slice 2 benchmark

- Date: 2026-03-11
- Before: `f11fa65734` (`feat: add first multidb execution slice`)
- After: working tree before commit `11.451`
- Scope:
  - external harness `RUNTEXT_SINGLEDB=0`
  - DB-scoped set hot-state / encoding floors
  - auxiliary-DB `MSET` / delete paths
  - real `SWAPDB`
  - multi-DB `INFO keyspace`
  - Redis/Valkey `SWAPDB` invalid-index error semantics
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters: `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Result

- `SET ops`: `62433 -> 61275` (`-1.85%`)
- `GET ops`: `64088 -> 65454` (`+2.13%`)
- `SET p99`: `0.127ms -> 0.151ms` (`+18.90%`)
- `GET p99`: `0.135ms -> 0.119ms` (`-11.85%`)

## Notes

- This slice mostly changes nonzero-DB and admin code paths. The benchmark exists as a DB0 fast-path non-regression guard because `MSET` and set hot-state plumbing were touched.
- The observed SET p99 increase is from a single smoke run and should be treated as noise unless reproduced in a longer run.
- The compatibility target for this slice was `unit/keyspace` without `--singledb`; that probe now passes at `ok=59 err=0 timeout=0 ignore=0`.

## Artifacts

- `artifacts/comparison.txt`
- `artifacts/ab/`
- `artifacts/benchmark.log`
- `artifacts/hashes.txt`
- `diff.patch`
