# 11.450 MultiDB slice 1 benchmark

- Date: 2026-03-11
- Before: `29ef731171` (`docs: refresh compatibility baseline after redis-cli fixes`)
- After: working tree before commit `11.450`
- Scope: first MultiDB slice (`SELECT`/`COPY DB`/`MOVE`/`FLUSHDB`/`FLUSHALL` on auxiliary DB state, DB-aware key/object/expiration accessors, DB1 keysizes accounting)
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters: `RUNS=1 THREADS=1 CONNS=4 REQUESTS=2000 PRELOAD_REQUESTS=2000 PIPELINE=1 SIZE_RANGE=1-64`

## Result

- `SET ops`: `57901 -> 64216` (`+10.91%`)
- `GET ops`: `65842 -> 68557` (`+4.12%`)
- `SET p99`: `0.159ms -> 0.111ms` (`-30.19%`)
- `GET p99`: `0.127ms -> 0.119ms` (`-6.30%`)

## Notes

- This smoke benchmark exists because the slice adds `selected_db` branches to string/object/expiration paths.
- The benchmark does not exercise cross-DB commands directly; it is a hot-path regression check for the DB0 fast path after the branching changes.
- Raw artifacts are under `artifacts/`.
