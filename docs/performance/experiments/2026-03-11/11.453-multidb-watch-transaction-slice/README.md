# 11.453 MultiDB watch/transaction slice benchmark

- Date: 2026-03-11
- Before: `5d765e15a3` (`docs: refresh compatibility baseline after multidb slice`)
- After: working tree before commit `11.453`
- Scope:
  - DB-aware `WATCH` / `EXEC` refresh/version checks
  - queued `SELECT` support inside `MULTI/EXEC`
  - auxiliary-DB `FLUSHDB` / `FLUSHALL` semantic deletes
  - auxiliary-DB active expire sweep
  - nonzero-DB `INCR` family / `INCRBYFLOAT`
  - replication rewrite for `MULTI` with interleaved `SELECT`
- Harness: `garnet-rs/benches/binary_ab_local.sh`
- Parameters:
  - smoke: `RUNS=1 THREADS=4 CONNS=8 REQUESTS=3000 PRELOAD_REQUESTS=3000 PIPELINE=1`
  - rerun: `RUNS=3 THREADS=4 CONNS=8 REQUESTS=3000 PRELOAD_REQUESTS=3000 PIPELINE=1`

## Result

- Smoke (`artifacts/binary-ab/comparison.txt`)
  - `SET ops`: `124537 -> 113494` (`-8.87%`)
  - `GET ops`: `146831 -> 141116` (`-3.89%`)
  - `SET p99`: `0.543ms -> 0.831ms` (`+53.04%`)
  - `GET p99`: `0.439ms -> 0.455ms` (`+3.64%`)
- Rerun (`artifacts/binary-ab-r3/comparison.txt`)
  - `SET ops`: `51388 -> 51525` (`+0.27%`)
  - `GET ops`: `40029 -> 119694` (`+199.02%`)
  - `SET p99`: `3.103ms -> 3.839ms` (`+23.72%`)
  - `GET p99`: `5.247ms -> 0.639ms` (`-87.82%`)

## Notes

- This slice is correctness-first for nonzero-DB transaction/watch semantics; DB0 fast-path throughput is only a guardrail here.
- The one-run smoke suggested a SET regression, but the `RUNS=3` rerun was highly noisy and not directionally stable, so there is no reproducible benchmark signal yet.
- External compatibility target for this slice was `unit/multi` without `--singledb`; that probe now passes at `ok=60 err=0 timeout=0 ignore=3`.

## Artifacts

- `artifacts/base_commit.txt`
- `artifacts/binary-ab/comparison.txt`
- `artifacts/binary-ab-r3/comparison.txt`
- `diff.patch`
