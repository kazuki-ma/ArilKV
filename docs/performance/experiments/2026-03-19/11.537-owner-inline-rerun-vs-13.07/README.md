# 11.537 Owner-inline rerun vs 13.07 milestone

- Date: 2026-03-19
- Purpose: verify whether Garnet actually regressed relative to the earlier `13.07` near-parity/milestone result by rerunning the same benchmark shape on the current tree.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 821; measurement/docs only)`
- Diff note: no runtime code changes in this iteration; this is a shape-matched benchmark rerun against the existing milestone artifact.

## Matched benchmark shape

- This rerun matches the old milestone profile, not the newer 2-thread owner-pool profile:
  - `THREADS=1`
  - `CONNS=4`
  - `REQUESTS=30000`
  - `PRELOAD_REQUESTS=30000`
  - `PIPELINE=1`
  - `SIZE_RANGE=1-256`
  - `SERVER_CPU_SET=0`
  - `CLIENT_CPU_SET=1`
  - `GARNET_OWNER_THREAD_PINNING=1`
  - `GARNET_OWNER_THREAD_CPU_SET=0`
  - `GARNET_OWNER_EXECUTION_INLINE=1`

## Current rerun result

- Garnet `SET`: `126411.46 ops/sec`, `p99 0.079 ms`
- Garnet `GET`: `134153.91 ops/sec`, `p99 0.079 ms`
- Dragonfly `SET`: `78911.81 ops/sec`, `p99 0.095 ms`
- Dragonfly `GET`: `92654.36 ops/sec`, `p99 0.087 ms`

## Comparison to 13.07

- Old `13.07` values:
  - Garnet `SET`: `196325.12 ops/sec`, `p99 0.063 ms`
  - Garnet `GET`: `106203.99 ops/sec`, `p99 0.071 ms`
  - Dragonfly `SET`: `86112.99 ops/sec`, `p99 0.087 ms`
  - Dragonfly `GET`: `92310.04 ops/sec`, `p99 0.087 ms`
- What changed:
  - Garnet `SET ops` `-35.61%`
  - Garnet `GET ops` `+26.32%`
  - Garnet `SET p99` `+25.40%`
  - Garnet `GET p99` `+11.27%`
  - Dragonfly `SET ops` `-8.36%`
  - Dragonfly `GET ops` `+0.37%`

## Key hotspot difference

- In this owner-inline rerun, the most surprising top sample for Garnet `SET` is `libc.so.6 cfree`, with stack ownership under `handle_set`.
- That is qualitatively different from the more obvious owner-thread wakeup story in the newer 2-thread synchronized owner-pool profile.
- So there does appear to be a real Garnet-side `SET` regression in the older owner-inline shape, and allocator/free churn is a credible suspect.

## Interpretation

- The answer to “did recent performance get worse?” is now more nuanced:
  - In the newer 2-thread owner-pool profile, the main story is still synchronous owner-thread boundary overhead.
  - In the older `13.07` owner-inline single-thread shape, current Garnet `SET` is materially worse than before while `GET` is better.
- That makes `SET` allocation/free churn the next evidence-backed place to inspect rather than treating everything as one generic owner-thread problem.
