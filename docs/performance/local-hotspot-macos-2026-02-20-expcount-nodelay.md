# Local Hotspot Follow-up (macOS) — 2026-02-20

## Scope

Validated two incremental GET/SET-path changes on macOS using
`garnet-rs/benches/local_hotspot_framegraph_macos.sh`:

1. Per-shard expiration-count fast path (`11.30`)
2. `TCP_NODELAY` on accepted sockets (`11.31`)
3. Internal shard routing hash swap (`redis_hash_slot` -> FNV-1a, `11.33`)
4. `OrderedMutex` backend swap experiment (`std` -> `parking_lot`, `11.36`, rejected)

All runs used:

- `STRING_STORE_SHARDS=2`
- default script settings (`THREADS=8`, `CONNS=16`, `REQ_PER_CLIENT=30000`,
  `PIPELINE=1`, `SIZE_RANGE=1-1024`)

## Artifact Paths

- Baseline (`post-slotcache`): `/tmp/garnet-hotspots-post-slotcache-20260220-131453`
- After expiration-count fast path: `/tmp/garnet-hotspots-post-expcount-20260220-131945`
- After `TCP_NODELAY`: `/tmp/garnet-hotspots-post-nodelay-20260220-132224`
- After shard-routing hash swap (rebuilt binary):
  `/tmp/garnet-hotspots-post-shardfnv-rebuild-20260220-133202`
- Parking_lot backend experiment (reverted / not adopted):
  `/tmp/garnet-hotspots-post-parkinglot-20260220-140407`

## Results Snapshot

### GET-only

| Run | Ops/sec | p99 (ms) | `__psynch_mutexwait` | `__sendto` | `__recvfrom` |
|---|---:|---:|---:|---:|---:|
| post-slotcache | 164110.49 | 1.59900 | 25.67% | 32.07% | 31.48% |
| post-expcount | 165290.39 | 1.64700 | 29.14% | 30.49% | 29.86% |
| post-nodelay | 165149.31 | 1.57500 | 24.98% | 32.41% | 31.88% |
| post-shardfnv-rebuild | 163656.59 | 1.56700 | 23.13% | 33.01% | 33.19% |
| post-parkinglot (reverted) | 153744.82 | 1.84700 | 0.22% | 24.65% | 22.98% |

### SET-only

| Run | Ops/sec | p99 (ms) | `__psynch_mutexwait` | `__sendto` | `__recvfrom` |
|---|---:|---:|---:|---:|---:|
| post-slotcache | 163204.58 | 1.59100 | 21.87% | 32.70% | 32.84% |
| post-expcount | 162249.22 | 1.60700 | 23.19% | 32.04% | 32.14% |
| post-nodelay | 163982.21 | 1.59900 | 22.81% | 31.94% | 32.63% |
| post-shardfnv-rebuild | 162441.75 | 1.59100 | 20.90% | 33.31% | 33.66% |
| post-parkinglot (reverted) | 159047.72 | 1.83900 | 0.20% | 24.29% | 23.14% |

## Observations

- `11.30` (expiration-count fast path) was near-neutral in this single-run sample:
  - GET `+0.72%`, SET `-0.59%` vs `post-slotcache`.
- `11.31` (`TCP_NODELAY`) showed a small favorable shift vs `post-expcount`:
  - GET `-0.09%` (flat), SET `+1.07%`
  - GET p99 `-4.37%`, SET p99 `-0.50%`
- `11.33` (FNV shard routing) removed `redis_hash_slot` from sampled local
  leaf hotspots (`0.00%` GET/SET in this run). Throughput was slightly lower
  in this single sample (`GET -0.90%`, `SET -0.94%` vs `post-nodelay`) while
  p99 was slightly better (`GET -0.51%`, `SET -0.50%`).
- `11.36` parking_lot backend swap looked superficially better on
  `__psynch_mutexwait`, but was net-negative in throughput/latency
  (`GET -6.06%`, `SET -2.09%`; p99 up sharply). New dominant leaf symbols
  included scheduler/yield paths (`swtch_pri`, `cthread_yield`) and
  `parking_lot::raw_mutex::RawMutex::lock_slow`, so this change was reverted.
- Hotspot distribution remains dominated by socket I/O and mutex wait in this
  low-pipeline benchmark profile.

## Repro Command

```bash
STRING_STORE_SHARDS=2 \
OUTDIR=/tmp/garnet-hotspots-manual-$(date +%Y%m%d-%H%M%S) \
garnet-rs/benches/local_hotspot_framegraph_macos.sh
```
