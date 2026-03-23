# 11.535 Docker/Linux loopback flamegraph

- Date: 2026-03-19
- Purpose: generate Linux-side flamegraphs from the idle-host Docker/Linux perf capture and verify whether the hotspot shape changed.
- Before commit: `fa4106fd5c`
- After commit: `working-tree (iteration 819; docs/perf artifacts only)`
- Diff note: no runtime code changed in this iteration; the flamegraphs were generated from the existing `perf script` outputs captured during `11.534`.

## Important constraint

- This benchmark path is container-local loopback.
- `linux_perf_diff_profile.sh` runs both server and `memtier_benchmark` inside the same Linux container with `HOST=127.0.0.1`.
- Because traffic never leaves loopback, true NIC Receive Flow Steering is not part of this measurement.
- On the current machine the real host is still Darwin, so there is no meaningful host-RFS toggle to apply for this experiment.

## Commands

- FlameGraph tools:
  - `git clone --depth 1 https://github.com/brendangregg/FlameGraph.git /tmp/FlameGraph-20260319`
- SVG generation:
  - `stackcollapse-perf.pl` + `flamegraph.pl` over:
    - `/tmp/garnet-linux-perf-diff-dragonfly-20260319/garnet/set/perf-script-set.txt`
    - `/tmp/garnet-linux-perf-diff-dragonfly-20260319/garnet/get/perf-script-get.txt`
    - `/tmp/garnet-linux-perf-diff-dragonfly-20260319/dragonfly/set/perf-script-set.txt`
    - `/tmp/garnet-linux-perf-diff-dragonfly-20260319/dragonfly/get/perf-script-get.txt`

## Results

- Raw SVGs:
  - Garnet SET: `/tmp/garnet-linux-perf-flamegraphs-20260319/garnet-set.flame.svg`
  - Garnet GET: `/tmp/garnet-linux-perf-flamegraphs-20260319/garnet-get.flame.svg`
  - Dragonfly SET: `/tmp/garnet-linux-perf-flamegraphs-20260319/dragonfly-set.flame.svg`
  - Dragonfly GET: `/tmp/garnet-linux-perf-flamegraphs-20260319/dragonfly-get.flame.svg`
- Garnet shape did not materially change:
  - SET and GET are still led by `try_to_wake_up` on `garnet-owner-0`
  - the next large block is still `__wake_up_sync_key` on the Tokio worker, tied to the socket send/receive wakeup path
- Dragonfly shows a different distribution:
  - more spread across `io_uring` proactor work, scheduler bookkeeping, and wakeups
  - less of a single dominant owner-thread handoff signature

## Interpretation

- The idle-host rerun improves absolute throughput, but it does not overturn the earlier hotspot hypothesis.
- For Garnet plain GET/SET, the hot path still looks boundary-heavy: owner-thread wakeup, runtime wakeup, and TCP wakeup/send activity dominate before value-codec work becomes visible.
- So the likely next optimization target remains the same: reduce `connection -> owner-thread -> wakeup` cost before spending time on deeper codec micro-optimizations.
