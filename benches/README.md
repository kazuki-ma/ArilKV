# Benchmarks

## Official Redis benchmark (`redis-benchmark`)

`redis_official_benchmark.sh` runs Redis official `redis-benchmark` against Garnet.

### What it does

1. Resolves `redis-benchmark` from `PATH` when available.
2. Otherwise downloads Redis official source from `https://download.redis.io/redis-stable.tar.gz`.
3. Builds only `redis-benchmark`.
4. Starts `garnet-server` (optional).
5. Runs benchmark and writes a timestamped report under `benches/results/`.

### Quick start

```bash
cd .
REDIS_BENCH_REQUESTS=10000 REDIS_BENCH_CLIENTS=20 \
  ./benches/redis_official_benchmark.sh
```

## Patterns To Re-check Before Benchmark Changes

- Validate benchmark integrity from stdout, not only exit code.
  - confirm expected test case or memtier summary counters (`Threads`, `Connections`, `Requests per client`, non-zero ops).
- Keep workload parameters identical for A/B runs.
  - pin `threads/clients/requests/pipeline/key-space/size-range`.
- Treat server-side error lines as hard failures.
  - `handle error response:` and `Connection error:` must fail the run.
- Separate noise from regressions.
  - run multiple iterations and compare median values, not single-run spikes.
- Record reproducibility metadata.
  - keep run command, binary path, commit hash, and output directory in artifacts.

## Fresh Checkout Performance Setup

Use the setup script when a machine or checkout may be missing benchmark tools:

```bash
cd .
benches/setup_perf_compare_env.sh
```

It verifies the Rust toolchains, Docker, `redis-cli`, `jq`, `memtier_benchmark`,
and local release build readiness, then runs a small Docker-backed smoke
comparison for Garnet, Dragonfly, and Valkey. The script does not install
Homebrew Valkey by default because it conflicts with Homebrew Redis binaries;
the Docker harness builds Valkey itself. Set `RUN_SMOKE=0` to only install and
build.

## Garnet / Dragonfly / Valkey Linux Comparison

`docker_linux_perf_diff_profile.sh` runs the comparable Linux harness from
inside a privileged Rust container. It builds Garnet for Linux, builds
`memtier_benchmark`, downloads Dragonfly, builds Valkey, then runs matching
SET/GET workloads through `linux_perf_diff_profile.sh`.

Quick smoke without perf collection:

```bash
cd .
CAPTURE_PERF=0 THREADS=1 CONNS=1 REQUESTS=50 PRELOAD_REQUESTS=50 \
  TARGETS="garnet dragonfly valkey" \
  benches/docker_linux_perf_diff_profile.sh
```

Full comparison with perf data:

```bash
cd .
THREADS=8 CONNS=16 REQUESTS=50000 PRELOAD_REQUESTS=50000 PIPELINE=1 \
  TARGETS="garnet dragonfly valkey" \
  benches/docker_linux_perf_diff_profile.sh
```

Defaults are pinned for reproducibility:

- `DRAGONFLY_VERSION=v1.38.1`
- `VALKEY_VERSION=9.0.4`

Override those environment variables to compare other releases. Use
`VALKEY_IO_THREADS=<n>` and `VALKEY_IO_THREADS_DO_READS=yes|no` for Valkey
threading experiments, and `DRAGONFLY_PROACTOR_THREADS=<n>` /
`DRAGONFLY_CONN_IO_THREADS=<n>` for Dragonfly threading experiments.

## ArilKV / Lux / FeOx Docker Sandbox Comparison

`rust_kv_sandbox_benchmark.sh` builds or reuses isolated Docker images for
ArilKV, Lux, and FeOx, then runs identical `redis-benchmark` workloads through
a private internal Docker bridge network. Runtime containers use no host ports,
non-root users, read-only root filesystems, `no-new-privileges`, and
`cap-drop=ALL`.

Quick run with existing images:

```bash
cd .
SKIP_BUILD=1 \
ARILKV_IMAGE=arilkv-bench:smoke2-20260511-064309 \
TARGETS=arilkv,lux,feox \
REQUESTS=1000000 CLIENTS=50 PIPELINES=1,64 TESTS=set,get \
SERVER_CPUSET=12-15 RESTART_BETWEEN_BENCHMARKS=1 \
benches/rust_kv_sandbox_benchmark.sh
```

On Apple Silicon Macs, avoid leaving benchmark server containers CPU-unbounded
when measuring high-pipeline `redis-benchmark` runs. On the local Apple M5 Max
used for the May 2026 investigation, macOS reported 18 cores with host CPU IDs
`12-17` mapped to the high-performance `cluster-type=P` group. Docker Desktop
does not provide a hard host-core affinity guarantee, but `--cpuset-cpus 12-15`
measured materially faster than `0-3` for ArilKV `GET -c 50 -P 64`.

Use `RESTART_BETWEEN_BENCHMARKS=1` when comparing raw command throughput. This
keeps `redis-benchmark -t get` from inheriting keyspace state from a preceding
`SET` measurement. Without this, GET hit-path and miss-path results are mixed
depending on test order.

Set `ARILKV_TOKIO_WORKER_THREADS=<n>` to force the ArilKV Tokio runtime worker
count inside the container. If it is unset, the harness also accepts the older
`TOKIO_WORKER_THREADS=<n>` environment variable as a fallback.

Representative local snapshot with `SERVER_CPUSET=12-15`,
`RESTART_BETWEEN_BENCHMARKS=1`, `REQUESTS=1000000`, `CLIENTS=50`,
`DATA_SIZE=32`, and `KEYSPACE=1000000`:

| Target | SET P1 | GET P1 | SET P64 | GET P64 |
|---|---:|---:|---:|---:|
| ArilKV | 249812.66 | 256278.83 | 988142.31 | 1838235.25 |
| Lux | 252334.09 | 256607.64 | 5586592.00 | 8403361.00 |
| FeOx | 207511.94 | 232720.50 | 1669449.12 | 7352941.00 |

### Common options

- `REDIS_BENCH_HOST` (default: `127.0.0.1`)
- `REDIS_BENCH_PORT` (default: `6389`)
- `REDIS_BENCH_REQUESTS` (default: `100000`)
- `REDIS_BENCH_CLIENTS` (default: `50`)
- `REDIS_BENCH_DATA_SIZE` (default: `32`)
- `REDIS_BENCH_TESTS` (default: `ping_mbulk,set,get,incr`)
- `REDIS_BENCH_READY_TEST` (default: `ping_mbulk`, probe command used for startup readiness)
- `REDIS_BENCH_START_SERVER` (default: `1`, set `0` to target an already-running server)
- `REDIS_BENCH_FORCE_DOWNLOAD` (default: `0`, set `1` to always fetch/build Redis benchmark binary)
- `REDIS_BENCH_SERVER_BUILD_MODE` (default: `release`, accepts `debug` or `release`)

### Output

Each run writes `redis-official-benchmark-<timestamp>.txt` with benchmark configuration and per-command throughput lines.

## Server launch env (single process)

`garnet-server` launch accepts these env overrides:

- `GARNET_BIND_ADDR` (single base bind, default `127.0.0.1:6379`)
- `GARNET_BIND_ADDRS` (comma-separated explicit binds; takes priority over `GARNET_BIND_ADDR`)
- `GARNET_OWNER_NODE_COUNT` (optional, default `1`; when `>1`, expands from `GARNET_BIND_ADDR` to sequential ports)
- `GARNET_MULTI_PORT_CLUSTER_MODE` (optional bool, default `0`; when enabled with multi-port launch, each port gets a local cluster view and slot-based `MOVED` routing)
- `GARNET_MULTI_PORT_SLOT_POLICY` (optional, default `modulo`; supports `modulo` or `contiguous` slot ownership layout for multi-port cluster mode)
- `GARNET_OWNER_THREAD_PINNING` (optional bool, default `0`; enable owner listener thread CPU pinning)
- `GARNET_OWNER_THREAD_CPU_SET` (optional comma-separated CPU indices, e.g. `0,1,2,3`; if set, pinning is auto-enabled and owner threads are assigned round-robin over this set)
- `GARNET_READ_BUFFER_SIZE`
- `TOKIO_WORKER_THREADS` (optional positive integer; overrides the single-port Tokio runtime worker-thread count)

Note: app-side pinning is best-effort. On some platforms/container settings, affinity calls can fail and the server continues without pinning. Use `taskset -c ...` (Linux) for strict process-level pinning.

Examples:

```bash
# single port
GARNET_BIND_ADDR=127.0.0.1:6389 cargo run -p garnet-server

# four owner nodes in one process (6389..6392)
GARNET_BIND_ADDR=127.0.0.1:6389 GARNET_OWNER_NODE_COUNT=4 cargo run -p garnet-server

# explicit multi-port list
GARNET_BIND_ADDRS=127.0.0.1:6389,127.0.0.1:7390 cargo run -p garnet-server

# 2-node in-process cluster routing (slot modulo split)
GARNET_BIND_ADDR=127.0.0.1:6389 GARNET_OWNER_NODE_COUNT=2 \
  GARNET_MULTI_PORT_CLUSTER_MODE=1 cargo run -p garnet-server

# 4 owner nodes with app-side CPU pinning
GARNET_BIND_ADDR=127.0.0.1:6389 GARNET_OWNER_NODE_COUNT=4 \
  GARNET_OWNER_THREAD_PINNING=1 GARNET_OWNER_THREAD_CPU_SET=0,1,2,3 \
  cargo run -p garnet-server
```

For local launch convenience, use:

```bash
cd .
OWNER_NODES=4 BIND_ADDR=127.0.0.1:6389 OWNER_THREAD_CPU_SET=0,1,2,3 \
  ./benches/run_owner_nodes_pinned_local.sh

# strict pinning on Linux (if taskset is available)
OWNER_NODES=4 BIND_ADDR=127.0.0.1:6389 OWNER_THREAD_CPU_SET=0,1,2,3 \
  USE_TASKSET=1 ./benches/run_owner_nodes_pinned_local.sh
```

## `tidwall/cache-benchmarks` integration

`cache_benchmarks_garnet_wrapper.sh` adapts .NET Garnet CLI flags to the current Rust server.

The wrapper maps `cache-benchmarks` `--memory` into
`GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES` and `--index` into
`GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS` automatically.

It can map thread hints (`--minthreads` / `--maxthreads`) into
`GARNET_TSAVORITE_STRING_STORE_SHARDS` when
`CACHE_BENCH_GARNET_AUTO_STRING_STORE_SHARDS=1` is set and
`GARNET_TSAVORITE_STRING_STORE_SHARDS` is not explicitly set.

It can also map thread hints into owner-thread routing via
`GARNET_STRING_OWNER_THREADS` when
`CACHE_BENCH_GARNET_AUTO_OWNER_THREADS=1` is set and
`GARNET_STRING_OWNER_THREADS` is not explicitly set.

For lock-striping experiments on string keys, set
`GARNET_TSAVORITE_STRING_STORE_SHARDS` (default `2`) to a higher value, e.g.
`8` or `16`.
Server-side hash-index default sizing is
`GARNET_TSAVORITE_HASH_INDEX_SIZE_BITS=16` (when unset); override it explicitly
for A/B runs.

Owner-thread routing uses inline execution by default, so the listener executes
single-key string command work directly on the owner path without a cross-thread
handoff. Set `GARNET_OWNER_EXECUTION_INLINE=0` only for legacy pooled-owner
experiments, and set `GARNET_STRING_OWNER_THREADS=<n>` when explicitly testing
pooled owner-thread counts.

### Minimal run (single benchmark)

```bash
chmod +x benches/cache_benchmarks_garnet_wrapper.sh
cd /tmp/cache-benchmarks-20260219
make
cat > /tmp/cache-benchmarks-garnet-rs.json <<'JSON'
{
  "paths": {
    "memtier": "/opt/homebrew/bin/memtier_benchmark",
    "garnet": "/absolute/path/to/benches/cache_benchmarks_garnet_wrapper.sh"
  }
}
JSON
./bench garnet --config=/tmp/cache-benchmarks-garnet-rs.json --tcp --threads=1 --pipeline=1 --perf=no --ops=2000 --bthreads=2 --conns=4 --sizerange=1-256
```

`./bench` writes `bench.json` with `sets`/`gets` throughput and latency metrics.

If capacity is too small you will now see:

`-ERR storage capacity exceeded (increase max in-memory pages)`

### Dragonfly comparison on macOS (Docker)

Dragonfly official release tarballs are Linux binaries, so on macOS use
`cache_benchmarks_dragonfly_docker_wrapper.sh` as the Dragonfly path in config.

For Docker-to-Docker fairness (both servers inside containers), use
`cache_benchmarks_garnet_docker_wrapper.sh` for Garnet as well.

```bash
chmod +x benches/cache_benchmarks_dragonfly_docker_wrapper.sh \
  benches/cache_benchmarks_garnet_docker_wrapper.sh
docker build -f benches/Dockerfile.garnet-rs-cachebench \
  -t garnet-rs-cachebench:latest garnet-rs
cat > /tmp/cache-benchmarks-compare.json <<'JSON'
{
  "paths": {
    "memtier": "/opt/homebrew/bin/memtier_benchmark",
    "garnet": "/absolute/path/to/benches/cache_benchmarks_garnet_docker_wrapper.sh",
    "dragonfly": "/absolute/path/to/benches/cache_benchmarks_dragonfly_docker_wrapper.sh"
  }
}
JSON
```

For investigation planning and unknown-item closure, see:
`benches/DEEPRESEARCH_DRAGONFLY_INSTRUCTION.md`.

If Docker Desktop is unstable (daemon socket unavailable, intermittent startup
failure), use:
`benches/DOCKER_TROUBLESHOOTING_LOCAL.md`.

## Local GET/SET hotspot framegraph (macOS)

Use `local_hotspot_framegraph_macos.sh` to capture GET-only and SET-only
hotspots from a local `garnet-server` process with macOS `sample`, and render
flamegraphs with Brendan Gregg's FlameGraph scripts.

The script also validates memtier run integrity from stdout summary lines:

- expected `Threads`
- expected `Connections per thread`
- expected `Requests per client`
- non-zero `Totals` Ops/sec (and non-zero GET/SET ops for the target mode)

This prevents treating "exit code only" as success.
The run also fails on memtier `Connection error:` and
`handle error response:` output.

Sampling now stops when the memtier run finishes, so captured stacks stay focused
on active workload time instead of idle tail time.
The default shard setting for this script is `STRING_STORE_SHARDS=2` to match
current server-side default policy.

```bash
cd .
chmod +x benches/local_hotspot_framegraph_macos.sh
./benches/local_hotspot_framegraph_macos.sh
```

Compare shard settings under the same workload:

```bash
cd .
STRING_STORE_SHARDS=1 OUTDIR=/tmp/garnet-hotspots-shards1 ./benches/local_hotspot_framegraph_macos.sh
STRING_STORE_SHARDS=16 OUTDIR=/tmp/garnet-hotspots-shards16 ./benches/local_hotspot_framegraph_macos.sh
```

Main outputs are written under `/tmp/garnet-hotspots-<timestamp>/`:

- `garnet-get.flame.svg`
- `garnet-set.flame.svg`
- `get.incl.top20.txt`
- `set.incl.top20.txt`
- `SUMMARY.txt`

`SUMMARY.txt` also records `string_store_shards=<N>` so A/B runs are traceable.

## String-store shard sweep (local)

Use `sweep_string_store_shards_local.sh` to run a repeatable local A/B/C...
throughput sweep for `GARNET_TSAVORITE_STRING_STORE_SHARDS`.

```bash
cd .
chmod +x benches/sweep_string_store_shards_local.sh
SHARD_COUNTS=\"1 2 4 8 16\" REQUESTS=20000 \
  ./benches/sweep_string_store_shards_local.sh
```

The script prints CSV and validates each run from memtier summary lines
(`Threads`, `Connections per thread`, `Requests per client`, and non-zero
`Ops/sec`) so failed/partial runs are not treated as success.
It also treats memtier `Connection error:` and `handle error response:`
lines as failed runs and defaults to `HOST=127.0.0.1` to avoid `localhost`
IPv6 fallback noise.

## String-store shard policy matrix (local)

Use `sweep_string_store_policy_matrix_local.sh` to run a workload matrix over
shard counts and owner-thread modes, then emit recommendation artifacts.

```bash
cd .
chmod +x benches/sweep_string_store_policy_matrix_local.sh
OWNER_THREAD_COUNTS="0 16" \
WORKLOAD_MATRIX="w1:1:4:20000 w2:4:8:20000 w3:8:16:20000" \
./benches/sweep_string_store_policy_matrix_local.sh
```

Outputs under `/tmp/garnet-shard-policy-matrix-<timestamp>/`:

- `matrix-results.csv` (per workload/owner/shard row)
- `summary-by-owner.csv` (median aggregation per owner+shard)
- `recommendations.txt` (best shard by median geo-mean throughput)

## Performance Regression Gate (local + CI)

Use `perf_regression_gate_local.sh` to run repeated SET/GET benchmarks, validate
memtier summary integrity, and fail on median throughput/latency threshold
regressions.
The gate also fails on memtier `Connection error:` and
`handle error response:` output.

```bash
cd .
chmod +x benches/perf_regression_gate_local.sh
RUNS=5 THREADS=4 CONNS=8 REQUESTS=10000 \
MIN_MEDIAN_SET_OPS=150000 MIN_MEDIAN_GET_OPS=150000 \
MAX_MEDIAN_SET_P99_MS=5 MAX_MEDIAN_GET_P99_MS=5 \
./benches/perf_regression_gate_local.sh
```

Outputs under `/tmp/garnet-perf-gate-<timestamp>/`:

- `runs.csv` (per-run SET/GET ops + p99)
- `summary.txt` (median metrics used for gate decisions)

Nightly/dispatch CI automation is defined in:

- `.github/workflows/garnet-rs-perf-gate.yml`

In this environment, GitHub Actions is not available, so run the local gate
script directly.

## Linux Differential Profiling (`perf`)

Use `linux_perf_diff_profile.sh` on a Linux host to capture `perf record`
profiles for `garnet` and `dragonfly` under matching memtier workload settings.

```bash
cd .
chmod +x benches/linux_perf_diff_profile.sh
DRAGONFLY_BIN=/usr/local/bin/dragonfly \
THREADS=8 CONNS=16 REQUESTS=50000 PIPELINE=1 \
./benches/linux_perf_diff_profile.sh
```

Outputs under `/tmp/garnet-linux-perf-diff-<timestamp>/`:

- `<target>/<workload>/perf.data`
- `<target>/<workload>/perf-report-<workload>.txt`
- `<target>/<workload>/perf-script-<workload>.txt`
- optional `flame-<target>-<workload>.svg` when `FLAMEGRAPH_DIR` is set

`HOST` defaults to `127.0.0.1` and is applied to server bind/probe and memtier
targeting for consistent runs.
`SERVER_CPU_SET` / `CLIENT_CPU_SET` now default to an automatic split from
`nproc` when not specified, and non-root runs auto-prefix `perf record` with
`sudo` if available.
The run now fails fast on memtier server-error output (`handle error response:`)
so storage-capacity faults are not treated as valid profiles.
For `garnet`, benchmark runs default to
`GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES=262144` (override via env) to avoid
capacity-induced benchmark corruption.

### Dockerized Linux `perf` profiling (macOS-friendly)

If you are on macOS (or want a hermetic Linux runner), use
`docker_linux_perf_diff_profile.sh`. It starts a Linux container, installs
`perf` + `memtier_benchmark`, downloads Dragonfly release binaries, and then
runs `linux_perf_diff_profile.sh` inside that container.
The wrapper isolates container build output with a container-local
`CARGO_TARGET_DIR`, so host `target/` binaries are not overwritten.

```bash
cd .
chmod +x benches/docker_linux_perf_diff_profile.sh
THREADS=8 CONNS=16 REQUESTS=5000 \
  ./benches/docker_linux_perf_diff_profile.sh
```

Optional Garnet tuning env vars are forwarded into the container, including:

- `GARNET_TSAVORITE_STRING_STORE_SHARDS`
- `GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES`
- `GARNET_STRING_OWNER_THREADS`
- `GARNET_OWNER_THREAD_PINNING`
- `GARNET_OWNER_THREAD_CPU_SET`
- `GARNET_OWNER_EXECUTION_INLINE`

`GARNET_OWNER_EXECUTION_INLINE` defaults to inline owner execution in normal
server startup. Set it to `0` only when intentionally measuring the older
cross-thread owner-pool path.

Outputs are still written under `benches/results/` on the host.
Latest published differential analysis:
`docs/performance/linux-perf-diff-docker-2026-02-20.md`.

For cleaner framegraph generation, `linux_perf_diff_profile.sh` now stores
`perf` warnings separately:

- `perf-report-<mode>.stderr.log`
- `perf-script-<mode>.stderr.log`

The corresponding `perf-report-<mode>.txt` and `perf-script-<mode>.txt` files
remain parse-clean for stack-collapsing tools.

### Dockerized Linux `perf` median-of-N wrapper

Use `linux_perf_diff_profile_median_local.sh` to run the Dockerized differential
profile repeatedly and aggregate median metrics for each
`{target, workload}` pair. This is the recommended path when run-to-run
variance is high.

```bash
cd .
chmod +x benches/linux_perf_diff_profile_median_local.sh
RUNS=3 THREADS=8 CONNS=16 REQUESTS=5000 \
  ./benches/linux_perf_diff_profile_median_local.sh
```

Outputs under `benches/results/linux-perf-diff-median-<timestamp>/`:

- `run-<n>/...` (full per-run artifacts from Docker differential runs)
- `runs.csv` (all parsed run metrics)
- `median_summary.csv` (median ops/latency per target/workload)
- `summary.txt` (quick ratio summary)

## Linux NIC Affinity + RFS Setup

Use `linux_nic_affinity_setup.sh` on Linux hosts to align NIC IRQ handling
with server CPU placement and optionally enable RFS knobs.

```bash
cd .
chmod +x benches/linux_nic_affinity_setup.sh

# inspect current state
./benches/linux_nic_affinity_setup.sh --iface eth0 --mode show

# apply IRQ pinning + RFS
sudo ./benches/linux_nic_affinity_setup.sh \
  --iface eth0 \
  --mode apply \
  --server-cpus 0-3 \
  --enable-rfs 1 \
  --rfs-entries 65536
```

Optional apply knobs:

- `--rps-cpumask <hex>` writes `rx-*/rps_cpus`
- `--xps-cpumask <hex>` writes `tx-*/xps_cpus`
- `--dry-run 1` previews without writes

The script writes an apply log to `/tmp/garnet-nic-affinity-<iface>-<timestamp>.log`
by default.

## Allocator A/B (default vs mimalloc)

`allocator_ab_local.sh` builds two `garnet-server` binaries (default allocator
and `mimalloc` feature-enabled), runs the same median gate for both, and writes
a delta summary.

```bash
cd .
chmod +x benches/allocator_ab_local.sh
RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 \
./benches/allocator_ab_local.sh
```

Outputs under `/tmp/garnet-allocator-ab-<timestamp>/`:

- `default/summary.txt`
- `mimalloc/summary.txt`
- `comparison.txt` (ops/p99 deltas)

## Binary A/B (local)

Use `binary_ab_local.sh` to compare two arbitrary `garnet-server` binaries
under the same median gate harness.

```bash
cd .
chmod +x benches/binary_ab_local.sh
BASE_BIN=/tmp/garnet-server-base \
NEW_BIN=/tmp/garnet-server-new \
RUNS=3 THREADS=8 CONNS=16 REQUESTS=5000 \
./benches/binary_ab_local.sh
```

The script reuses `perf_regression_gate_local.sh` for each binary and writes:

- `<outdir>/<label>/runs.csv`
- `<outdir>/<label>/summary.txt`
- `<outdir>/comparison.txt` (median ops/p99 deltas)
