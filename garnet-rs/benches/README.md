# Benchmarks

## Official Redis benchmark (`redis-benchmark`)

`redis_official_benchmark.sh` runs Redis official `redis-benchmark` against Garnet.

### What it does

1. Resolves `redis-benchmark` from `PATH` when available.
2. Otherwise downloads Redis official source from `https://download.redis.io/redis-stable.tar.gz`.
3. Builds only `redis-benchmark`.
4. Starts `garnet-server` (optional).
5. Runs benchmark and writes a timestamped report under `garnet-rs/benches/results/`.

### Quick start

```bash
cd garnet-rs
REDIS_BENCH_REQUESTS=10000 REDIS_BENCH_CLIENTS=20 \
  ./benches/redis_official_benchmark.sh
```

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

For fiber-free owner-thread routing experiments, set
`GARNET_STRING_OWNER_THREADS=<n>` (optional, disabled by default). This routes
single-key string commands through shard-owner threads while keeping the Tokio
network loop unchanged.

### Minimal run (single benchmark)

```bash
chmod +x garnet-rs/benches/cache_benchmarks_garnet_wrapper.sh
cd /tmp/cache-benchmarks-20260219
make
cat > /tmp/cache-benchmarks-garnet-rs.json <<'JSON'
{
  "paths": {
    "memtier": "/opt/homebrew/bin/memtier_benchmark",
    "garnet": "/absolute/path/to/garnet-rs/benches/cache_benchmarks_garnet_wrapper.sh"
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
chmod +x garnet-rs/benches/cache_benchmarks_dragonfly_docker_wrapper.sh \
  garnet-rs/benches/cache_benchmarks_garnet_docker_wrapper.sh
docker build -f garnet-rs/benches/Dockerfile.garnet-rs-cachebench \
  -t garnet-rs-cachebench:latest garnet-rs
cat > /tmp/cache-benchmarks-compare.json <<'JSON'
{
  "paths": {
    "memtier": "/opt/homebrew/bin/memtier_benchmark",
    "garnet": "/absolute/path/to/garnet-rs/benches/cache_benchmarks_garnet_docker_wrapper.sh",
    "dragonfly": "/absolute/path/to/garnet-rs/benches/cache_benchmarks_dragonfly_docker_wrapper.sh"
  }
}
JSON
```

For investigation planning and unknown-item closure, see:
`garnet-rs/benches/DEEPRESEARCH_DRAGONFLY_INSTRUCTION.md`.

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

Sampling now stops when the memtier run finishes, so captured stacks stay focused
on active workload time instead of idle tail time.

```bash
cd garnet-rs
chmod +x benches/local_hotspot_framegraph_macos.sh
./benches/local_hotspot_framegraph_macos.sh
```

Compare shard settings under the same workload:

```bash
cd garnet-rs
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
cd garnet-rs
chmod +x benches/sweep_string_store_shards_local.sh
SHARD_COUNTS=\"1 2 4 8 16\" REQUESTS=20000 \
  ./benches/sweep_string_store_shards_local.sh
```

The script prints CSV and validates each run from memtier summary lines
(`Threads`, `Connections per thread`, `Requests per client`, and non-zero
`Ops/sec`) so failed/partial runs are not treated as success.

## String-store shard policy matrix (local)

Use `sweep_string_store_policy_matrix_local.sh` to run a workload matrix over
shard counts and owner-thread modes, then emit recommendation artifacts.

```bash
cd garnet-rs
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

```bash
cd garnet-rs
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

## Linux Differential Profiling (`perf`)

Use `linux_perf_diff_profile.sh` on a Linux host to capture `perf record`
profiles for `garnet` and `dragonfly` under matching memtier workload settings.

```bash
cd garnet-rs
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

## Allocator A/B (default vs mimalloc)

`allocator_ab_local.sh` builds two `garnet-server` binaries (default allocator
and `mimalloc` feature-enabled), runs the same median gate for both, and writes
a delta summary.

```bash
cd garnet-rs
chmod +x benches/allocator_ab_local.sh
RUNS=3 THREADS=4 CONNS=8 REQUESTS=5000 \
./benches/allocator_ab_local.sh
```

Outputs under `/tmp/garnet-allocator-ab-<timestamp>/`:

- `default/summary.txt`
- `mimalloc/summary.txt`
- `comparison.txt` (ops/p99 deltas)
