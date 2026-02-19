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
