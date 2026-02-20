# Docker Troubleshooting (Local/macOS)

This runbook is for local benchmark/profiling work in this repository when
Docker Desktop is unstable.

This environment does not run GitHub Actions, so all steps below assume local
execution.

## 1. Quick health check

```bash
docker context ls
docker info --format '{{.ServerVersion}} {{.OperatingSystem}}'
docker run --rm alpine:3.20 sh -c 'echo docker-ok'
```

If `docker info` fails with:

`Cannot connect to the Docker daemon at unix:///Users/<user>/.docker/run/docker.sock`

continue with restart/recovery.

## 2. Soft restart

```bash
osascript -e 'quit app "Docker"'
open -a Docker
for i in $(seq 1 120); do
  docker info --format '{{.ServerVersion}} {{.OperatingSystem}}' >/dev/null 2>&1 && break
  sleep 1
done
docker info --format '{{.ServerVersion}} {{.OperatingSystem}}'
```

## 3. Hard restart (backend reset)

```bash
pkill -f com.docker.backend || true
pkill -f "Docker Desktop" || true
sleep 2
open -a Docker
for i in $(seq 1 120); do
  docker info --format '{{.ServerVersion}} {{.OperatingSystem}}' >/dev/null 2>&1 && break
  sleep 1
done
docker info --format '{{.ServerVersion}} {{.OperatingSystem}}'
```

## 4. Verify socket and listener

```bash
ls -la ~/.docker/run
lsof -U | rg 'docker.sock|com.docker' || true
pgrep -laf 'Docker|com.docker.backend|vpnkit|hyperkit'
```

If `docker.sock` exists but no listener appears for it, inspect logs.

## 5. Logs to inspect

```bash
tail -n 200 ~/Library/Containers/com.docker.docker/Data/log/host/com.docker.backend.log
tail -n 200 ~/Library/Containers/com.docker.docker/Data/log/host/Docker.log
tail -n 200 ~/Library/Containers/com.docker.docker/Data/log/vm/01-docker.log
```

Known noisy signature seen in this environment:

- `expected 1 nodes but found 8` (Kubernetes-related backend loop)

If this repeats while daemon stays unavailable, disable/reset Kubernetes in
Docker Desktop settings and retry.

## 6. Benchmark fallback while Docker is down

Use non-Docker local scripts where possible:

- `garnet-rs/benches/redis_official_benchmark.sh`
- `garnet-rs/benches/perf_regression_gate_local.sh`
- `garnet-rs/benches/local_hotspot_framegraph_macos.sh`

Docker-dependent comparison paths should be retried only after daemon health
checks pass.
