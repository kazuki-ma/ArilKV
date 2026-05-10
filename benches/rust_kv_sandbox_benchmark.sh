#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_ROOT="${RUST_KV_BENCH_RESULTS_DIR:-${SCRIPT_DIR}/results/rust-kv-sandbox}"
RUN_ID="${RUST_KV_BENCH_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${RESULTS_ROOT}/${RUN_ID}"
WORK_DIR="${RUN_DIR}/build-context"

LUX_REPO="${LUX_REPO:-https://github.com/lux-db/lux.git}"
LUX_REF="${LUX_REF:-8adc0e72cf177239616cc2922f1ee72ed55a31b0}"
FEOX_REPO="${FEOX_REPO:-https://github.com/mehrantsi/feox-server.git}"
FEOX_REF="${FEOX_REF:-8871393633109255eafbd404ebaa25b77bd5ddc6}"

REQUESTS="${REQUESTS:-1000000}"
CLIENTS="${CLIENTS:-50}"
PIPELINES="${PIPELINES:-1,64}"
TESTS="${TESTS:-set,get}"
DATA_SIZE="${DATA_SIZE:-32}"
KEYSPACE="${KEYSPACE:-1000000}"
SERVER_THREADS="${SERVER_THREADS:-}"
SERVER_MEMORY="${SERVER_MEMORY:-4g}"
SERVER_CPUS="${SERVER_CPUS:-}"
SERVER_CPUSET="${SERVER_CPUSET:-}"
ARILKV_TOKIO_WORKER_THREADS="${ARILKV_TOKIO_WORKER_THREADS:-${TOKIO_WORKER_THREADS:-}}"
BENCH_MEMORY="${BENCH_MEMORY:-2g}"
BENCH_CPUSET="${BENCH_CPUSET:-}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7-alpine}"
NETWORK_NAME="${NETWORK_NAME:-arilkv-rust-kv-bench-${RUN_ID}}"
TARGETS="${TARGETS:-arilkv,lux,feox}"
SKIP_BUILD="${SKIP_BUILD:-0}"
RESTART_BETWEEN_BENCHMARKS="${RESTART_BETWEEN_BENCHMARKS:-0}"

ARILKV_IMAGE="${ARILKV_IMAGE:-arilkv-bench:${RUN_ID}}"
LUX_IMAGE="${LUX_IMAGE:-lux-bench:${LUX_REF:0:12}}"
FEOX_IMAGE="${FEOX_IMAGE:-feox-bench:${FEOX_REF:0:12}}"

mkdir -p "${WORK_DIR}"

log() {
    printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

csv_escape() {
    local value="$1"
    printf '"%s"' "${value//\"/\"\"}"
}

image_for_target() {
    case "$1" in
        arilkv) echo "${ARILKV_IMAGE}" ;;
        lux) echo "${LUX_IMAGE}" ;;
        feox) echo "${FEOX_IMAGE}" ;;
        *) return 1 ;;
    esac
}

entrypoint_for_target() {
    case "$1" in
        arilkv)
            echo "/usr/local/bin/garnet-server"
            ;;
        lux)
            echo "/usr/local/bin/lux"
            ;;
        feox)
            if [[ -n "${SERVER_THREADS}" ]]; then
                echo "/usr/local/bin/feox-server --bind 0.0.0.0 --port 6379 --threads ${SERVER_THREADS} --log-level warn"
            else
                echo "/usr/local/bin/feox-server --bind 0.0.0.0 --port 6379 --log-level warn"
            fi
            ;;
        *)
            return 1
            ;;
    esac
}

build_arilkv_image() {
    log "building ArilKV image ${ARILKV_IMAGE}"
    docker build \
        -f "${REPO_ROOT}/benches/Dockerfile.garnet-rs-cachebench" \
        -t "${ARILKV_IMAGE}" \
        "${REPO_ROOT}" \
        2>&1 | tee "${RUN_DIR}/build-arilkv.log"
}

write_lux_dockerfile() {
    cat >"${WORK_DIR}/Dockerfile.lux" <<EOF
FROM rust:1.95-slim AS builder
ARG LUX_REPO
ARG LUX_REF
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \\
    && apt-get install -y --no-install-recommends git ca-certificates pkg-config build-essential \\
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone "\${LUX_REPO}" lux \\
    && cd lux \\
    && git checkout "\${LUX_REF}" \\
    && cargo build --release

FROM debian:trixie-slim
RUN groupadd --system app \\
    && useradd --system --gid app --home-dir /nonexistent --shell /usr/sbin/nologin app
COPY --from=builder /src/lux/target/release/lux /usr/local/bin/lux
USER app
ENTRYPOINT ["/usr/local/bin/lux"]
EOF
}

write_feox_dockerfile() {
    cat >"${WORK_DIR}/Dockerfile.feox" <<EOF
FROM rust:1.95-slim AS builder
ARG FEOX_REPO
ARG FEOX_REF
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \\
    && apt-get install -y --no-install-recommends git ca-certificates pkg-config build-essential \\
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone "\${FEOX_REPO}" feox-server \\
    && cd feox-server \\
    && git checkout "\${FEOX_REF}" \\
    && cargo build --release

FROM debian:trixie-slim
RUN groupadd --system app \\
    && useradd --system --gid app --home-dir /nonexistent --shell /usr/sbin/nologin app
COPY --from=builder /src/feox-server/target/release/feox-server /usr/local/bin/feox-server
USER app
ENTRYPOINT ["/usr/local/bin/feox-server"]
EOF
}

build_lux_image() {
    write_lux_dockerfile
    log "building Lux image ${LUX_IMAGE} from ${LUX_REF}"
    docker build \
        --build-arg "LUX_REPO=${LUX_REPO}" \
        --build-arg "LUX_REF=${LUX_REF}" \
        -f "${WORK_DIR}/Dockerfile.lux" \
        -t "${LUX_IMAGE}" \
        "${WORK_DIR}" \
        2>&1 | tee "${RUN_DIR}/build-lux.log"
}

build_feox_image() {
    write_feox_dockerfile
    log "building FeOx image ${FEOX_IMAGE} from ${FEOX_REF}"
    docker build \
        --build-arg "FEOX_REPO=${FEOX_REPO}" \
        --build-arg "FEOX_REF=${FEOX_REF}" \
        -f "${WORK_DIR}/Dockerfile.feox" \
        -t "${FEOX_IMAGE}" \
        "${WORK_DIR}" \
        2>&1 | tee "${RUN_DIR}/build-feox.log"
}

prepare_images() {
    if [[ "${SKIP_BUILD}" == "1" ]]; then
        log "SKIP_BUILD=1; using existing images"
        return
    fi
    build_arilkv_image
    build_lux_image
    build_feox_image
    docker pull "${REDIS_IMAGE}" >/dev/null
}

cleanup() {
    docker rm -f "${SERVER_CONTAINER:-}" >/dev/null 2>&1 || true
    docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

create_network() {
    docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
    docker network create --internal "${NETWORK_NAME}" >/dev/null
}

wait_for_server() {
    local attempts=120
    for _ in $(seq 1 "${attempts}"); do
        if docker run --rm --network "${NETWORK_NAME}" --cap-drop ALL --security-opt no-new-privileges "${REDIS_IMAGE}" \
            timeout 2 redis-cli -h server -p 6379 PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    return 1
}

run_server() {
    local target="$1"
    local image
    local entrypoint
    image="$(image_for_target "${target}")"
    entrypoint="$(entrypoint_for_target "${target}")"
    SERVER_CONTAINER="rust-kv-${target}-${RUN_ID}"
    docker rm -f "${SERVER_CONTAINER}" >/dev/null 2>&1 || true

    local args=(
        -d
        --name "${SERVER_CONTAINER}"
        --network "${NETWORK_NAME}"
        --network-alias server
        --read-only
        --tmpfs /tmp:rw,noexec,nosuid,size=128m
        --cap-drop ALL
        --security-opt no-new-privileges
        --pids-limit 1024
        --memory "${SERVER_MEMORY}"
    )
    if [[ -n "${SERVER_CPUS}" ]]; then
        args+=(--cpus "${SERVER_CPUS}")
    fi
    if [[ -n "${SERVER_CPUSET}" ]]; then
        args+=(--cpuset-cpus "${SERVER_CPUSET}")
    fi

    case "${target}" in
        arilkv)
            args+=(
                -e GARNET_BIND_ADDR=0.0.0.0:6379
                -e RUST_BACKTRACE=0
            )
            if [[ -n "${ARILKV_TOKIO_WORKER_THREADS}" ]]; then
                args+=(-e "TOKIO_WORKER_THREADS=${ARILKV_TOKIO_WORKER_THREADS}")
            fi
            ;;
        lux)
            args+=(-e RUST_BACKTRACE=0)
            ;;
        feox)
            args+=(-e RUST_BACKTRACE=0)
            ;;
    esac

    log "starting ${target}"
    # shell form allows FeOx arguments while keeping image entrypoints simple.
    docker run "${args[@]}" --entrypoint /bin/sh "${image}" -c "${entrypoint}" >/dev/null
    if ! wait_for_server; then
        docker logs "${SERVER_CONTAINER}" >"${RUN_DIR}/${target}-server-startup.log" 2>&1 || true
        echo "server ${target} did not become ready; see ${RUN_DIR}/${target}-server-startup.log" >&2
        return 1
    fi
}

stop_server() {
    local target="$1"
    docker logs "${SERVER_CONTAINER}" >"${RUN_DIR}/${target}-server.log" 2>&1 || true
    docker rm -f "${SERVER_CONTAINER}" >/dev/null 2>&1 || true
    SERVER_CONTAINER=""
}

parse_rps() {
    awk -F, '
        NR == 2 {
            gsub(/"/, "", $2)
            gsub(/^[ \t]+|[ \t]+$/, "", $2)
            print $2
            exit
        }
    ' "$1"
}

run_one_benchmark() {
    local target="$1"
    local pipeline="$2"
    local test="$3"
    local raw_file="${RUN_DIR}/${target}-p${pipeline}-${test}.csv"
    log "benchmark ${target}: test=${test} pipeline=${pipeline}"
    local bench_args=(
        --rm
        --network "${NETWORK_NAME}" \
        --cap-drop ALL \
        --security-opt no-new-privileges \
        --memory "${BENCH_MEMORY}" \
    )
    if [[ -n "${BENCH_CPUSET}" ]]; then
        bench_args+=(--cpuset-cpus "${BENCH_CPUSET}")
    fi

    docker run "${bench_args[@]}" \
        "${REDIS_IMAGE}" \
        redis-benchmark \
            -h server \
            -p 6379 \
            -n "${REQUESTS}" \
            -c "${CLIENTS}" \
            -P "${pipeline}" \
            -d "${DATA_SIZE}" \
            -r "${KEYSPACE}" \
            -t "${test}" \
            --csv \
        >"${raw_file}"

    local rps
    rps="$(parse_rps "${raw_file}")"
    if [[ -z "${rps}" || "${rps}" == "0" ]]; then
        echo "failed to parse non-zero rps from ${raw_file}" >&2
        return 1
    fi
    {
        csv_escape "${target}"
        printf ','
        csv_escape "${test}"
        printf ',%s,%s,%s,%s,%s\n' "${pipeline}" "${REQUESTS}" "${CLIENTS}" "${DATA_SIZE}" "${rps}"
    } >>"${RUN_DIR}/summary.csv"
}

run_target() {
    local target="$1"
    local pipeline test
    IFS=',' read -r -a pipeline_values <<<"${PIPELINES}"
    IFS=',' read -r -a test_values <<<"${TESTS}"
    if [[ "${RESTART_BETWEEN_BENCHMARKS}" != "1" ]]; then
        run_server "${target}"
    fi
    for pipeline in "${pipeline_values[@]}"; do
        for test in "${test_values[@]}"; do
            if [[ "${RESTART_BETWEEN_BENCHMARKS}" == "1" ]]; then
                run_server "${target}"
            fi
            run_one_benchmark "${target}" "${pipeline}" "${test}"
            if [[ "${RESTART_BETWEEN_BENCHMARKS}" == "1" ]]; then
                stop_server "${target}"
            fi
        done
    done
    if [[ "${RESTART_BETWEEN_BENCHMARKS}" != "1" ]]; then
        stop_server "${target}"
    fi
}

write_metadata() {
    {
        echo "run_id=${RUN_ID}"
        echo "date=$(date -Iseconds)"
        echo "repo_root=${REPO_ROOT}"
        echo "arilkv_commit=$(git -C "${REPO_ROOT}" rev-parse HEAD)"
        echo "lux_repo=${LUX_REPO}"
        echo "lux_ref=${LUX_REF}"
        echo "feox_repo=${FEOX_REPO}"
        echo "feox_ref=${FEOX_REF}"
        echo "requests=${REQUESTS}"
        echo "clients=${CLIENTS}"
        echo "pipelines=${PIPELINES}"
        echo "tests=${TESTS}"
        echo "data_size=${DATA_SIZE}"
        echo "keyspace=${KEYSPACE}"
        echo "server_threads=${SERVER_THREADS:-default}"
        echo "server_memory=${SERVER_MEMORY}"
        echo "server_cpus=${SERVER_CPUS:-unlimited}"
        echo "server_cpuset=${SERVER_CPUSET:-unconstrained}"
        echo "arilkv_tokio_worker_threads=${ARILKV_TOKIO_WORKER_THREADS:-default}"
        echo "bench_memory=${BENCH_MEMORY}"
        echo "bench_cpuset=${BENCH_CPUSET:-unconstrained}"
        echo "restart_between_benchmarks=${RESTART_BETWEEN_BENCHMARKS}"
        echo "runtime_sandbox=dedicated internal Docker bridge; no host ports; cap-drop=ALL; no-new-privileges; read-only server rootfs; tmpfs /tmp; non-root images"
    } >"${RUN_DIR}/metadata.txt"
}

write_report() {
    {
        echo "# Rust KV sandbox benchmark"
        echo
        echo '```'
        cat "${RUN_DIR}/metadata.txt"
        echo '```'
        echo
        echo "| Target | Command | Pipeline | Requests | Clients | Data bytes | Requests/sec |"
        echo "|---|---:|---:|---:|---:|---:|---:|"
        tail -n +2 "${RUN_DIR}/summary.csv" | while IFS=, read -r target test pipeline requests clients data_size rps; do
            target="${target%\"}"
            target="${target#\"}"
            test="${test%\"}"
            test="${test#\"}"
            printf "| %s | %s | %s | %s | %s | %s | %s |\n" \
                "${target}" "${test}" "${pipeline}" "${requests}" "${clients}" "${data_size}" "${rps}"
        done
    } >"${RUN_DIR}/REPORT.md"
}

main() {
    write_metadata
    echo 'target,test,pipeline,requests,clients,data_size,rps' >"${RUN_DIR}/summary.csv"
    prepare_images
    create_network

    IFS=',' read -r -a targets <<<"${TARGETS}"
    for target in "${targets[@]}"; do
        run_target "${target}"
    done
    write_report
    log "saved summary: ${RUN_DIR}/summary.csv"
    log "saved report: ${RUN_DIR}/REPORT.md"
}

main "$@"
