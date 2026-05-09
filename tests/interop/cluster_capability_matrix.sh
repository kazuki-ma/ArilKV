#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}"

RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/cluster-capability-$(date +%Y%m%d-%H%M%S)}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
DRAGONFLY_IMAGE="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.36.0}"
REDIS_CLI_IMAGE="${REDIS_CLI_IMAGE:-redis:7.2-alpine}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-rustup run 1.95.0 cargo run -p garnet-server --release}"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
GARNET_BASE_PORT="${GARNET_BASE_PORT:-7420}"

mkdir -p "${RESULT_DIR}"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"
echo "test,status,details" > "${SUMMARY_CSV}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

require_cmd docker
require_cmd redis-cli
require_cmd grep
require_cmd awk
require_cmd sed

record_result() {
    local test_name="$1"
    local status="$2"
    local details="$3"
    local csv_details
    csv_details="$(echo "${details}" | tr ',' ';' | tr '\n' ' ' | sed -e 's/  */ /g')"
    echo "${test_name},${status},${csv_details}" >> "${SUMMARY_CSV}"
}

wait_for_ping() {
    local host="$1"
    local port="$2"
    for _ in $(seq 1 1200); do
        if redis-cli -h "${host}" -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

cleanup_docker_nodes() {
    for name in "$@"; do
        docker rm -f "${name}" >/dev/null 2>&1 || true
    done
}

test_redis_cluster_create() {
    local log_file="${RESULT_DIR}/redis-cluster-create.log"
    local net="interop-redis-${RANDOM}"
    local status="FAIL"
    local details="redis cluster create failed"

    if ! docker network create "${net}" >"${log_file}" 2>&1; then
        record_result "redis_cluster_create" "${status}" "failed to create docker network; log=${log_file}"
        return 0
    fi

    for n in 1 2 3; do
        if ! docker run -d --rm --name "interop-r${n}" --network "${net}" "${REDIS_IMAGE}" \
            redis-server --port 6379 --cluster-enabled yes \
            --cluster-config-file nodes.conf --cluster-node-timeout 5000 \
            --appendonly no --save "" >>"${log_file}" 2>&1; then
            details="failed to start redis node interop-r${n}"
            cleanup_docker_nodes interop-r1 interop-r2 interop-r3
            docker network rm "${net}" >/dev/null 2>&1 || true
            record_result "redis_cluster_create" "${status}" "${details}; log=${log_file}"
            return 0
        fi
    done

    sleep 2
    if ! docker run --rm --network "${net}" "${REDIS_CLI_IMAGE}" redis-cli --cluster create \
        interop-r1:6379 interop-r2:6379 interop-r3:6379 \
        --cluster-replicas 0 --cluster-yes >>"${log_file}" 2>&1; then
        details="redis-cli cluster create returned non-zero"
        cleanup_docker_nodes interop-r1 interop-r2 interop-r3
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_cluster_create" "${status}" "${details}; log=${log_file}"
        return 0
    fi

    for _ in $(seq 1 30); do
        local info
        info="$(docker run --rm --network "${net}" "${REDIS_CLI_IMAGE}" redis-cli -h interop-r1 -p 6379 CLUSTER INFO 2>>"${log_file}" || true)"
        if echo "${info}" | grep -q "cluster_state:ok"; then
            status="PASS"
            details="cluster_state:ok and 3-node slot coverage confirmed"
            break
        fi
        sleep 0.3
    done
    if [[ "${status}" != "PASS" ]]; then
        details="cluster_state did not converge to ok"
    fi

    cleanup_docker_nodes interop-r1 interop-r2 interop-r3
    docker network rm "${net}" >/dev/null 2>&1 || true
    record_result "redis_cluster_create" "${status}" "${details}; log=${log_file}"
}

test_dragonfly_cluster_modes() {
    local emu_log="${RESULT_DIR}/dragonfly-emulated.log"
    local yes_log="${RESULT_DIR}/dragonfly-cluster-yes.log"
    local emu_status="FAIL"
    local emu_details="cluster_mode=emulated check failed"

    local emu_cid
    emu_cid="$(docker run -d --rm -p 7435:6379 "${DRAGONFLY_IMAGE}" --port=6379 --cluster_mode=emulated 2>>"${emu_log}" || true)"
    if [[ -n "${emu_cid}" ]]; then
        echo "${emu_cid}" >>"${emu_log}"
    fi
    if [[ -n "${emu_cid}" ]] && wait_for_ping 127.0.0.1 7435; then
        local emu_info
        emu_info="$(redis-cli -h 127.0.0.1 -p 7435 CLUSTER INFO 2>>"${emu_log}" || true)"
        if echo "${emu_info}" | grep -q "cluster_state:ok" && echo "${emu_info}" | grep -q "cluster_known_nodes:1"; then
            emu_status="PASS"
            emu_details="cluster_mode=emulated exposes 1-node Redis cluster view"
        else
            emu_details="unexpected CLUSTER INFO in emulated mode"
        fi
    else
        emu_details="dragonfly emulated node did not become ready"
    fi
    if [[ -n "${emu_cid}" ]]; then
        docker kill "${emu_cid}" >/dev/null 2>&1 || true
    fi
    record_result "dragonfly_cluster_mode_emulated" "${emu_status}" "${emu_details}; log=${emu_log}"

    local net="interop-df-${RANDOM}"
    local yes_status="FAIL"
    local yes_details="cluster_mode=yes behavior unknown"
    local create_output=""

    if ! docker network create "${net}" >"${yes_log}" 2>&1; then
        record_result "dragonfly_cluster_mode_yes_create" "${yes_status}" "failed to create docker network; log=${yes_log}"
        return 0
    fi

    for n in 1 2 3; do
        if ! docker run -d --rm --name "interop-df${n}" --network "${net}" "${DRAGONFLY_IMAGE}" \
            --cluster_mode=yes --cluster_announce_ip="interop-df${n}" >>"${yes_log}" 2>&1; then
            yes_details="failed to start dragonfly node interop-df${n}"
            cleanup_docker_nodes interop-df1 interop-df2 interop-df3
            docker network rm "${net}" >/dev/null 2>&1 || true
            record_result "dragonfly_cluster_mode_yes_create" "${yes_status}" "${yes_details}; log=${yes_log}"
            return 0
        fi
    done

    sleep 2
    set +e
    create_output="$(docker run --rm --network "${net}" "${REDIS_CLI_IMAGE}" redis-cli --cluster create \
        interop-df1:6379 interop-df2:6379 interop-df3:6379 \
        --cluster-replicas 0 --cluster-yes 2>&1)"
    local create_rc=$?
    set -e
    echo "${create_output}" >>"${yes_log}"

    if [[ "${create_rc}" -eq 0 ]]; then
        yes_status="PASS"
        yes_details="multi-node bootstrap succeeded"
    else
        if echo "${create_output}" | grep -q "Cluster is not yet configured"; then
            yes_status="UNSUPPORTED"
            yes_details="multi-node bootstrap via redis-cli cluster create is not available"
        else
            yes_status="FAIL"
            yes_details="redis-cli cluster create failed with unexpected output"
        fi
    fi

    cleanup_docker_nodes interop-df1 interop-df2 interop-df3
    docker network rm "${net}" >/dev/null 2>&1 || true
    record_result "dragonfly_cluster_mode_yes_create" "${yes_status}" "${yes_details}; log=${yes_log}"
}

test_garnet_multiport_cluster_surface() {
    local log_file="${RESULT_DIR}/garnet-multiport.log"
    local base_port="${GARNET_BASE_PORT}"
    local status="FAIL"
    local details="garnet multi-port routing check failed"

    (
        cd "${GARNET_RS_ROOT}"
        GARNET_BIND_ADDR="127.0.0.1:${base_port}" \
        RUST_BACKTRACE="${RUST_BACKTRACE}" \
        GARNET_OWNER_NODE_COUNT=3 \
        GARNET_MULTI_PORT_CLUSTER_MODE=1 \
        bash -lc "${GARNET_SERVER_CMD}"
    ) >"${log_file}" 2>&1 &
    local garnet_pid="$!"

    local ready=true
    for p in "${base_port}" "$((base_port + 1))" "$((base_port + 2))"; do
        if ! wait_for_ping 127.0.0.1 "${p}"; then
            ready=false
            details="garnet port ${p} did not become ready"
            break
        fi
    done

    if [[ "${ready}" == true ]]; then
        local cluster_info
        cluster_info="$(redis-cli -h 127.0.0.1 -p "${base_port}" CLUSTER INFO 2>&1 || true)"
        if ! echo "${cluster_info}" | grep -q "ERR unknown command"; then
            details="unexpected CLUSTER INFO response: ${cluster_info}"
        else
            local moved=""
            local moved_key=""
            for i in $(seq 1 600); do
                local candidate="interop-key-${i}"
                local out
                out="$(redis-cli -h 127.0.0.1 -p "${base_port}" GET "${candidate}" 2>&1 || true)"
                if echo "${out}" | grep -q "MOVED"; then
                    moved="${out}"
                    moved_key="${candidate}"
                    break
                fi
            done

            if [[ -n "${moved}" ]]; then
                status="PASS"
                details="internal MOVED routing works (example key=${moved_key}); CLUSTER command surface is not implemented"
                {
                    echo "moved_key=${moved_key}"
                    echo "moved_response=${moved}"
                } >>"${log_file}"
            else
                details="did not observe MOVED response from base port"
            fi
        fi
    fi

    kill "${garnet_pid}" >/dev/null 2>&1 || true
    wait "${garnet_pid}" >/dev/null 2>&1 || true
    record_result "garnet_multiport_routing_surface" "${status}" "${details}; log=${log_file}"
}

test_redis_cluster_create
test_dragonfly_cluster_modes
test_garnet_multiport_cluster_surface

echo "cluster capability summary:"
if command -v column >/dev/null 2>&1; then
    column -t -s, "${SUMMARY_CSV}"
else
    cat "${SUMMARY_CSV}"
fi
echo "result_dir=${RESULT_DIR}"
