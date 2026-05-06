#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}/garnet-rs"

RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/replication-capability-$(date +%Y%m%d-%H%M%S)}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2-alpine}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-cargo run -p garnet-server --release}"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
REDIS_REPL_MASTER_HOST="${REDIS_REPL_MASTER_HOST:-host.docker.internal}"
PORT_BASE="${PORT_BASE:-7480}"

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
    for _ in $(seq 1 300); do
        if redis-cli -h "${host}" -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

stop_pid() {
    local pid="$1"
    if [[ -n "${pid}" ]]; then
        kill "${pid}" >/dev/null 2>&1 || true
        wait "${pid}" >/dev/null 2>&1 || true
    fi
}

cleanup_docker_nodes() {
    for name in "$@"; do
        docker rm -f "${name}" >/dev/null 2>&1 || true
    done
}

start_garnet() {
    local port="$1"
    local log_file="$2"
    (
        cd "${GARNET_RS_ROOT}"
        GARNET_BIND_ADDR="127.0.0.1:${port}" RUST_BACKTRACE="${RUST_BACKTRACE}" bash -lc "${GARNET_SERVER_CMD}"
    ) >"${log_file}" 2>&1 &
    echo "$!"
}

replication_info_field() {
    local port="$1"
    local key="$2"
    redis-cli -h 127.0.0.1 -p "${port}" INFO replication \
        | awk -F: -v k="${key}" '$1 == k {print $2}' \
        | tr -d '\r'
}

test_redis_to_redis_replication_master_switch() {
    local log_file="${RESULT_DIR}/redis-to-redis-master-switch.log"
    local suffix="${RANDOM}"
    local net="interop-repl-${suffix}"
    local master_name="repl-rm-${suffix}"
    local replica_name="repl-rr-${suffix}"
    local master_port=$((PORT_BASE + 0))
    local replica_port=$((PORT_BASE + 1))
    local status="FAIL"
    local details="redis->redis replication with master switch failed"

    if ! docker network create "${net}" >"${log_file}" 2>&1; then
        record_result "redis_to_redis_replication_master_switch" "${status}" "failed to create docker network; log=${log_file}"
        return 0
    fi

    if ! docker run -d --rm --name "${master_name}" --network "${net}" -p "${master_port}:6379" "${REDIS_IMAGE}" \
        redis-server --port 6379 --save "" --appendonly no >>"${log_file}" 2>&1; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "failed to start redis master; log=${log_file}"
        return 0
    fi

    if ! docker run -d --rm --name "${replica_name}" --network "${net}" -p "${replica_port}:6379" "${REDIS_IMAGE}" \
        redis-server --port 6379 --save "" --appendonly no >>"${log_file}" 2>&1; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "failed to start redis replica; log=${log_file}"
        return 0
    fi

    if ! wait_for_ping 127.0.0.1 "${master_port}" || ! wait_for_ping 127.0.0.1 "${replica_port}"; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "master/replica did not become ready; log=${log_file}"
        return 0
    fi

    local repl_resp
    repl_resp="$(redis-cli -h 127.0.0.1 -p "${replica_port}" REPLICAOF "${master_name}" 6379 2>&1 || true)"
    echo "replicaof_response=${repl_resp}" >>"${log_file}"
    if ! echo "${repl_resp}" | grep -q "^OK"; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "REPLICAOF failed (${repl_resp}); log=${log_file}"
        return 0
    fi

    local link_up=false
    for _ in $(seq 1 80); do
        local link
        link="$(replication_info_field "${replica_port}" "master_link_status")"
        if [[ "${link}" == "up" ]]; then
            link_up=true
            break
        fi
        sleep 0.25
    done

    if [[ "${link_up}" != true ]]; then
        local link
        link="$(replication_info_field "${replica_port}" "master_link_status")"
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "phase1 master_link_status=${link:-unknown}; log=${log_file}"
        return 0
    fi

    redis-cli -h 127.0.0.1 -p "${master_port}" SET repl:probe:phase1 baseline >>"${log_file}" 2>&1 || true
    local replicated_phase1=""
    for _ in $(seq 1 40); do
        replicated_phase1="$(redis-cli -h 127.0.0.1 -p "${replica_port}" GET repl:probe:phase1 2>/dev/null || true)"
        if [[ "${replicated_phase1}" == "baseline" ]]; then
            status="PASS"
            details="phase1 replication established"
            break
        fi
        sleep 0.25
    done
    if [[ "${status}" != "PASS" ]]; then
        details="phase1 master_link_status=up but probe key was not replicated"
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "${details}; log=${log_file}"
        return 0
    fi

    local switch_resp
    switch_resp="$(redis-cli -h 127.0.0.1 -p "${replica_port}" REPLICAOF NO ONE 2>&1 || true)"
    echo "switch_step_promote_replica=${switch_resp}" >>"${log_file}"
    if ! echo "${switch_resp}" | grep -q "^OK"; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "failed to promote replica to master (${switch_resp}); log=${log_file}"
        return 0
    fi

    local promoted=false
    for _ in $(seq 1 40); do
        local role
        role="$(replication_info_field "${replica_port}" "role")"
        if [[ "${role}" == "master" ]]; then
            promoted=true
            break
        fi
        sleep 0.25
    done
    if [[ "${promoted}" != true ]]; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "promoted node role did not become master; log=${log_file}"
        return 0
    fi

    switch_resp="$(redis-cli -h 127.0.0.1 -p "${master_port}" REPLICAOF "${replica_name}" 6379 2>&1 || true)"
    echo "switch_step_demote_old_master=${switch_resp}" >>"${log_file}"
    if ! echo "${switch_resp}" | grep -q "^OK"; then
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "failed to reattach old master as replica (${switch_resp}); log=${log_file}"
        return 0
    fi

    local second_link_up=false
    for _ in $(seq 1 80); do
        local role link
        role="$(replication_info_field "${master_port}" "role")"
        link="$(replication_info_field "${master_port}" "master_link_status")"
        if [[ "${role}" == "slave" && "${link}" == "up" ]]; then
            second_link_up=true
            break
        fi
        sleep 0.25
    done
    if [[ "${second_link_up}" != true ]]; then
        local role link
        role="$(replication_info_field "${master_port}" "role")"
        link="$(replication_info_field "${master_port}" "master_link_status")"
        cleanup_docker_nodes "${master_name}" "${replica_name}"
        docker network rm "${net}" >/dev/null 2>&1 || true
        record_result "redis_to_redis_replication_master_switch" "${status}" "phase2 role/link not ready (role=${role:-unknown}, link=${link:-unknown}); log=${log_file}"
        return 0
    fi

    redis-cli -h 127.0.0.1 -p "${replica_port}" SET repl:probe:phase2 switched >>"${log_file}" 2>&1 || true
    local replicated_phase2=""
    for _ in $(seq 1 40); do
        replicated_phase2="$(redis-cli -h 127.0.0.1 -p "${master_port}" GET repl:probe:phase2 2>/dev/null || true)"
        if [[ "${replicated_phase2}" == "switched" ]]; then
            status="PASS"
            details="phase1+phase2 succeeded (master switch and SET/GET replication on both master states)"
            break
        fi
        sleep 0.25
    done
    if [[ "${replicated_phase2}" != "switched" ]]; then
        status="FAIL"
        details="phase2 link up but switched-master probe key was not replicated"
    fi

    cleanup_docker_nodes "${master_name}" "${replica_name}"
    docker network rm "${net}" >/dev/null 2>&1 || true
    record_result "redis_to_redis_replication_master_switch" "${status}" "${details}; log=${log_file}"
}

test_redis_primary_to_garnet_replica() {
    local log_file="${RESULT_DIR}/redis-primary-to-garnet-replica.log"
    local redis_name="repl-rm-${RANDOM}"
    local redis_port=$((PORT_BASE + 2))
    local garnet_port=$((PORT_BASE + 3))
    local status="FAIL"
    local details="redis primary -> garnet replica check failed"
    local garnet_pid=""

    if ! docker run -d --rm --name "${redis_name}" -p "${redis_port}:6379" "${REDIS_IMAGE}" \
        redis-server --port 6379 --save "" --appendonly no >"${log_file}" 2>&1; then
        record_result "redis_primary_to_garnet_replica" "${status}" "failed to start redis primary; log=${log_file}"
        return 0
    fi

    garnet_pid="$(start_garnet "${garnet_port}" "${log_file}.garnet")"
    echo "garnet_pid=${garnet_pid}" >>"${log_file}"

    if ! wait_for_ping 127.0.0.1 "${redis_port}" || ! wait_for_ping 127.0.0.1 "${garnet_port}"; then
        stop_pid "${garnet_pid}"
        cleanup_docker_nodes "${redis_name}"
        record_result "redis_primary_to_garnet_replica" "${status}" "redis/garnet did not become ready; log=${log_file}"
        return 0
    fi

    local repl_resp
    repl_resp="$(redis-cli -h 127.0.0.1 -p "${garnet_port}" REPLICAOF 127.0.0.1 "${redis_port}" 2>&1 || true)"
    echo "replicaof_response=${repl_resp}" >>"${log_file}"

    if echo "${repl_resp}" | grep -qi "unknown command"; then
        status="UNSUPPORTED"
        details="garnet does not expose REPLICAOF command surface yet"
    elif echo "${repl_resp}" | grep -q "^OK"; then
        redis-cli -h 127.0.0.1 -p "${redis_port}" SET repl:probe:garnet-side from-redis > /dev/null 2>&1 || true
        local replicated=""
        for _ in $(seq 1 40); do
            replicated="$(redis-cli -h 127.0.0.1 -p "${garnet_port}" GET repl:probe:garnet-side 2>/dev/null || true)"
            if [[ "${replicated}" == "from-redis" ]]; then
                status="PASS"
                details="garnet accepted REPLICAOF and replicated SET/GET from redis master"
                break
            fi
            sleep 0.25
        done
        if [[ "${status}" != "PASS" ]]; then
            status="FAIL"
            details="REPLICAOF accepted by garnet but probe key not replicated"
        fi
    else
        status="FAIL"
        details="unexpected REPLICAOF response: ${repl_resp}"
    fi

    stop_pid "${garnet_pid}"
    cleanup_docker_nodes "${redis_name}"
    record_result "redis_primary_to_garnet_replica" "${status}" "${details}; log=${log_file}"
}

test_garnet_primary_to_redis_replica() {
    local log_file="${RESULT_DIR}/garnet-primary-to-redis-replica.log"
    local redis_name="repl-rr-${RANDOM}"
    local garnet_port=$((PORT_BASE + 4))
    local redis_port=$((PORT_BASE + 5))
    local status="FAIL"
    local details="garnet primary -> redis replica check failed"
    local garnet_pid=""

    garnet_pid="$(start_garnet "${garnet_port}" "${log_file}.garnet")"
    if ! docker run -d --rm --name "${redis_name}" -p "${redis_port}:6379" \
        --add-host=host.docker.internal:host-gateway \
        "${REDIS_IMAGE}" redis-server --port 6379 --save "" --appendonly no >"${log_file}" 2>&1; then
        stop_pid "${garnet_pid}"
        record_result "garnet_primary_to_redis_replica" "${status}" "failed to start redis replica; log=${log_file}"
        return 0
    fi

    if ! wait_for_ping 127.0.0.1 "${garnet_port}" || ! wait_for_ping 127.0.0.1 "${redis_port}"; then
        stop_pid "${garnet_pid}"
        cleanup_docker_nodes "${redis_name}"
        record_result "garnet_primary_to_redis_replica" "${status}" "garnet/redis did not become ready; log=${log_file}"
        return 0
    fi

    redis-cli -h 127.0.0.1 -p "${garnet_port}" SET repl:seed:redis-side seed-from-garnet > /dev/null 2>&1 || true

    local repl_resp
    repl_resp="$(redis-cli -h 127.0.0.1 -p "${redis_port}" REPLICAOF "${REDIS_REPL_MASTER_HOST}" "${garnet_port}" 2>&1 || true)"
    echo "replicaof_response=${repl_resp}" >>"${log_file}"
    if ! echo "${repl_resp}" | grep -q "^OK"; then
        stop_pid "${garnet_pid}"
        cleanup_docker_nodes "${redis_name}"
        record_result "garnet_primary_to_redis_replica" "${status}" "redis replica rejected REPLICAOF (${repl_resp}); log=${log_file}"
        return 0
    fi

    local link_up=false
    for _ in $(seq 1 80); do
        local link
        link="$(replication_info_field "${redis_port}" "master_link_status")"
        if [[ "${link}" == "up" ]]; then
            link_up=true
            break
        fi
        sleep 0.25
    done

    if [[ "${link_up}" != true ]]; then
        local role link master_host
        role="$(replication_info_field "${redis_port}" "role")"
        link="$(replication_info_field "${redis_port}" "master_link_status")"
        master_host="$(replication_info_field "${redis_port}" "master_host")"
        status="UNSUPPORTED"
        details="handshake did not reach master_link_status=up (role=${role:-unknown}, link=${link:-unknown}, master_host=${master_host:-unknown})"
        stop_pid "${garnet_pid}"
        cleanup_docker_nodes "${redis_name}"
        record_result "garnet_primary_to_redis_replica" "${status}" "${details}; log=${log_file}"
        return 0
    fi

    local seeded=""
    for _ in $(seq 1 40); do
        seeded="$(redis-cli -h 127.0.0.1 -p "${redis_port}" GET repl:seed:redis-side 2>/dev/null || true)"
        if [[ "${seeded}" == "seed-from-garnet" ]]; then
            break
        fi
        sleep 0.25
    done
    if [[ "${seeded}" != "seed-from-garnet" ]]; then
        status="FAIL"
        details="master_link_status=up but seeded pre-sync key was not replicated"
        stop_pid "${garnet_pid}"
        cleanup_docker_nodes "${redis_name}"
        record_result "garnet_primary_to_redis_replica" "${status}" "${details}; log=${log_file}"
        return 0
    fi

    redis-cli -h 127.0.0.1 -p "${garnet_port}" SET repl:probe garnet > /dev/null 2>&1 || true
    local replicated=""
    for _ in $(seq 1 40); do
        replicated="$(redis-cli -h 127.0.0.1 -p "${redis_port}" GET repl:probe 2>/dev/null || true)"
        if [[ "${replicated}" == "garnet" ]]; then
            status="PASS"
            details="redis replica synced from garnet, loaded seeded full-sync data, and received post-sync probe key"
            break
        fi
        sleep 0.25
    done
    if [[ "${status}" != "PASS" ]]; then
        status="FAIL"
        details="master_link_status=up but probe key was not replicated"
    fi

    stop_pid "${garnet_pid}"
    cleanup_docker_nodes "${redis_name}"
    record_result "garnet_primary_to_redis_replica" "${status}" "${details}; log=${log_file}"
}

test_redis_to_redis_replication_master_switch
test_redis_primary_to_garnet_replica
test_garnet_primary_to_redis_replica

echo "replication capability summary:"
if command -v column >/dev/null 2>&1; then
    column -t -s, "${SUMMARY_CSV}"
else
    cat "${SUMMARY_CSV}"
fi
echo "result_dir=${RESULT_DIR}"
