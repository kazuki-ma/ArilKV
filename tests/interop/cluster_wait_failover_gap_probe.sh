#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}/garnet-rs"

COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.cluster-wait-failover.yml}"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/cluster-wait-failover-gap-$(date +%Y%m%d-%H%M%S)}"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"

GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-cargo run -p garnet-server --release}"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
GARNET_STANDALONE_PORT="${GARNET_STANDALONE_PORT:-7490}"
GARNET_CLUSTER_BASE_PORT="${GARNET_CLUSTER_BASE_PORT:-7491}"

REDIS_PRIMARY_PORT="${REDIS_PRIMARY_PORT:-7391}"
REDIS_REPLICA_PORT="${REDIS_REPLICA_PORT:-7392}"
REDIS_CLUSTER_PORT_1="${REDIS_CLUSTER_PORT_1:-7393}"
REDIS_CLUSTER_PORT_2="${REDIS_CLUSTER_PORT_2:-7394}"
REDIS_CLUSTER_PORT_3="${REDIS_CLUSTER_PORT_3:-7395}"

mkdir -p "${RESULT_DIR}"
echo "test,status,details" > "${SUMMARY_CSV}"

GARNET_PID_STANDALONE=""
GARNET_PID_CLUSTER=""

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

require_cmd docker
require_cmd redis-cli
require_cmd awk
require_cmd sed
require_cmd grep

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "${COMPOSE_FILE}" "$@"
        return
    fi
    if command -v docker-compose >/dev/null 2>&1; then
        docker-compose -f "${COMPOSE_FILE}" "$@"
        return
    fi
    echo "docker compose not available" >&2
    exit 2
}

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

start_garnet() {
    local log_file="$1"
    shift
    (
        cd "${GARNET_RS_ROOT}"
        env RUST_BACKTRACE="${RUST_BACKTRACE}" "$@" bash -lc "${GARNET_SERVER_CMD}"
    ) >"${log_file}" 2>&1 &
    echo "$!"
}

stop_pid() {
    local pid="$1"
    if [[ -n "${pid}" ]]; then
        kill "${pid}" >/dev/null 2>&1 || true
        wait "${pid}" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    stop_pid "${GARNET_PID_STANDALONE}"
    stop_pid "${GARNET_PID_CLUSTER}"
    compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

replication_info_field() {
    local port="$1"
    local key="$2"
    redis-cli -h 127.0.0.1 -p "${port}" INFO replication \
        | awk -F: -v k="${key}" '$1 == k {print $2}' \
        | tr -d '\r'
}

probe_redis_reference_repl() {
    local log_file="${RESULT_DIR}/redis-reference-repl.log"
    compose --profile repl up -d redis-primary redis-replica >"${log_file}" 2>&1

    if ! wait_for_ping 127.0.0.1 "${REDIS_PRIMARY_PORT}" \
        || ! wait_for_ping 127.0.0.1 "${REDIS_REPLICA_PORT}"; then
        record_result "redis_reference_repl" "FAIL" "redis primary/replica did not become ready; log=${log_file}"
        return
    fi

    local replicaof_out
    replicaof_out="$(compose exec -T redis-replica redis-cli -p 6379 REPLICAOF redis-primary 6379 2>&1 || true)"
    echo "replicaof_out=${replicaof_out}" >>"${log_file}"

    local link_up="false"
    for _ in $(seq 1 80); do
        local link
        link="$(replication_info_field "${REDIS_REPLICA_PORT}" "master_link_status")"
        if [[ "${link}" == "up" ]]; then
            link_up="true"
            break
        fi
        sleep 0.25
    done

    redis-cli -h 127.0.0.1 -p "${REDIS_PRIMARY_PORT}" SET cwf:probe:repl ok >>"${log_file}" 2>&1 || true

    local replicated_value=""
    for _ in $(seq 1 40); do
        replicated_value="$(redis-cli -h 127.0.0.1 -p "${REDIS_REPLICA_PORT}" GET cwf:probe:repl 2>/dev/null || true)"
        if [[ "${replicated_value}" == "ok" ]]; then
            break
        fi
        sleep 0.25
    done

    local wait_out
    local waitaof_out
    wait_out="$(redis-cli -h 127.0.0.1 -p "${REDIS_PRIMARY_PORT}" WAIT 1 2000 2>&1 || true)"
    waitaof_out="$(redis-cli -h 127.0.0.1 -p "${REDIS_PRIMARY_PORT}" WAITAOF 1 1 2000 2>&1 || true)"

    local status="PASS"
    local details="master_link_status=${link_up}; replica_probe=${replicated_value}; WAIT='${wait_out}'; WAITAOF='${waitaof_out}'"
    if [[ "${link_up}" != "true" ]]; then
        status="FAIL"
    fi

    record_result "redis_reference_repl" "${status}" "${details}; log=${log_file}"
}

probe_redis_reference_cluster() {
    local log_file="${RESULT_DIR}/redis-reference-cluster.log"
    compose --profile cluster up -d redis-cluster-1 redis-cluster-2 redis-cluster-3 >"${log_file}" 2>&1

    if ! wait_for_ping 127.0.0.1 "${REDIS_CLUSTER_PORT_1}" \
        || ! wait_for_ping 127.0.0.1 "${REDIS_CLUSTER_PORT_2}" \
        || ! wait_for_ping 127.0.0.1 "${REDIS_CLUSTER_PORT_3}"; then
        record_result "redis_reference_cluster" "FAIL" "redis cluster nodes did not become ready; log=${log_file}"
        return
    fi

    local create_out
    set +e
    create_out="$(compose exec -T redis-cluster-1 redis-cli --cluster create \
        redis-cluster-1:6379 redis-cluster-2:6379 redis-cluster-3:6379 \
        --cluster-replicas 0 --cluster-yes 2>&1)"
    local create_rc=$?
    set -e

    echo "cluster_create_rc=${create_rc}" >>"${log_file}"
    echo "cluster_create_out=${create_out}" >>"${log_file}"

    local info_out
    info_out="$(redis-cli -h 127.0.0.1 -p "${REDIS_CLUSTER_PORT_1}" CLUSTER INFO 2>&1 || true)"
    echo "cluster_info=${info_out}" >>"${log_file}"

    local status="PASS"
    if ! echo "${info_out}" | grep -q "cluster_state:ok" \
        || ! echo "${info_out}" | grep -q "cluster_slots_assigned:16384"; then
        status="FAIL"
    fi

    record_result "redis_reference_cluster" "${status}" "cluster_create_rc=${create_rc}; CLUSTER_INFO='${info_out}'; log=${log_file}"
}

probe_garnet_standalone() {
    local log_file="${RESULT_DIR}/garnet-standalone.log"
    GARNET_PID_STANDALONE="$(start_garnet "${log_file}" "GARNET_BIND_ADDR=127.0.0.1:${GARNET_STANDALONE_PORT}")"

    if ! wait_for_ping 127.0.0.1 "${GARNET_STANDALONE_PORT}"; then
        record_result "garnet_standalone_boot" "FAIL" "garnet standalone did not become ready; log=${log_file}"
        return
    fi

    local wait_out
    local waitaof_out
    local waitaof_local_out
    local migrate_out
    local failover_out

    redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" SET cwf:probe:migrate value >>"${log_file}" 2>&1 || true

    wait_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" WAIT 1 1000 2>&1 || true)"
    waitaof_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" WAITAOF 0 1 1000 2>&1 || true)"
    waitaof_local_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" WAITAOF 1 1 1000 2>&1 || true)"
    migrate_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" MIGRATE 127.0.0.1 6379 key 0 1000 2>&1 || true)"
    failover_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_STANDALONE_PORT}" FAILOVER 2>&1 || true)"

    local wait_status="PASS"
    if [[ "${wait_out}" == "0" || "${wait_out}" == ":0" || "${wait_out}" == "(integer) 0" ]]; then
        wait_status="GAP"
    fi

    local waitaof_status="PASS"
    if echo "${waitaof_local_out}" | grep -qi "appendonly is disabled"; then
        waitaof_status="GAP"
    fi

    local migrate_status="PASS"
    if echo "${migrate_out}" | grep -Eqi "MIGRATE is disabled|NOKEY"; then
        migrate_status="GAP"
    fi

    local failover_status="PASS"
    if echo "${failover_out}" | grep -qi "cluster support disabled"; then
        failover_status="GAP"
    fi

    record_result "garnet_wait" "${wait_status}" "WAIT='${wait_out}'; log=${log_file}"
    record_result "garnet_waitaof" "${waitaof_status}" "WAITAOF(0,1)='${waitaof_out}'; WAITAOF(1,1)='${waitaof_local_out}'; log=${log_file}"
    record_result "garnet_migrate" "${migrate_status}" "MIGRATE='${migrate_out}'; log=${log_file}"
    record_result "garnet_failover" "${failover_status}" "FAILOVER='${failover_out}'; log=${log_file}"

    stop_pid "${GARNET_PID_STANDALONE}"
    GARNET_PID_STANDALONE=""
}

probe_garnet_cluster_mode() {
    local log_file="${RESULT_DIR}/garnet-cluster-mode.log"
    GARNET_PID_CLUSTER="$(start_garnet "${log_file}" \
        "GARNET_BIND_ADDR=127.0.0.1:${GARNET_CLUSTER_BASE_PORT}" \
        "GARNET_OWNER_NODE_COUNT=3" \
        "GARNET_MULTI_PORT_CLUSTER_MODE=1")"

    if ! wait_for_ping 127.0.0.1 "${GARNET_CLUSTER_BASE_PORT}" \
        || ! wait_for_ping 127.0.0.1 "$((GARNET_CLUSTER_BASE_PORT + 1))" \
        || ! wait_for_ping 127.0.0.1 "$((GARNET_CLUSTER_BASE_PORT + 2))"; then
        record_result "garnet_cluster_mode_boot" "FAIL" "garnet cluster-mode ports did not become ready; log=${log_file}"
        return
    fi

    local cluster_info_out
    local cluster_nodes_out
    local cluster_slots_out
    local count_slot_out
    local getkeys_slot_out
    local readonly_out
    local readwrite_out
    local moved_example=""

    cluster_info_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" CLUSTER INFO 2>&1 || true)"
    cluster_nodes_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" CLUSTER NODES 2>&1 || true)"
    cluster_slots_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" CLUSTER SLOTS 2>&1 || true)"
    count_slot_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" CLUSTER COUNTKEYSINSLOT 0 2>&1 || true)"
    getkeys_slot_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" CLUSTER GETKEYSINSLOT 0 10 2>&1 || true)"
    readonly_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" READONLY 2>&1 || true)"
    readwrite_out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" READWRITE 2>&1 || true)"

    for i in $(seq 1 512); do
        local key="cwf:key:${i}"
        local out
        out="$(redis-cli -h 127.0.0.1 -p "${GARNET_CLUSTER_BASE_PORT}" GET "${key}" 2>&1 || true)"
        if echo "${out}" | grep -q "MOVED"; then
            moved_example="${out}"
            break
        fi
    done

    local readonly_status="PASS"
    if echo "${readonly_out}" | grep -qi "cluster support disabled"; then
        readonly_status="GAP"
    fi

    local readwrite_status="PASS"
    if echo "${readwrite_out}" | grep -qi "cluster support disabled"; then
        readwrite_status="GAP"
    fi

    local moved_status="FAIL"
    if [[ -n "${moved_example}" ]]; then
        moved_status="PASS"
    fi

    record_result "garnet_cluster_surface" "PASS" "CLUSTER_INFO='${cluster_info_out}'; CLUSTER_NODES='${cluster_nodes_out}'; CLUSTER_SLOTS='${cluster_slots_out}'; COUNTKEYSINSLOT='${count_slot_out}'; GETKEYSINSLOT='${getkeys_slot_out}'; log=${log_file}"
    record_result "garnet_cluster_readonly" "${readonly_status}" "READONLY='${readonly_out}'; READWRITE='${readwrite_out}'; log=${log_file}"
    record_result "garnet_cluster_moved" "${moved_status}" "MOVED='${moved_example}'; log=${log_file}"

    stop_pid "${GARNET_PID_CLUSTER}"
    GARNET_PID_CLUSTER=""
}

probe_redis_reference_repl
probe_redis_reference_cluster
probe_garnet_standalone
probe_garnet_cluster_mode

echo "cluster/wait/failover gap summary:"
if command -v column >/dev/null 2>&1; then
    column -t -s, "${SUMMARY_CSV}"
else
    cat "${SUMMARY_CSV}"
fi

echo "result_dir=${RESULT_DIR}"
