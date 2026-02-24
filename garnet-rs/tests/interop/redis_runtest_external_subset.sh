#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
GARNET_RS_ROOT="${WORKSPACE_ROOT}/garnet-rs"

REDIS_REPO_ROOT="${REDIS_REPO_ROOT:-/Users/kazuki-matsuda/dev/src/github.com/redis/redis}"
RUNTEXT_BIN="${REDIS_RUNTEXT_BIN:-${REDIS_REPO_ROOT}/runtest}"
RESULT_DIR="${RESULT_DIR:-${SCRIPT_DIR}/results/redis-runtest-external-$(date +%Y%m%d-%H%M%S)}"
GARNET_PORT="${GARNET_PORT:-6396}"
GARNET_SERVER_CMD="${GARNET_SERVER_CMD:-cargo run -p garnet-server --release}"

mkdir -p "${RESULT_DIR}"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"
echo "case,status,details" > "${SUMMARY_CSV}"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 2
    fi
}

record_result() {
    local case_name="$1"
    local status="$2"
    local details="$3"
    local csv_details
    csv_details="$(echo "${details}" | tr ',' ';' | tr '\n' ' ')"
    echo "${case_name},${status},${csv_details}" >> "${SUMMARY_CSV}"
}

wait_for_ping() {
    local port="$1"
    for _ in $(seq 1 200); do
        if redis-cli -h 127.0.0.1 -p "${port}" PING >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

run_runtest_case() {
    local case_name="$1"
    local unit="$2"
    shift 2
    local log_file="${RESULT_DIR}/${case_name}.log"
    local expected_ok="$#"
    local cmd=(
        "${RUNTEXT_BIN}"
        --host 127.0.0.1
        --port "${GARNET_PORT}"
        --singledb
        --force-resp3
        --dont-clean
        --single "${unit}"
    )

    for test_name in "$@"; do
        cmd+=(--only "${test_name}")
    done

    if (
        cd "${REDIS_REPO_ROOT}"
        "${cmd[@]}"
    ) >"${log_file}" 2>&1; then
        local actual_ok
        actual_ok="$(
            awk '
            BEGIN { esc = sprintf("%c", 27); ok = 0 }
            {
                line = $0
                gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
                if (line ~ /^\[ok\]:/) ok++
            }
            END { print ok + 0 }
            ' "${log_file}"
        )"
        if [[ "${actual_ok}" -eq "${expected_ok}" ]]; then
            record_result "${case_name}" "PASS" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=${actual_ok}; log=${log_file}"
        else
            record_result "${case_name}" "FAIL" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=${actual_ok}; mismatch; log=${log_file}"
        fi
    else
        record_result "${case_name}" "FAIL" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=NA; runtest_exit_nonzero; log=${log_file}"
    fi
}

run_cli_probe_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    {
        redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" FLUSHALL
        redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" SET type:string value
        redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" HSET type:hash field value
        type_string="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" TYPE type:string)"
        type_hash="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" TYPE type:hash)"
        type_none="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" TYPE type:none)"
        echo "type:string=${type_string}"
        echo "type:hash=${type_hash}"
        echo "type:none=${type_none}"
        [[ "${type_string}" == "string" ]]
        [[ "${type_hash}" == "hash" ]]
        [[ "${type_none}" == "none" ]]
    } >"${log_file}" 2>&1
    if [[ $? -eq 0 ]]; then
        record_result "${case_name}" "PASS" "redis-cli TYPE probe passed; log=${log_file}"
    else
        record_result "${case_name}" "FAIL" "redis-cli TYPE probe failed; log=${log_file}"
    fi
}

require_cmd redis-cli
require_cmd cargo
require_cmd "${RUNTEXT_BIN}"

if [[ ! -x "${RUNTEXT_BIN}" ]]; then
    echo "redis runtest binary is not executable: ${RUNTEXT_BIN}" >&2
    exit 2
fi

GARNET_PID=""
cleanup() {
    if [[ -n "${GARNET_PID}" ]]; then
        kill "${GARNET_PID}" >/dev/null 2>&1 || true
        wait "${GARNET_PID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

(
    cd "${GARNET_RS_ROOT}"
    GARNET_BIND_ADDR="127.0.0.1:${GARNET_PORT}" bash -lc "${GARNET_SERVER_CMD}"
) >"${RESULT_DIR}/garnet-server.log" 2>&1 &
GARNET_PID="$!"

if ! wait_for_ping "${GARNET_PORT}"; then
    echo "garnet server failed to start on port ${GARNET_PORT}" >&2
    exit 1
fi

run_runtest_case \
    "redis_runtest_string_mget_mset" \
    "unit/type/string" \
    "SET and GET an item" \
    "MGET" \
    "MGET against non existing key" \
    "MGET against non-string key" \
    "MSET base case" \
    "MSET with already existing - same key twice"

run_runtest_case \
    "redis_runtest_incrby_decrby" \
    "unit/type/incr" \
    "INCRBY over 32bit value with over 32bit increment" \
    "DECRBY negation overflow" \
    "DECRBY over 32bit value with over 32bit increment, negative res" \
    "DECRBY against key is not exist"

run_runtest_case \
    "redis_runtest_keyspace_exists" \
    "unit/keyspace" \
    "EXISTS" \
    "Zero length value in key. SET/GET/EXISTS"

run_cli_probe_case "redis_cli_type_probe"

echo "redis runtest external subset summary"
cat "${SUMMARY_CSV}"
echo "result_dir=${RESULT_DIR}"
