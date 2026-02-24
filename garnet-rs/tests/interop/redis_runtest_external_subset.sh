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
REDIS_RUNTEXT_MODE="${REDIS_RUNTEXT_MODE:-full}"
RUNTEXT_TIMEOUT_SECONDS="${RUNTEXT_TIMEOUT_SECONDS:-}"
RUNTEXT_CLIENTS="${RUNTEXT_CLIENTS:-}"
RUNTEXT_EXTRA_ARGS="${RUNTEXT_EXTRA_ARGS:-}"

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

run_full_runtest_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    local failed_tests_file="${RESULT_DIR}/failed-tests.txt"
    local cmd=(
        "${RUNTEXT_BIN}"
        --host 127.0.0.1
        --port "${GARNET_PORT}"
        --singledb
        --force-resp3
        --dont-clean
        --durable
    )
    local extra_args=()

    if [[ -n "${RUNTEXT_TIMEOUT_SECONDS}" ]]; then
        cmd+=(--timeout "${RUNTEXT_TIMEOUT_SECONDS}")
    fi
    if [[ -n "${RUNTEXT_CLIENTS}" ]]; then
        cmd+=(--clients "${RUNTEXT_CLIENTS}")
    fi
    if [[ -n "${RUNTEXT_EXTRA_ARGS}" ]]; then
        # shellcheck disable=SC2206
        extra_args=(${RUNTEXT_EXTRA_ARGS})
        cmd+=("${extra_args[@]}")
    fi

    local exit_code=0
    if (
        cd "${REDIS_REPO_ROOT}"
        "${cmd[@]}"
    ) >"${log_file}" 2>&1; then
        exit_code=0
    else
        exit_code=$?
    fi

    local parsed_counts
    parsed_counts="$(
        awk '
        BEGIN { esc = sprintf("%c", 27); ok = 0; err = 0; ignore = 0 }
        {
            line = $0
            gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
            gsub(/\r/, "", line)
            if (line ~ /^\[ok\]:/) {
                ok++
            } else if (line ~ /^\[err\]:/) {
                err++
            } else if (line ~ /^\[ignore\]:/) {
                ignore++
            }
        }
        END { printf("%d,%d,%d\n", ok + 0, err + 0, ignore + 0) }
        ' "${log_file}"
    )"

    local ok_count err_count ignore_count
    IFS=',' read -r ok_count err_count ignore_count <<<"${parsed_counts}"

    awk '
    BEGIN { esc = sprintf("%c", 27) }
    {
        line = $0
        gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
        gsub(/\r/, "", line)
        if (line ~ /^\[err\]:/) {
            sub(/^\[err\]:[[:space:]]*/, "", line)
            print line
        }
    }
    ' "${log_file}" > "${failed_tests_file}"

    local status="FAIL"
    if [[ "${exit_code}" -eq 0 && "${err_count}" -eq 0 ]]; then
        status="PASS"
    fi

    local details
    details="mode=full; exit_code=${exit_code}; ok=${ok_count}; err=${err_count}; ignore=${ignore_count}; log=${log_file}; failed_tests=${failed_tests_file}"
    record_result "${case_name}" "${status}" "${details}"
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

run_cli_scripting_probe_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    local status="FAIL"
    local details=""

    {
        eval_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw EVAL "return 1" 0 2>&1 || true)"
        echo "eval_output=${eval_output}"
        if [[ "${eval_output}" == "1" || "${eval_output}" == "(integer) 1" ]]; then
            local library_source
            library_source=$'#!lua name=lib_probe\nredis.register_function{function_name=\'ro_ping\', callback=function(keys, args) return redis.call(\'PING\') end, flags={\'no-writes\'}}'
            load_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw FUNCTION LOAD REPLACE "${library_source}" 2>&1 || true)"
            echo "function_load_output=${load_output}"
            fcall_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw FCALL_RO ro_ping 0 2>&1 || true)"
            echo "fcall_ro_output=${fcall_output}"
            flush_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw FUNCTION FLUSH 2>&1 || true)"
            echo "function_flush_output=${flush_output}"

            if [[ "${load_output}" == "lib_probe" && "${fcall_output}" == "PONG" ]]; then
                status="PASS"
                details="scripting_enabled_mode; eval=ok; function_load=ok; fcall_ro=ok; log=${log_file}"
            else
                status="FAIL"
                details="scripting_enabled_mode_unexpected_output; log=${log_file}"
            fi
        elif [[ "${eval_output}" == *"scripting is disabled in this server"* ]]; then
            status="PASS"
            details="scripting_disabled_mode; eval disabled as expected; log=${log_file}"
        else
            status="FAIL"
            details="unexpected_eval_output; log=${log_file}"
        fi
    } >"${log_file}" 2>&1

    record_result "${case_name}" "${status}" "${details}"
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

case "${REDIS_RUNTEXT_MODE}" in
    full)
        run_full_runtest_case "redis_runtest_full_external"
        ;;
    subset)
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
        ;;
    *)
        echo "invalid REDIS_RUNTEXT_MODE: ${REDIS_RUNTEXT_MODE} (expected: full|subset)" >&2
        exit 1
        ;;
esac

run_cli_probe_case "redis_cli_type_probe"
run_cli_scripting_probe_case "redis_cli_scripting_probe"

echo "redis runtest external summary (mode=${REDIS_RUNTEXT_MODE})"
cat "${SUMMARY_CSV}"
echo "result_dir=${RESULT_DIR}"
