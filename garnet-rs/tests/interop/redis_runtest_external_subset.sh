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
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
GARNET_SCRIPTING_ENABLED="${GARNET_SCRIPTING_ENABLED:-1}"
REDIS_RUNTEXT_MODE="${REDIS_RUNTEXT_MODE:-full}"
RUNTEXT_ENABLE_CORE_DUMP="${RUNTEXT_ENABLE_CORE_DUMP:-1}"
RUNTEXT_CAPTURE_CRASH_REPORT="${RUNTEXT_CAPTURE_CRASH_REPORT:-1}"
if [[ -z "${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES:-}" ]]; then
    if [[ "${REDIS_RUNTEXT_MODE}" == "full" ]]; then
        # Full external probes sweep many suites in one process, so keep
        # a higher default page budget to reduce false capacity failures.
        GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="16384"
    else
        GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="4096"
    fi
fi
RUNTEXT_TIMEOUT_SECONDS="${RUNTEXT_TIMEOUT_SECONDS:-}"
RUNTEXT_WALL_TIMEOUT_SECONDS="${RUNTEXT_WALL_TIMEOUT_SECONDS:-1800}"
RUNTEXT_CLIENTS="${RUNTEXT_CLIENTS:-}"
RUNTEXT_EXTRA_ARGS="${RUNTEXT_EXTRA_ARGS:-}"
RUNTEXT_SINGLEDB="${RUNTEXT_SINGLEDB:-1}"
EXPECTED_FAILS_FILE="${EXPECTED_FAILS_FILE:-}"
REDIS_CLI_OVERRIDE_BIN="${REDIS_CLI_OVERRIDE_BIN:-}"
RUNTEXT_SKIP_QUERYBUF_IN_FULL="${RUNTEXT_SKIP_QUERYBUF_IN_FULL:-0}"
RUNTEXT_RUN_QUERYBUF_ISOLATED="${RUNTEXT_RUN_QUERYBUF_ISOLATED:-0}"
RUNTEXT_SKIP_SCRIPTING_IN_FULL="${RUNTEXT_SKIP_SCRIPTING_IN_FULL:-0}"
RUNTEXT_RUN_SCRIPTING_ISOLATED="${RUNTEXT_RUN_SCRIPTING_ISOLATED:-0}"
RUNTEXT_SKIP_SCRIPTING_NOREPLICAS_TEST_IN_FULL="${RUNTEXT_SKIP_SCRIPTING_NOREPLICAS_TEST_IN_FULL:-1}"
RUNTEXT_RUN_SCRIPTING_NOREPLICAS_TEST_ISOLATED="${RUNTEXT_RUN_SCRIPTING_NOREPLICAS_TEST_ISOLATED:-1}"
RUNTEXT_SKIP_OTHER_IN_FULL="${RUNTEXT_SKIP_OTHER_IN_FULL:-0}"
RUNTEXT_SKIP_OTHER_PIPELINE_STRESSER_IN_FULL="${RUNTEXT_SKIP_OTHER_PIPELINE_STRESSER_IN_FULL:-1}"
RUNTEXT_OTHER_PIPELINE_STRESSER_TEST_NAME="${RUNTEXT_OTHER_PIPELINE_STRESSER_TEST_NAME:-PIPELINING stresser (also a regression for the old epoll bug)}"
RUNTEXT_RUN_OTHER_ISOLATED="${RUNTEXT_RUN_OTHER_ISOLATED:-0}"
RUNTEXT_RUN_OTHER_PIPELINE_STRESSER_ISOLATED="${RUNTEXT_RUN_OTHER_PIPELINE_STRESSER_ISOLATED:-1}"
RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS="${RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS:-180}"
RUNTEXT_SKIP_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_IN_FULL="${RUNTEXT_SKIP_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_IN_FULL:-1}"
RUNTEXT_RUN_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_ISOLATED="${RUNTEXT_RUN_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_ISOLATED:-1}"
RUNTEXT_RUN_ONLY_ISOLATED_UNIT="${RUNTEXT_RUN_ONLY_ISOLATED_UNIT:-}"
RUNTEXT_ALLOW_EXTERNAL_SKIP="${RUNTEXT_ALLOW_EXTERNAL_SKIP:-0}"

if [[ -z "${RUNTEXT_TIMEOUT_SECONDS}" && "${REDIS_RUNTEXT_MODE}" == "full" ]]; then
    # Keep full external probes from stalling for the runtest default (20 minutes)
    # when a single case blocks indefinitely.
    RUNTEXT_TIMEOUT_SECONDS="120"
fi

if [[ "${RUNTEXT_SINGLEDB}" != "0" && "${RUNTEXT_SINGLEDB}" != "1" ]]; then
    echo "RUNTEXT_SINGLEDB must be 0 or 1" >&2
    exit 2
fi

if [[ -z "${EXPECTED_FAILS_FILE}" && "${RUNTEXT_SINGLEDB}" == "1" ]]; then
    EXPECTED_FAILS_FILE="${SCRIPT_DIR}/known-failed-tests-singledb.txt"
fi

RUNTEXT_DB_MODE_ARGS=()
if [[ "${RUNTEXT_SINGLEDB}" == "1" ]]; then
    RUNTEXT_DB_MODE_ARGS+=(--singledb)
fi

mkdir -p "${RESULT_DIR}"
SUMMARY_CSV="${RESULT_DIR}/summary.csv"
GARNET_LOG_FILE="${RESULT_DIR}/garnet-server.log"
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

garnet_process_id() {
    local info_output=""
    local process_id=""

    info_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw INFO SERVER 2>/dev/null || true)"
    process_id="$(
        printf '%s\n' "${info_output}" |
            awk -F: '/^process_id:/ { gsub(/\r/, "", $2); print $2; exit }'
    )"
    if [[ "${process_id}" =~ ^[0-9]+$ ]]; then
        echo "${process_id}"
        return 0
    fi
    return 1
}

install_redis_external_pid_patch() {
    local server_tcl="${REDIS_REPO_ROOT}/tests/support/server.tcl"
    local pid_patch_marker="# garnet-rs external pid injection"
    local skip_patch_marker="# garnet-rs external skip override"

    if grep -Fq "${pid_patch_marker}" "${server_tcl}" && grep -Fq "${skip_patch_marker}" "${server_tcl}"; then
        return 0
    fi

    REDIS_SERVER_TCL_BACKUP="${RESULT_DIR}/server.tcl.before-garnet-external-pid-patch"
    cp "${server_tcl}" "${REDIS_SERVER_TCL_BACKUP}"

    if ! grep -Fq "${pid_patch_marker}" "${server_tcl}"; then
        perl -0pi -e '
            s/set client \[redis \$::host \$::port 0 \$::tls\]\n    dict set srv "client" \$client\n/set client [redis \$::host \$::port 0 \$::tls]\n    dict set srv "client" \$client\n    # garnet-rs external pid injection\n    if {[info exists ::env(GARNET_EXTERNAL_SERVER_PID)] && \$::env(GARNET_EXTERNAL_SERVER_PID) ne ""} {\n        dict set srv "pid" \$::env(GARNET_EXTERNAL_SERVER_PID)\n    }\n/
        ' "${server_tcl}"
    fi

    if ! grep -Fq "${skip_patch_marker}" "${server_tcl}"; then
        perl -0pi -e '
            s/    if \{\$::external && \[lsearch \$tags "external:skip"\] >= 0\} \{\n        set err "Not supported on external server"\n        return 0\n    \}/    if {\$::external && [lsearch \$tags "external:skip"] >= 0} {\n        # garnet-rs external skip override\n        if {![info exists ::env(GARNET_EXTERNAL_ALLOW_SKIP)] || \$::env(GARNET_EXTERNAL_ALLOW_SKIP) ne "1"} {\n            set err "Not supported on external server"\n            return 0\n        }\n    }/
        ' "${server_tcl}"
    fi

    if ! grep -Fq "${pid_patch_marker}" "${server_tcl}" || ! grep -Fq "${skip_patch_marker}" "${server_tcl}"; then
        cp "${REDIS_SERVER_TCL_BACKUP}" "${server_tcl}"
        echo "failed to install Redis external pid patch into ${server_tcl}" >&2
        return 1
    fi

    return 0
}

restore_redis_external_pid_patch() {
    local server_tcl="${REDIS_REPO_ROOT}/tests/support/server.tcl"
    if [[ -n "${REDIS_SERVER_TCL_BACKUP:-}" && -f "${REDIS_SERVER_TCL_BACKUP}" ]]; then
        cp "${REDIS_SERVER_TCL_BACKUP}" "${server_tcl}"
        REDIS_SERVER_TCL_BACKUP=""
    fi
}

resolve_redis_cli_override_bin() {
    if [[ -n "${REDIS_CLI_OVERRIDE_BIN}" ]]; then
        if "${REDIS_CLI_OVERRIDE_BIN}" --version >/dev/null 2>&1; then
            echo "${REDIS_CLI_OVERRIDE_BIN}"
            return 0
        fi
        echo "configured REDIS_CLI_OVERRIDE_BIN is not runnable: ${REDIS_CLI_OVERRIDE_BIN}" >&2
        return 1
    fi

    local repo_cli="${REDIS_REPO_ROOT}/src/redis-cli"
    if [[ -x "${repo_cli}" ]] && "${repo_cli}" --version >/dev/null 2>&1; then
        echo "${repo_cli}"
        return 0
    fi

    if [[ -d "${REDIS_REPO_ROOT}" ]]; then
        local build_log="${RESULT_DIR}/redis-cli-build.log"
        if make -C "${REDIS_REPO_ROOT}" redis-cli -j4 >"${build_log}" 2>&1; then
            if [[ -x "${repo_cli}" ]] && "${repo_cli}" --version >/dev/null 2>&1; then
                echo "${repo_cli}"
                return 0
            fi
        fi
    fi

    local native_cli=""
    native_cli="$(command -v redis-cli || true)"
    if [[ -n "${native_cli}" ]] && "${native_cli}" --version >/dev/null 2>&1; then
        echo "${native_cli}"
        return 0
    fi

    echo "failed to find a runnable redis-cli binary" >&2
    return 1
}

install_redis_cli_override_patch() {
    local cli_tcl="${REDIS_REPO_ROOT}/tests/support/cli.tcl"
    local patch_marker="# garnet-rs external redis-cli override"

    if ! REDIS_CLI_OVERRIDE_RESOLVED_BIN="$(resolve_redis_cli_override_bin)"; then
        return 1
    fi
    export REDIS_CLI_OVERRIDE_RESOLVED_BIN

    if grep -Fq "${patch_marker}" "${cli_tcl}"; then
        return 0
    fi

    REDIS_CLI_TCL_BACKUP="${RESULT_DIR}/cli.tcl.before-garnet-redis-cli-override-patch"
    cp "${cli_tcl}" "${REDIS_CLI_TCL_BACKUP}"

    CLI_TCL_PATH="${cli_tcl}" python3 <<'PY'
from pathlib import Path
import os
import re

path = Path(os.environ["CLI_TCL_PATH"])
text = path.read_text()
new = """proc rediscli_binary {} {
    # garnet-rs external redis-cli override
    if {[info exists ::env(REDIS_CLI_OVERRIDE_RESOLVED_BIN)] && $::env(REDIS_CLI_OVERRIDE_RESOLVED_BIN) ne ""} {
        return $::env(REDIS_CLI_OVERRIDE_RESOLVED_BIN)
    }
    return src/redis-cli
}

proc rediscli {host port {opts {}}} {
    set cmd [list [rediscli_binary] -h $host -p $port]
    lappend cmd {*}[rediscli_tls_config "tests"]
    lappend cmd {*}$opts
    return $cmd
}

# Returns command line for executing redis-cli with a unix socket address
proc rediscli_unixsocket {unixsocket {opts {}}} {
    return [list [rediscli_binary] -s $unixsocket {*}$opts]
}
"""
pattern = re.compile(
    r"""proc rediscli \{host port \{opts \{\}\}\} \{\n"""
    r"""(?:    .*\n)+?"""
    r"""\}\n\n"""
    r"""# Returns command line for executing redis-cli with a unix socket address\n"""
    r"""proc rediscli_unixsocket \{unixsocket \{opts \{\}\}\} \{\n"""
    r"""(?:    .*\n)+?"""
    r"""\}\n""",
    re.MULTILINE,
)
if not pattern.search(text):
    raise SystemExit(f"expected cli.tcl block not found in {path}")
path.write_text(pattern.sub(new, text, count=1))
PY

    if ! grep -Fq "${patch_marker}" "${cli_tcl}"; then
        cp "${REDIS_CLI_TCL_BACKUP}" "${cli_tcl}"
        echo "failed to install Redis cli override patch into ${cli_tcl}" >&2
        return 1
    fi

    return 0
}

restore_redis_cli_override_patch() {
    local cli_tcl="${REDIS_REPO_ROOT}/tests/support/cli.tcl"
    if [[ -n "${REDIS_CLI_TCL_BACKUP:-}" && -f "${REDIS_CLI_TCL_BACKUP}" ]]; then
        cp "${REDIS_CLI_TCL_BACKUP}" "${cli_tcl}"
        REDIS_CLI_TCL_BACKUP=""
    fi
    unset REDIS_CLI_OVERRIDE_RESOLVED_BIN || true
}

start_garnet_server() {
    if [[ -n "${GARNET_PID:-}" ]] && kill -0 "${GARNET_PID}" >/dev/null 2>&1; then
        return 0
    fi

    (
        cd "${GARNET_RS_ROOT}"
        if [[ "${RUNTEXT_ENABLE_CORE_DUMP}" == "1" ]]; then
            ulimit -c unlimited >/dev/null 2>&1 || true
        fi
        GARNET_BIND_ADDR="127.0.0.1:${GARNET_PORT}" \
        RUST_BACKTRACE="${RUST_BACKTRACE}" \
        GARNET_SCRIPTING_ENABLED="${GARNET_SCRIPTING_ENABLED}" \
        GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES="${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES}" \
        bash -lc "${GARNET_SERVER_CMD}"
    ) >>"${GARNET_LOG_FILE}" 2>&1 &
    GARNET_PID="$!"

    if ! wait_for_ping "${GARNET_PORT}"; then
        echo "garnet server failed to start on port ${GARNET_PORT}" >&2
        stop_garnet_server
        return 1
    fi

    if ! GARNET_EXTERNAL_SERVER_PID="$(garnet_process_id)"; then
        echo "failed to determine garnet server process_id via INFO SERVER" >&2
        stop_garnet_server
        return 1
    fi
    export GARNET_EXTERNAL_SERVER_PID

    return 0
}

stop_garnet_server() {
    if [[ -n "${GARNET_PID:-}" ]]; then
        kill "${GARNET_PID}" >/dev/null 2>&1 || true
        wait "${GARNET_PID}" >/dev/null 2>&1 || true
        GARNET_PID=""
    fi
    unset GARNET_EXTERNAL_SERVER_PID || true
}

restart_garnet_server() {
    stop_garnet_server
    start_garnet_server
}

kill_process_tree() {
    local pid="$1"
    local signal="${2:-TERM}"
    local child_pids=""

    child_pids="$(pgrep -P "${pid}" 2>/dev/null || true)"
    if [[ -n "${child_pids}" ]]; then
        while IFS= read -r child_pid; do
            [[ -z "${child_pid}" ]] && continue
            kill_process_tree "${child_pid}" "${signal}"
        done <<< "${child_pids}"
    fi

    kill "-${signal}" "${pid}" >/dev/null 2>&1 || true
}

find_latest_garnet_crash_report_since() {
    local since_epoch="$1"
    local latest_report=""
    local latest_mtime=0
    local report=""

    while IFS= read -r report; do
        [[ -z "${report}" ]] && continue
        local mtime
        mtime="$(stat -f %m "${report}" 2>/dev/null || echo 0)"
        if [[ "${mtime}" =~ ^[0-9]+$ ]] && (( mtime >= since_epoch )) && (( mtime > latest_mtime )); then
            latest_report="${report}"
            latest_mtime="${mtime}"
        fi
    done < <(compgen -G "${HOME}/Library/Logs/DiagnosticReports/garnet-server-*.ips" || true)

    echo "${latest_report}"
}

find_latest_core_dump_since() {
    local since_epoch="$1"
    local latest_core=""
    local latest_mtime=0
    local core_file=""

    while IFS= read -r core_file; do
        [[ -z "${core_file}" ]] && continue
        local mtime
        mtime="$(stat -f %m "${core_file}" 2>/dev/null || echo 0)"
        if [[ "${mtime}" =~ ^[0-9]+$ ]] && (( mtime >= since_epoch )) && (( mtime > latest_mtime )); then
            latest_core="${core_file}"
            latest_mtime="${mtime}"
        fi
    done < <(compgen -G "/cores/core.*" || true)

    echo "${latest_core}"
}

run_with_watchdogs() {
    local wall_timeout_seconds="$1"
    shift
    local -a cmd=("$@")
    local cmd_pid=""
    local start_epoch=""
    local elapsed_seconds=0
    local timed_out=0
    local server_exited=0

    if [[ -n "${wall_timeout_seconds}" && ! "${wall_timeout_seconds}" =~ ^[0-9]+$ ]]; then
        echo "invalid RUNTEXT_WALL_TIMEOUT_SECONDS value: ${wall_timeout_seconds}" >&2
        return 2
    fi

    "${cmd[@]}" &
    cmd_pid="$!"
    start_epoch="$(date +%s)"

    while kill -0 "${cmd_pid}" >/dev/null 2>&1; do
        if [[ -n "${GARNET_PID:-}" ]] && ! kill -0 "${GARNET_PID}" >/dev/null 2>&1; then
            echo "warning: garnet server exited during runtest; aborting runtest command" >&2
            server_exited=1
            kill_process_tree "${cmd_pid}" TERM
            sleep 2
            kill_process_tree "${cmd_pid}" KILL
            break
        fi

        if [[ -n "${wall_timeout_seconds}" && "${wall_timeout_seconds}" -gt 0 ]]; then
            elapsed_seconds="$(( $(date +%s) - start_epoch ))"
            if (( elapsed_seconds >= wall_timeout_seconds )); then
                echo "warning: runtest exceeded wall timeout (${wall_timeout_seconds}s); aborting command" >&2
                timed_out=1
                kill_process_tree "${cmd_pid}" TERM
                sleep 2
                kill_process_tree "${cmd_pid}" KILL
                break
            fi
        fi

        sleep 1
    done

    local cmd_exit=0
    if wait "${cmd_pid}" >/dev/null 2>&1; then
        cmd_exit=0
    else
        cmd_exit=$?
    fi

    if (( server_exited == 1 )); then
        return 125
    fi
    if (( timed_out == 1 )); then
        return 124
    fi
    return "${cmd_exit}"
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
        "${RUNTEXT_DB_MODE_ARGS[@]}"
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
            record_result "${case_name}" "PASS" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=${actual_ok}"
        else
            record_result "${case_name}" "FAIL" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=${actual_ok}; mismatch"
        fi
    else
        record_result "${case_name}" "FAIL" "unit=${unit}; expected_ok=${expected_ok}; actual_ok=NA; runtest_exit_nonzero"
    fi
}

run_full_runtest_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    local failed_tests_file="${RESULT_DIR}/failed-tests.txt"
    local expected_failed_tests_file="${RESULT_DIR}/expected-failed-tests.txt"
    local unexpected_failed_tests_file="${RESULT_DIR}/unexpected-failed-tests.txt"
    local cmd=(
        "${RUNTEXT_BIN}"
        --host 127.0.0.1
        --port "${GARNET_PORT}"
        "${RUNTEXT_DB_MODE_ARGS[@]}"
        --dont-clean
        --durable
    )
    local skip_querybuf_applied=0
    local skip_scripting_applied=0
    local skiptest_scripting_noreplicas_applied=0
    local skip_other_applied=0
    local skiptest_other_pipeline_applied=0
    local skiptest_redis_cli_connecting_as_replica_applied=0
    local extra_args=()
    local case_start_epoch
    case_start_epoch="$(date +%s)"

    if [[ -n "${RUNTEXT_TIMEOUT_SECONDS}" ]]; then
        cmd+=(--timeout "${RUNTEXT_TIMEOUT_SECONDS}")
    fi
    if [[ -n "${RUNTEXT_CLIENTS}" ]]; then
        cmd+=(--clients "${RUNTEXT_CLIENTS}")
    fi
    if [[ -n "${RUNTEXT_EXTRA_ARGS}" ]]; then
        # Parse developer-provided extra args with shell quoting preserved
        # (for example: --only "LATENCY of expire events are correctly collected").
        # shellcheck disable=SC2294,SC2206
        eval "extra_args=(${RUNTEXT_EXTRA_ARGS})"
        cmd+=("${extra_args[@]}")
    else
        if [[ "${RUNTEXT_SKIP_QUERYBUF_IN_FULL}" == "1" ]]; then
            # Keep this escape hatch for focused diagnosis. The default path now keeps
            # `unit/querybuf` in-band, but an early failure can still leave
            # `DEBUG PAUSE-CRON 1` behind and contaminate later suites.
            cmd+=(--skipunit "unit/querybuf")
            skip_querybuf_applied=1
        fi
        if [[ "${RUNTEXT_SKIP_SCRIPTING_IN_FULL}" == "1" ]]; then
            # Keep this escape hatch for focused diagnosis. The default path now keeps
            # `unit/scripting` in-band, but an early failure can still leave
            # DEBUG expiration toggles behind and contaminate later suites.
            cmd+=(--skipunit "unit/scripting")
            skip_scripting_applied=1
        elif [[ "${RUNTEXT_SKIP_SCRIPTING_NOREPLICAS_TEST_IN_FULL}" == "1" ]]; then
            # This exact scripting case requires zero connected replicas.
            # Earlier shared-lane replication coverage can leave an attached replica,
            # so keep the rest of unit/scripting in-band and replay only this test
            # after a restart on a clean server.
            cmd+=(--skiptest "not enough good replicas")
            skiptest_scripting_noreplicas_applied=1
        fi
        if [[ "${RUNTEXT_SKIP_OTHER_IN_FULL}" == "1" ]]; then
            # `unit/other` currently passes on a fresh server but can time out in the
            # long-lived full-run process at the inline pipeline stresser. Run it in
            # isolation with a server restart so the main full probe can continue.
            cmd+=(--skipunit "unit/other")
            skip_other_applied=1
        elif [[ "${RUNTEXT_SKIP_OTHER_PIPELINE_STRESSER_IN_FULL}" == "1" ]]; then
            # `unit/other` is stable in the main full probe except for the
            # inline pipelining stresser. Keep the rest of the unit in the
            # shared run and isolate only that one test after a restart.
            cmd+=(--skiptest "${RUNTEXT_OTHER_PIPELINE_STRESSER_TEST_NAME}")
            skiptest_other_pipeline_applied=1
        fi
        if [[ "${RUNTEXT_SKIP_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_IN_FULL}" == "1" ]]; then
            # `redis-cli --replica` expects a clean master/replica lifecycle and is
            # sensitive to replica state left behind by earlier shared-lane tests.
            # Keep the rest of integration/redis-cli in-band and replay just this
            # case after a restart.
            cmd+=(--skiptest "Connecting as a replica")
            skiptest_redis_cli_connecting_as_replica_applied=1
        fi
    fi

    local exit_code=0
    local exit_reason="completed"
    if (
        cd "${REDIS_REPO_ROOT}"
        run_with_watchdogs "${RUNTEXT_WALL_TIMEOUT_SECONDS}" "${cmd[@]}"
    ) >"${log_file}" 2>&1; then
        exit_code=0
    else
        exit_code=$?
    fi

    case "${exit_code}" in
        124)
            exit_reason="wall_timeout"
            ;;
        125)
            exit_reason="garnet_server_exited"
            ;;
        0)
            exit_reason="completed"
            ;;
        *)
            exit_reason="runtest_exit_nonzero"
            ;;
    esac

    local parsed_counts
    parsed_counts="$(
        awk '
        BEGIN { esc = sprintf("%c", 27); ok = 0; err = 0; ignore = 0; timeout = 0 }
        {
            line = $0
            gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
            gsub(/\r/, "", line)
            if (line ~ /^\[ok\]:/) {
                ok++
            } else if (line ~ /^\[err\]:/ || line ~ /^\[exception\]:/) {
                err++
            } else if (line ~ /^\[ignore\]:/) {
                ignore++
            } else if (line ~ /^\[TIMEOUT\]:/) {
                timeout++
            }
        }
        END { printf("%d,%d,%d,%d\n", ok + 0, err + 0, ignore + 0, timeout + 0) }
        ' "${log_file}"
    )"

    local ok_count err_count ignore_count timeout_count
    IFS=',' read -r ok_count err_count ignore_count timeout_count <<<"${parsed_counts}"

    awk '
    BEGIN { esc = sprintf("%c", 27) }
    {
        line = $0
        gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
        gsub(/\r/, "", line)
        if (line ~ /^\[err\]:/) {
            sub(/^\[err\]:[[:space:]]*/, "", line)
            print line
        } else if (line ~ /^\[exception\]:/) {
            sub(/^\[exception\]:[[:space:]]*/, "", line)
            print line
        } else if (line ~ /^sock[0-9]+ => \(IN PROGRESS\)/) {
            sub(/^sock[0-9]+ => \(IN PROGRESS\)[[:space:]]*/, "", line)
            print line
        }
    }
    ' "${log_file}" > "${failed_tests_file}"

    local failed_tests_count
    failed_tests_count="$(awk 'END {print NR+0}' "${failed_tests_file}")"
    local expected_fail_count=0
    local unexpected_fail_count=0

    : > "${expected_failed_tests_file}"
    : > "${unexpected_failed_tests_file}"
    if [[ "${failed_tests_count}" -gt 0 ]]; then
        if [[ -f "${EXPECTED_FAILS_FILE}" && -s "${EXPECTED_FAILS_FILE}" ]]; then
            grep -Fxf "${EXPECTED_FAILS_FILE}" "${failed_tests_file}" > "${expected_failed_tests_file}" || true
            grep -Fvx -f "${EXPECTED_FAILS_FILE}" "${failed_tests_file}" > "${unexpected_failed_tests_file}" || true
            expected_fail_count="$(awk 'END {print NR+0}' "${expected_failed_tests_file}")"
            unexpected_fail_count="$(awk 'END {print NR+0}' "${unexpected_failed_tests_file}")"
        else
            cp "${failed_tests_file}" "${unexpected_failed_tests_file}"
            unexpected_fail_count="${failed_tests_count}"
        fi
    fi

    local status="FAIL"
    if [[ "${exit_code}" -eq 0 && "${err_count}" -eq 0 && "${timeout_count}" -eq 0 && "${failed_tests_count}" -eq 0 ]]; then
        status="PASS"
    elif [[ "${failed_tests_count}" -gt 0 && "${unexpected_fail_count}" -eq 0 ]]; then
        status="PASS_WITH_KNOWN_GAPS"
    fi

    local details
    details="mode=full; tsavorite_pages=${GARNET_TSAVORITE_MAX_IN_MEMORY_PAGES}; skipunit_querybuf=${skip_querybuf_applied}; skipunit_scripting=${skip_scripting_applied}; skiptest_scripting_noreplicas=${skiptest_scripting_noreplicas_applied}; skipunit_other=${skip_other_applied}; skiptest_other_pipeline=${skiptest_other_pipeline_applied}; skiptest_redis_cli_connecting_as_replica=${skiptest_redis_cli_connecting_as_replica_applied}; exit_code=${exit_code}; exit_reason=${exit_reason}; wall_timeout_seconds=${RUNTEXT_WALL_TIMEOUT_SECONDS}; ok=${ok_count}; err=${err_count}; timeout=${timeout_count}; ignore=${ignore_count}; failed_tests=${failed_tests_count}; expected_failed_tests=${expected_fail_count}; unexpected_failed_tests=${unexpected_fail_count}"

    if [[ "${RUNTEXT_CAPTURE_CRASH_REPORT}" == "1" ]]; then
        local crash_report
        crash_report="$(find_latest_garnet_crash_report_since "${case_start_epoch}")"
        if [[ -n "${crash_report}" ]]; then
            local crash_copy
            crash_copy="${RESULT_DIR}/$(basename "${crash_report}")"
            cp -f "${crash_report}" "${crash_copy}" >/dev/null 2>&1 || true
            details="${details}; crash_report=$(basename "${crash_copy}")"
        fi
    fi

    local core_dump
    core_dump="$(find_latest_core_dump_since "${case_start_epoch}")"
    if [[ -n "${core_dump}" ]]; then
        local core_copy
        core_copy="${RESULT_DIR}/$(basename "${core_dump}")"
        cp -f "${core_dump}" "${core_copy}" >/dev/null 2>&1 || true
        details="${details}; core_dump=$(basename "${core_copy}")"
    fi

    record_result "${case_name}" "${status}" "${details}"
}

run_isolated_unit_case() {
    local case_name="$1"
    local unit="$2"
    shift 2
    local timeout_override=""
    if [[ "$#" -gt 0 ]]; then
        timeout_override="$1"
        shift
    fi
    local case_args=("$@")
    local log_file="${RESULT_DIR}/${case_name}.log"
    local failed_tests_file="${RESULT_DIR}/${case_name}.failed-tests.txt"
    local cmd=(
        "${RUNTEXT_BIN}"
        --host 127.0.0.1
        --port "${GARNET_PORT}"
        "${RUNTEXT_DB_MODE_ARGS[@]}"
        --dont-clean
        --durable
        --single "${unit}"
        "${case_args[@]}"
    )

    local effective_timeout="${RUNTEXT_TIMEOUT_SECONDS}"
    if [[ -n "${timeout_override}" ]]; then
        effective_timeout="${timeout_override}"
    fi

    if [[ -n "${effective_timeout}" ]]; then
        cmd+=(--timeout "${effective_timeout}")
    fi
    if [[ -n "${RUNTEXT_CLIENTS}" ]]; then
        cmd+=(--clients "${RUNTEXT_CLIENTS}")
    fi

    local exit_code=0
    local exit_reason="completed"
    if (
        cd "${REDIS_REPO_ROOT}"
        run_with_watchdogs "${RUNTEXT_WALL_TIMEOUT_SECONDS}" "${cmd[@]}"
    ) >"${log_file}" 2>&1; then
        exit_code=0
    else
        exit_code=$?
    fi

    case "${exit_code}" in
        124)
            exit_reason="wall_timeout"
            ;;
        125)
            exit_reason="garnet_server_exited"
            ;;
        0)
            exit_reason="completed"
            ;;
        *)
            exit_reason="runtest_exit_nonzero"
            ;;
    esac

    local parsed_counts
    parsed_counts="$(
        awk '
        BEGIN { esc = sprintf("%c", 27); ok = 0; err = 0; ignore = 0; timeout = 0 }
        {
            line = $0
            gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
            gsub(/\r/, "", line)
            if (line ~ /^\[ok\]:/) {
                ok++
            } else if (line ~ /^\[err\]:/ || line ~ /^\[exception\]:/) {
                err++
            } else if (line ~ /^\[ignore\]:/) {
                ignore++
            } else if (line ~ /^\[TIMEOUT\]:/) {
                timeout++
            }
        }
        END { printf("%d,%d,%d,%d\n", ok + 0, err + 0, ignore + 0, timeout + 0) }
        ' "${log_file}"
    )"

    local ok_count err_count ignore_count timeout_count
    IFS=',' read -r ok_count err_count ignore_count timeout_count <<<"${parsed_counts}"

    awk '
    BEGIN { esc = sprintf("%c", 27) }
    {
        line = $0
        gsub(esc "\\[[0-9;]*[A-Za-z]", "", line)
        gsub(/\r/, "", line)
        if (line ~ /^\[err\]:/) {
            sub(/^\[err\]:[[:space:]]*/, "", line)
            print line
        } else if (line ~ /^\[exception\]:/) {
            sub(/^\[exception\]:[[:space:]]*/, "", line)
            print line
        } else if (line ~ /^sock[0-9]+ => \(IN PROGRESS\)/) {
            sub(/^sock[0-9]+ => \(IN PROGRESS\)[[:space:]]*/, "", line)
            print line
        }
    }
    ' "${log_file}" > "${failed_tests_file}"

    local failed_tests_count
    failed_tests_count="$(awk 'END {print NR+0}' "${failed_tests_file}")"

    local status="FAIL"
    if [[ "${exit_code}" -eq 0 && "${err_count}" -eq 0 && "${timeout_count}" -eq 0 && "${failed_tests_count}" -eq 0 ]]; then
        status="PASS"
    fi

    local details
    details="mode=full; isolated_unit=${unit}; timeout_seconds=${effective_timeout:-default}; exit_code=${exit_code}; exit_reason=${exit_reason}; wall_timeout_seconds=${RUNTEXT_WALL_TIMEOUT_SECONDS}; ok=${ok_count}; err=${err_count}; timeout=${timeout_count}; ignore=${ignore_count}; failed_tests=${failed_tests_count}"
    record_result "${case_name}" "${status}" "${details}"
}

reset_expiration_debug_state() {
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw DEBUG PAUSE-CRON 0 >/dev/null 2>&1 || true
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw DEBUG SET-ACTIVE-EXPIRE 1 >/dev/null 2>&1 || true
}

reset_server_after_runtest() {
    # External runtest can leave the server in BUSY_SCRIPT and/or read-only
    # replica state. Clear these so post-run probes observe steady behavior.
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw SCRIPT KILL >/dev/null 2>&1 || true
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw FUNCTION KILL >/dev/null 2>&1 || true
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw REPLICAOF NO ONE >/dev/null 2>&1 || true
    redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw CLIENT UNPAUSE >/dev/null 2>&1 || true
    reset_expiration_debug_state
    if ! wait_for_server_not_busy 100; then
        echo "warning: server remained BUSY after reset attempts; restarting server for CLI probes" >&2
        if restart_garnet_server; then
            reset_expiration_debug_state
            wait_for_server_not_busy 50 >/dev/null 2>&1 || true
        else
            echo "warning: failed to restart server for post-run probes" >&2
        fi
    fi
}

wait_for_server_not_busy() {
    local max_attempts="${1:-50}"
    local ping_output=""
    local attempt=0

    while (( attempt < max_attempts )); do
        ping_output="$(redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw PING 2>&1 || true)"
        if [[ "${ping_output}" == "PONG" ]]; then
            return 0
        fi
        # Pause tests may leave CLIENT PAUSE active; clear it so probe cleanup
        # and post-runtest redis-cli checks cannot block indefinitely.
        redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw CLIENT UNPAUSE >/dev/null 2>&1 || true
        if [[ "${ping_output}" == *"BUSY Redis is busy running a script"* ]]; then
            redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw SCRIPT KILL >/dev/null 2>&1 || true
            redis-cli -h 127.0.0.1 -p "${GARNET_PORT}" --raw FUNCTION KILL >/dev/null 2>&1 || true
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    return 1
}

run_cli_probe_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    wait_for_server_not_busy 20 >/dev/null 2>&1 || true
    if {
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
    } >"${log_file}" 2>&1; then
        record_result "${case_name}" "PASS" "redis-cli TYPE probe passed"
    else
        record_result "${case_name}" "FAIL" "redis-cli TYPE probe failed"
    fi
}

run_cli_scripting_probe_case() {
    local case_name="$1"
    local log_file="${RESULT_DIR}/${case_name}.log"
    local status="FAIL"
    local details=""
    wait_for_server_not_busy 20 >/dev/null 2>&1 || true

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
                details="scripting_enabled_mode; eval=ok; function_load=ok; fcall_ro=ok"
            else
                status="FAIL"
                details="scripting_enabled_mode_unexpected_output"
            fi
        elif [[ "${eval_output}" == *"scripting is disabled in this server"* ]]; then
            status="PASS"
            details="scripting_disabled_mode; eval disabled as expected"
        else
            status="FAIL"
            details="unexpected_eval_output"
        fi
    } >"${log_file}" 2>&1

    record_result "${case_name}" "${status}" "${details}"
}

require_cmd redis-cli
require_cmd cargo
require_cmd pgrep
require_cmd perl
require_cmd python3
require_cmd "${RUNTEXT_BIN}"

if [[ ! -x "${RUNTEXT_BIN}" ]]; then
    echo "redis runtest binary is not executable: ${RUNTEXT_BIN}" >&2
    exit 2
fi

if [[ "${RUNTEXT_ALLOW_EXTERNAL_SKIP}" != "0" && "${RUNTEXT_ALLOW_EXTERNAL_SKIP}" != "1" ]]; then
    echo "RUNTEXT_ALLOW_EXTERNAL_SKIP must be 0 or 1" >&2
    exit 2
fi

export GARNET_EXTERNAL_ALLOW_SKIP="${RUNTEXT_ALLOW_EXTERNAL_SKIP}"

GARNET_PID=""
REDIS_SERVER_TCL_BACKUP=""
REDIS_CLI_TCL_BACKUP=""
cleanup() {
    restore_redis_cli_override_patch
    restore_redis_external_pid_patch
    stop_garnet_server
}
trap cleanup EXIT

: > "${GARNET_LOG_FILE}"

if ! install_redis_external_pid_patch; then
    exit 1
fi

if ! install_redis_cli_override_patch; then
    exit 1
fi

if ! start_garnet_server; then
    exit 1
fi

case "${REDIS_RUNTEXT_MODE}" in
    full)
        if [[ -n "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" ]]; then
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated ${RUNTEXT_RUN_ONLY_ISOLATED_UNIT} run" >&2
            fi
            reset_expiration_debug_state
            isolated_timeout_override=""
            if [[ "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" == "unit/other" ]]; then
                isolated_timeout_override="${RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS}"
            fi
            isolated_case_name="redis_runtest_${RUNTEXT_RUN_ONLY_ISOLATED_UNIT//\//_}_external"
            run_isolated_unit_case "${isolated_case_name}" "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" "${isolated_timeout_override}"
            reset_expiration_debug_state
        else
            run_full_runtest_case "redis_runtest_full_external"
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_SCRIPTING_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            # Opt-in debugging path: isolate scripting after a restart when chasing
            # DEBUG expiration-toggle contamination or scripting-only failures.
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated scripting run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case "redis_runtest_unit_scripting_external" "unit/scripting"
            reset_expiration_debug_state
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_SCRIPTING_NOREPLICAS_TEST_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated scripting no-replicas run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case \
                "redis_runtest_unit_scripting_not_enough_good_replicas_external" \
                "unit/scripting" \
                "" \
                --only "not enough good replicas"
            reset_expiration_debug_state
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_QUERYBUF_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            # Opt-in debugging path: isolate querybuf after a restart when chasing
            # query-buffer or DEBUG PAUSE-CRON contamination.
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated querybuf run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case "redis_runtest_unit_querybuf_external" "unit/querybuf"
            reset_expiration_debug_state
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_OTHER_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated unit/other run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case "redis_runtest_unit_other_external" "unit/other" "${RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS}"
            reset_expiration_debug_state
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_OTHER_PIPELINE_STRESSER_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated unit/other pipeline stresser run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case \
                "redis_runtest_unit_other_pipeline_stresser_external" \
                "unit/other" \
                "${RUNTEXT_ISOLATED_OTHER_TIMEOUT_SECONDS}" \
                --only "${RUNTEXT_OTHER_PIPELINE_STRESSER_TEST_NAME}"
            reset_expiration_debug_state
        fi
        if [[ -z "${RUNTEXT_RUN_ONLY_ISOLATED_UNIT}" && "${RUNTEXT_RUN_REDIS_CLI_CONNECTING_AS_REPLICA_TEST_ISOLATED}" == "1" && -z "${RUNTEXT_EXTRA_ARGS}" ]]; then
            if ! restart_garnet_server; then
                echo "warning: failed to restart server before isolated redis-cli replica run" >&2
            fi
            reset_expiration_debug_state
            run_isolated_unit_case \
                "redis_runtest_integration_redis_cli_connecting_as_replica_external" \
                "integration/redis-cli" \
                "" \
                --only "Connecting as a replica"
            reset_expiration_debug_state
        fi
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

reset_server_after_runtest
run_cli_probe_case "redis_cli_type_probe"
run_cli_scripting_probe_case "redis_cli_scripting_probe"

echo "redis runtest external summary (mode=${REDIS_RUNTEXT_MODE})"
cat "${SUMMARY_CSV}"
echo "result_dir=${RESULT_DIR}"
