#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPAT_DIR="${WORKSPACE_ROOT}/docs/compatibility"

STATUS_SCRIPT="${STATUS_SCRIPT:-${SCRIPT_DIR}/build_command_status_matrix.sh}"
MATURITY_SCRIPT="${MATURITY_SCRIPT:-${SCRIPT_DIR}/build_command_maturity_matrix.sh}"
SUBSET_SCRIPT="${SUBSET_SCRIPT:-${SCRIPT_DIR}/redis_runtest_external_subset.sh}"
COMPAT_PROBE_MODE="${COMPAT_PROBE_MODE:-full}"

STATUS_CSV="${STATUS_CSV:-${COMPAT_DIR}/redis-command-status.csv}"
MATURITY_CSV="${MATURITY_CSV:-${COMPAT_DIR}/redis-command-maturity.csv}"
REPORT_MD="${REPORT_MD:-${COMPAT_DIR}/compatibility-report.md}"

RESULT_ROOT="${RESULT_ROOT:-${SCRIPT_DIR}/results/compatibility-report-$(date +%Y%m%d-%H%M%S)}"
PROBE_RESULT_DIR="${PROBE_RESULT_DIR:-${RESULT_ROOT}/redis-runtest-external-${COMPAT_PROBE_MODE}}"
PROBE_SUMMARY_CSV="${PROBE_RESULT_DIR}/summary.csv"
PROBE_HEARTBEAT_ENABLED="${PROBE_HEARTBEAT_ENABLED:-1}"
PROBE_HEARTBEAT_INTERVAL_SECONDS="${PROBE_HEARTBEAT_INTERVAL_SECONDS:-30}"

choose_probe_progress_log() {
    if [[ -f "${PROBE_RESULT_DIR}/redis_runtest_full_external.log" ]]; then
        echo "${PROBE_RESULT_DIR}/redis_runtest_full_external.log"
        return
    fi

    local latest_log
    latest_log="$(
        find "${PROBE_RESULT_DIR}" -maxdepth 1 -type f -name 'redis_runtest_*.log' 2>/dev/null \
            | sort \
            | tail -n 1
    )"
    if [[ -n "${latest_log}" ]]; then
        echo "${latest_log}"
    fi
}

probe_progress_snapshot() {
    local log_file="$1"
    awk '
    BEGIN {
        esc = sprintf("%c", 27)
        ok = 0
        err = 0
        ignore = 0
        timeout = 0
        latest = ""
    }
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
        } else if (line ~ /^\[TIMEOUT\]:/) {
            timeout++
        }
        if (line ~ /^Testing / || line ~ /^\[[0-9]+\/[0-9]+ .*done\]:/) {
            latest = line
        }
    }
    END {
        if (latest == "") {
            latest = "progress pending"
        }
        gsub(/"/, "\\\"", latest)
        printf("ok=%d err=%d ignore=%d timeout=%d latest=\"%s\"", ok, err, ignore, timeout, latest)
    }
    ' "${log_file}"
}

run_external_probe_with_heartbeat() {
    RESULT_DIR="${PROBE_RESULT_DIR}" REDIS_RUNTEXT_MODE="${COMPAT_PROBE_MODE}" "${SUBSET_SCRIPT}" &
    local probe_pid="$!"
    local start_epoch
    start_epoch="$(date +%s)"

    while kill -0 "${probe_pid}" >/dev/null 2>&1; do
        sleep "${PROBE_HEARTBEAT_INTERVAL_SECONDS}"
        if ! kill -0 "${probe_pid}" >/dev/null 2>&1; then
            break
        fi

        local now_epoch elapsed_seconds progress_log progress_snapshot
        now_epoch="$(date +%s)"
        elapsed_seconds="$(( now_epoch - start_epoch ))"
        progress_log="$(choose_probe_progress_log)"
        if [[ -n "${progress_log}" && -f "${progress_log}" ]]; then
            progress_snapshot="$(probe_progress_snapshot "${progress_log}")"
        else
            progress_snapshot="probe-log-not-yet-created"
        fi
        echo "[3/4] heartbeat: elapsed=${elapsed_seconds}s ${progress_snapshot}"
    done

    wait "${probe_pid}"
}

if [[ ! -x "${STATUS_SCRIPT}" ]]; then
    echo "missing executable: ${STATUS_SCRIPT}" >&2
    exit 1
fi
if [[ ! -x "${MATURITY_SCRIPT}" ]]; then
    echo "missing executable: ${MATURITY_SCRIPT}" >&2
    exit 1
fi
if [[ ! -x "${SUBSET_SCRIPT}" ]]; then
    echo "missing executable: ${SUBSET_SCRIPT}" >&2
    exit 1
fi
if [[ "${PROBE_HEARTBEAT_ENABLED}" != "0" ]]; then
    if [[ ! "${PROBE_HEARTBEAT_INTERVAL_SECONDS}" =~ ^[0-9]+$ ]] \
        || [[ "${PROBE_HEARTBEAT_INTERVAL_SECONDS}" -le 0 ]]; then
        echo "invalid PROBE_HEARTBEAT_INTERVAL_SECONDS: ${PROBE_HEARTBEAT_INTERVAL_SECONDS}" >&2
        exit 1
    fi
fi

case "${COMPAT_PROBE_MODE}" in
    full)
        ;;
    subset)
        ;;
    *)
        echo "invalid COMPAT_PROBE_MODE: ${COMPAT_PROBE_MODE} (expected: subset|full)" >&2
        exit 1
        ;;
esac

mkdir -p "${RESULT_ROOT}"

echo "[1/4] building declared command status matrix..."
"${STATUS_SCRIPT}"

echo "[2/4] building command maturity matrix from yaml..."
"${MATURITY_SCRIPT}"

echo "[3/4] running redis runtest external probe (mode=${COMPAT_PROBE_MODE})..."
PROBE_SCRIPT_EXIT_CODE=0
if [[ "${PROBE_HEARTBEAT_ENABLED}" == "0" ]]; then
    if RESULT_DIR="${PROBE_RESULT_DIR}" REDIS_RUNTEXT_MODE="${COMPAT_PROBE_MODE}" "${SUBSET_SCRIPT}"; then
        PROBE_SCRIPT_EXIT_CODE=0
    else
        PROBE_SCRIPT_EXIT_CODE=$?
    fi
else
    if run_external_probe_with_heartbeat; then
        PROBE_SCRIPT_EXIT_CODE=0
    else
        PROBE_SCRIPT_EXIT_CODE=$?
    fi
fi
if [[ "${PROBE_SCRIPT_EXIT_CODE}" -eq 0 ]]; then
    PROBE_SCRIPT_EXIT_CODE=0
else
    echo "warning: external probe script exited non-zero (${PROBE_SCRIPT_EXIT_CODE}); continuing report generation from captured artifacts" >&2
fi

if [[ ! -f "${STATUS_CSV}" ]]; then
    echo "status csv not found: ${STATUS_CSV}" >&2
    exit 1
fi
if [[ ! -f "${MATURITY_CSV}" ]]; then
    echo "maturity csv not found: ${MATURITY_CSV}" >&2
    exit 1
fi
if [[ ! -f "${PROBE_SUMMARY_CSV}" ]]; then
    echo "probe summary csv not found: ${PROBE_SUMMARY_CSV}" >&2
    exit 1
fi

redis_total="$(awk -F, 'NR>1 && $3=="1" {c++} END {print c+0}' "${STATUS_CSV}")"
garnet_total="$(awk -F, 'NR>1 && $4=="1" {c++} END {print c+0}' "${STATUS_CSV}")"
supported_declared="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" {c++} END {print c+0}' "${STATUS_CSV}")"
not_implemented="$(awk -F, 'NR>1 && $2=="NOT_IMPLEMENTED" {c++} END {print c+0}' "${STATUS_CSV}")"
extensions="$(awk -F, 'NR>1 && $2=="GARNET_EXTENSION" {c++} END {print c+0}' "${STATUS_CSV}")"

full_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="FULL" {c++} END {print c+0}' "${MATURITY_CSV}")"
partial_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="PARTIAL_MINIMAL" {c++} END {print c+0}' "${MATURITY_CSV}")"
disabled_total="$(awk -F, 'NR>1 && $2=="SUPPORTED_DECLARED" && $5=="DISABLED" {c++} END {print c+0}' "${MATURITY_CSV}")"
full_ratio="$(awk -v full="${full_total}" -v total="${supported_declared}" 'BEGIN { if (total==0) print "0.00"; else printf "%.2f", (full*100.0)/total }')"

probe_pass="$(awk -F, 'NR>1 && $2=="PASS" {c++} END {print c+0}' "${PROBE_SUMMARY_CSV}")"
probe_pass_known_gaps="$(awk -F, 'NR>1 && $2=="PASS_WITH_KNOWN_GAPS" {c++} END {print c+0}' "${PROBE_SUMMARY_CSV}")"
probe_fail="$(awk -F, 'NR>1 && $2=="FAIL" {c++} END {print c+0}' "${PROBE_SUMMARY_CSV}")"
probe_total="$(awk -F, 'NR>1 {c++} END {print c+0}' "${PROBE_SUMMARY_CSV}")"
FAILED_TESTS_FILE="${PROBE_RESULT_DIR}/failed-tests.txt"
FAILED_TESTS_COUNT=0
if [[ -f "${FAILED_TESTS_FILE}" ]]; then
    FAILED_TESTS_COUNT="$(awk 'END {print NR+0}' "${FAILED_TESTS_FILE}")"
fi

echo "[4/4] generating compatibility report..."
{
    echo "# Compatibility Report (Auto-Generated)"
    echo
    echo "> This file is generated by \`garnet-rs/tests/interop/build_compatibility_report.sh\`."
    echo "> Do not edit manually."
    echo
    echo "- External probe mode: \`${COMPAT_PROBE_MODE}\`"
    echo
    echo "## Declaration Coverage Snapshot"
    echo
    echo "- Redis baseline command count: \`${redis_total}\`"
    echo "- Garnet declared command count: \`${garnet_total}\`"
    echo "- \`SUPPORTED_DECLARED\`: \`${supported_declared}\`"
    echo "- \`NOT_IMPLEMENTED\`: \`${not_implemented}\`"
    echo "- \`GARNET_EXTENSION\`: \`${extensions}\`"
    echo
    echo "## Implementation Maturity Snapshot"
    echo
    echo "- \`FULL\`: \`${full_total}\`"
    echo "- \`PARTIAL_MINIMAL\`: \`${partial_total}\`"
    echo "- \`DISABLED\`: \`${disabled_total}\`"
    echo "- Full ratio over declared commands: \`${full_ratio}%\`"
    echo
    echo "## External Probe Snapshot"
    echo
    echo "- Probe script exit code: \`${PROBE_SCRIPT_EXIT_CODE}\`"
    echo "- Cases: \`${probe_total}\`"
    echo "- PASS: \`${probe_pass}\`"
    echo "- PASS_WITH_KNOWN_GAPS: \`${probe_pass_known_gaps}\`"
    echo "- FAIL: \`${probe_fail}\`"
    echo
    echo "| Case | Status | Details |"
    echo "|---|---|---|"
    awk -F, 'NR>1 {printf("| `%s` | `%s` | %s |\n", $1, $2, $3)}' "${PROBE_SUMMARY_CSV}"

    if [[ -f "${FAILED_TESTS_FILE}" ]]; then
        echo
        echo "## External Probe Failed Tests"
        echo
        echo "- Failed tests extracted from runtest log: \`${FAILED_TESTS_COUNT}\`"
        if [[ "${FAILED_TESTS_COUNT}" -gt 0 ]]; then
            echo
            echo "| Test |"
            echo "|---|"
            awk '
            {
                line = $0
                gsub(/\|/, "\\|", line)
                printf("| `%s` |\n", line)
            }
            ' "${FAILED_TESTS_FILE}"
        fi
    fi

    echo
    echo "## Non-Full Commands (Declared Surface With Known Gaps)"
    echo
    echo "| Command | Maturity | Comment |"
    echo "|---|---|---|"
    awk -F, '
    NR>1 && $2=="SUPPORTED_DECLARED" && $5!="FULL" {
        comment = ($6 == "" ? "-" : $6);
        printf("| `%s` | `%s` | %s |\n", $1, $5, comment);
    }
    ' "${MATURITY_CSV}"
} > "${REPORT_MD}"

echo "wrote ${REPORT_MD}"
echo "probe_result_dir=${PROBE_RESULT_DIR}"
