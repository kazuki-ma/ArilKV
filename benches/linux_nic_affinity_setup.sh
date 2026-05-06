#!/usr/bin/env bash
set -euo pipefail

MODE="show"
IFACE=""
SERVER_CPU_LIST=""
ENABLE_RFS="1"
RPS_SOCK_FLOW_ENTRIES="32768"
RPS_CPUMASK=""
XPS_CPUMASK=""
DRY_RUN="0"
STATE_LOG=""

usage() {
  cat <<'EOF'
Usage:
  linux_nic_affinity_setup.sh --iface IFACE [options]

Modes:
  --mode show|apply        Default: show

Apply options:
  --server-cpus LIST       CPU list for IRQ pinning (e.g. 0-3,8)
  --enable-rfs 0|1         Default: 1
  --rfs-entries N          net.core.rps_sock_flow_entries value (default: 32768)
  --rps-cpumask MASK       Optional hex cpumask for rx-*/rps_cpus (e.g. f)
  --xps-cpumask MASK       Optional hex cpumask for tx-*/xps_cpus (e.g. f0)
  --dry-run 0|1            Print actions without writing
  --state-log PATH         Optional log path (default: /tmp/garnet-nic-affinity-<iface>-<ts>.log)

Examples:
  ./benches/linux_nic_affinity_setup.sh --iface eth0 --mode show
  sudo ./benches/linux_nic_affinity_setup.sh --iface eth0 --mode apply --server-cpus 0-3 --enable-rfs 1 --rfs-entries 65536
  sudo ./benches/linux_nic_affinity_setup.sh --iface eth0 --mode apply --server-cpus 0-3 --rps-cpumask f --xps-cpumask f
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"; shift 2 ;;
    --iface)
      IFACE="${2:-}"; shift 2 ;;
    --server-cpus)
      SERVER_CPU_LIST="${2:-}"; shift 2 ;;
    --enable-rfs)
      ENABLE_RFS="${2:-}"; shift 2 ;;
    --rfs-entries)
      RPS_SOCK_FLOW_ENTRIES="${2:-}"; shift 2 ;;
    --rps-cpumask)
      RPS_CPUMASK="${2:-}"; shift 2 ;;
    --xps-cpumask)
      XPS_CPUMASK="${2:-}"; shift 2 ;;
    --dry-run)
      DRY_RUN="${2:-}"; shift 2 ;;
    --state-log)
      STATE_LOG="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

if [[ -z "${IFACE}" ]]; then
  echo "--iface is required" >&2
  usage
  exit 1
fi

if [[ "${MODE}" != "show" && "${MODE}" != "apply" ]]; then
  echo "--mode must be show or apply" >&2
  exit 1
fi

if [[ "${MODE}" == "apply" ]]; then
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "apply mode requires root (run with sudo)" >&2
    exit 1
  fi
  if [[ -z "${SERVER_CPU_LIST}" ]]; then
    echo "--server-cpus is required in apply mode" >&2
    exit 1
  fi
fi

if [[ ! -d "/sys/class/net/${IFACE}" ]]; then
  echo "interface not found: ${IFACE}" >&2
  exit 1
fi

if [[ -z "${STATE_LOG}" ]]; then
  ts="$(date +%Y%m%d-%H%M%S)"
  STATE_LOG="/tmp/garnet-nic-affinity-${IFACE}-${ts}.log"
fi

run_or_echo() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[dry-run] $*"
  else
    eval "$@"
  fi
}

log_line() {
  printf '%s\n' "$*" >>"${STATE_LOG}"
}

collect_irqs() {
  local irq_dir="/sys/class/net/${IFACE}/device/msi_irqs"
  if [[ -d "${irq_dir}" ]]; then
    ls -1 "${irq_dir}" 2>/dev/null | sort -n
    return 0
  fi
  awk -F: -v iface="${IFACE}" '$0 ~ iface {gsub(/[[:space:]]+/, "", $1); if ($1 ~ /^[0-9]+$/) print $1}' /proc/interrupts | sort -n
}

expand_cpu_list() {
  local list="$1"
  local -n out_ref="$2"
  out_ref=()
  IFS=',' read -ra parts <<<"${list}"
  for part in "${parts[@]}"; do
    if [[ "${part}" =~ ^[0-9]+-[0-9]+$ ]]; then
      local start="${part%-*}"
      local end="${part#*-}"
      if (( end < start )); then
        echo "invalid cpu range: ${part}" >&2
        exit 1
      fi
      local cpu
      for ((cpu=start; cpu<=end; cpu++)); do
        out_ref+=("${cpu}")
      done
    elif [[ "${part}" =~ ^[0-9]+$ ]]; then
      out_ref+=("${part}")
    else
      echo "invalid cpu token in list: ${part}" >&2
      exit 1
    fi
  done
  if [[ "${#out_ref[@]}" -eq 0 ]]; then
    echo "cpu list is empty: ${list}" >&2
    exit 1
  fi
}

show_state() {
  echo "interface=${IFACE}"
  echo "rx_queues=$(find "/sys/class/net/${IFACE}/queues" -maxdepth 1 -type d -name 'rx-*' | wc -l | tr -d ' ')"
  echo "tx_queues=$(find "/sys/class/net/${IFACE}/queues" -maxdepth 1 -type d -name 'tx-*' | wc -l | tr -d ' ')"
  if command -v ethtool >/dev/null 2>&1; then
    echo "---- ethtool -l ${IFACE} ----"
    ethtool -l "${IFACE}" 2>/dev/null || true
  fi

  echo "---- IRQ affinity ----"
  local irq
  while read -r irq; do
    [[ -z "${irq}" ]] && continue
    local aff
    aff="$(cat "/proc/irq/${irq}/smp_affinity_list" 2>/dev/null || echo "?")"
    echo "irq=${irq} smp_affinity_list=${aff}"
  done < <(collect_irqs)

  echo "---- RFS/RPS ----"
  if [[ -f /proc/sys/net/core/rps_sock_flow_entries ]]; then
    echo "net.core.rps_sock_flow_entries=$(cat /proc/sys/net/core/rps_sock_flow_entries)"
  fi
  local rxq
  for rxq in /sys/class/net/"${IFACE}"/queues/rx-*; do
    [[ -d "${rxq}" ]] || continue
    local qname
    qname="$(basename "${rxq}")"
    local rps_cpus="n/a"
    local rps_flow_cnt="n/a"
    [[ -f "${rxq}/rps_cpus" ]] && rps_cpus="$(cat "${rxq}/rps_cpus")"
    [[ -f "${rxq}/rps_flow_cnt" ]] && rps_flow_cnt="$(cat "${rxq}/rps_flow_cnt")"
    echo "${qname} rps_cpus=${rps_cpus} rps_flow_cnt=${rps_flow_cnt}"
  done

  echo "---- XPS ----"
  local txq
  for txq in /sys/class/net/"${IFACE}"/queues/tx-*; do
    [[ -d "${txq}" ]] || continue
    local qname
    qname="$(basename "${txq}")"
    local xps_cpus="n/a"
    [[ -f "${txq}/xps_cpus" ]] && xps_cpus="$(cat "${txq}/xps_cpus")"
    echo "${qname} xps_cpus=${xps_cpus}"
  done
}

apply_state() {
  : >"${STATE_LOG}"
  log_line "# garnet nic affinity apply log"
  log_line "# iface=${IFACE}"
  log_line "# ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log_line "# dry_run=${DRY_RUN}"
  log_line "# server_cpus=${SERVER_CPU_LIST}"
  log_line "# enable_rfs=${ENABLE_RFS}"
  log_line "# rfs_entries=${RPS_SOCK_FLOW_ENTRIES}"
  log_line "# rps_cpumask=${RPS_CPUMASK}"
  log_line "# xps_cpumask=${XPS_CPUMASK}"

  local -a cpus
  expand_cpu_list "${SERVER_CPU_LIST}" cpus

  local -a irqs=()
  mapfile -t irqs < <(collect_irqs)
  if [[ "${#irqs[@]}" -eq 0 ]]; then
    echo "no IRQ entries found for interface ${IFACE}" >&2
    exit 1
  fi

  local i irq cpu old_aff
  for ((i=0; i<${#irqs[@]}; i++)); do
    irq="${irqs[$i]}"
    cpu="${cpus[$((i % ${#cpus[@]}))]}"
    old_aff="$(cat "/proc/irq/${irq}/smp_affinity_list" 2>/dev/null || echo "?")"
    log_line "irq,${irq},old=${old_aff},new=${cpu}"
    run_or_echo "echo '${cpu}' > '/proc/irq/${irq}/smp_affinity_list'"
  done

  if [[ "${ENABLE_RFS}" == "1" ]]; then
    if [[ -f /proc/sys/net/core/rps_sock_flow_entries ]]; then
      local old_global
      old_global="$(cat /proc/sys/net/core/rps_sock_flow_entries)"
      log_line "rps_sock_flow_entries,old=${old_global},new=${RPS_SOCK_FLOW_ENTRIES}"
      run_or_echo "echo '${RPS_SOCK_FLOW_ENTRIES}' > /proc/sys/net/core/rps_sock_flow_entries"
    fi

    local -a rxqs=()
    mapfile -t rxqs < <(find "/sys/class/net/${IFACE}/queues" -maxdepth 1 -type d -name 'rx-*' | sort)
    local qcount="${#rxqs[@]}"
    if (( qcount > 0 )); then
      local per_queue=$((RPS_SOCK_FLOW_ENTRIES / qcount))
      if (( per_queue < 1 )); then
        per_queue=1
      fi
      local rxq old_cnt old_mask
      for rxq in "${rxqs[@]}"; do
        if [[ -f "${rxq}/rps_flow_cnt" ]]; then
          old_cnt="$(cat "${rxq}/rps_flow_cnt")"
          log_line "rps_flow_cnt,queue=$(basename "${rxq}"),old=${old_cnt},new=${per_queue}"
          run_or_echo "echo '${per_queue}' > '${rxq}/rps_flow_cnt'"
        fi
        if [[ -n "${RPS_CPUMASK}" && -f "${rxq}/rps_cpus" ]]; then
          old_mask="$(cat "${rxq}/rps_cpus")"
          log_line "rps_cpus,queue=$(basename "${rxq}"),old=${old_mask},new=${RPS_CPUMASK}"
          run_or_echo "echo '${RPS_CPUMASK}' > '${rxq}/rps_cpus'"
        fi
      done
    fi
  fi

  if [[ -n "${XPS_CPUMASK}" ]]; then
    local txq old_xps
    for txq in /sys/class/net/"${IFACE}"/queues/tx-*; do
      [[ -d "${txq}" ]] || continue
      if [[ -f "${txq}/xps_cpus" ]]; then
        old_xps="$(cat "${txq}/xps_cpus")"
        log_line "xps_cpus,queue=$(basename "${txq}"),old=${old_xps},new=${XPS_CPUMASK}"
        run_or_echo "echo '${XPS_CPUMASK}' > '${txq}/xps_cpus'"
      fi
    done
  fi
}

if [[ "${MODE}" == "show" ]]; then
  show_state
  exit 0
fi

apply_state
echo "applied NIC affinity settings for ${IFACE}"
echo "state_log=${STATE_LOG}"
echo "current_state:"
show_state
