#!/usr/bin/env bash
#
# inject.sh -- fault injection for Turnstile, using the kernel rather than a
#              library.
#
# A correctness claim that only holds on a healthy network is not worth much.
# The interesting question is whether the saga still compensates when the link
# to Postgres goes slow, when packets to Kafka drop, or when a node is frozen
# mid-transaction. This injects those conditions with tc, iptables, and signals,
# then leaves it to `verify-invariants.sh` to say whether the log survived.
#
# Using the kernel's own traffic control instead of an application-level chaos
# library matters: a library can only break paths it knows about, whereas tc
# degrades the real socket the JDBC driver is using and cannot be routed around.
#
# REQUIREMENTS
#   Linux with iproute2 and iptables, and root. This does not run on macOS or
#   on Git Bash for Windows: there is no tc there. It is written to be run on
#   the deployment host or in CI, not on a laptop.
#
# SAFETY
#   Every rule is reverted by the EXIT trap, including on Ctrl-C. Run `reset`
#   if a previous run was killed with SIGKILL and left rules behind.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"
enable_error_trace

readonly DEFAULT_IFACE="${TURNSTILE_IFACE:-eth0}"

usage() {
  cat <<'USAGE'
Usage: inject.sh <fault> [options]

Faults:
  latency  -m MS [-j MS]   add MS milliseconds of delay, optional jitter
  loss     -p PERCENT      drop PERCENT of egress packets
  partition -P PORT        blackhole traffic to PORT (Postgres 5432, Kafka 9092)
  freeze   -n NAME [-s S]  SIGSTOP a container or pid for S seconds, then SIGCONT
  reset                    remove every rule this script can create

Common options:
  -i IFACE   interface to shape (default: $TURNSTILE_IFACE or eth0)
  -d SECONDS hold the fault for SECONDS, then revert (default: 30)
  -h         this help

Examples:
  sudo ./inject.sh latency -m 300 -j 50 -d 60
  sudo ./inject.sh loss -p 10 -d 30
  sudo ./inject.sh partition -P 9092 -d 20
  sudo ./inject.sh freeze -n turnstile-app -s 15

Typical use, with the audit as the actual assertion:
  sudo ./inject.sh loss -p 15 -d 60 &
  turnstilectl simulate --buyers 20000 > events.ndjson
  wait
  ./scripts/verify-invariants.sh -f events.ndjson -s
USAGE
}

require_linux() {
  [[ "$(uname -s)" == 'Linux' ]] ||
    die "this script needs Linux traffic control; uname says $(uname -s)"
  [[ "$(id -u)" -eq 0 ]] ||
    die 'must run as root: tc and iptables need CAP_NET_ADMIN'
}

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
# Registered before any rule is added, so an interrupted run still reverts.
_IFACE_SHAPED=''
_PORT_BLOCKED=''
_FROZEN_TARGET=''

revert_all() {
  if [[ -n "$_IFACE_SHAPED" ]]; then
    log_info "removing qdisc from ${_IFACE_SHAPED}"
    tc qdisc del dev "$_IFACE_SHAPED" root 2>/dev/null || true
  fi
  if [[ -n "$_PORT_BLOCKED" ]]; then
    log_info "unblocking port ${_PORT_BLOCKED}"
    iptables -D OUTPUT -p tcp --dport "$_PORT_BLOCKED" -j DROP 2>/dev/null || true
  fi
  if [[ -n "$_FROZEN_TARGET" ]]; then
    log_info "thawing ${_FROZEN_TARGET}"
    docker unpause "$_FROZEN_TARGET" 2>/dev/null || kill -CONT "$_FROZEN_TARGET" 2>/dev/null || true
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Faults
# ---------------------------------------------------------------------------

fault_latency() {
  local iface="$1" ms="$2" jitter="$3" duration="$4"
  require_command tc

  _IFACE_SHAPED="$iface"
  log_warn "adding ${ms}ms (+/-${jitter}ms) delay on ${iface} for ${duration}s"

  # netem sits at the root of the egress qdisc, so it affects every outbound
  # connection from this host: the DB, the broker, and the health check alike.
  if [[ "$jitter" -gt 0 ]]; then
    tc qdisc add dev "$iface" root netem delay "${ms}ms" "${jitter}ms" distribution normal
  else
    tc qdisc add dev "$iface" root netem delay "${ms}ms"
  fi

  sleep "$duration"
}

fault_loss() {
  local iface="$1" percent="$2" duration="$3"
  require_command tc

  _IFACE_SHAPED="$iface"
  log_warn "dropping ${percent}% of egress packets on ${iface} for ${duration}s"

  # Packet loss is more interesting than latency for an outbox: a dropped ACK
  # means the publisher genuinely does not know whether the write landed, which
  # is the exact case idempotency keys exist for.
  tc qdisc add dev "$iface" root netem loss "${percent}%"

  sleep "$duration"
}

fault_partition() {
  local port="$1" duration="$2"
  require_command iptables

  _PORT_BLOCKED="$port"
  log_warn "blackholing tcp/${port} for ${duration}s"

  # DROP rather than REJECT on purpose. REJECT sends an RST and the client fails
  # fast; DROP makes it hang until its own timeout, which is the harsher and
  # more realistic failure and the one that actually exercises the timeout path.
  iptables -I OUTPUT -p tcp --dport "$port" -j DROP

  sleep "$duration"
}

fault_freeze() {
  local target="$1" seconds="$2"

  _FROZEN_TARGET="$target"
  log_warn "freezing ${target} for ${seconds}s"

  # A frozen process holds its locks and its socket while making no progress,
  # which is worse than a crash and is what a long GC pause or a stalled VM
  # looks like from the outside.
  if docker inspect "$target" >/dev/null 2>&1; then
    docker pause "$target"
    sleep "$seconds"
    docker unpause "$target"
  else
    kill -STOP "$target"
    sleep "$seconds"
    kill -CONT "$target"
  fi
  _FROZEN_TARGET=''
}

fault_reset() {
  local iface="$1"
  log_info 'removing every rule this script can create'
  tc qdisc del dev "$iface" root 2>/dev/null || true
  while iptables -D OUTPUT -p tcp --dport 5432 -j DROP 2>/dev/null; do :; done
  while iptables -D OUTPUT -p tcp --dport 9092 -j DROP 2>/dev/null; do :; done
  log_ok 'reset complete'
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main() {
  [[ $# -ge 1 ]] || { usage >&2; exit 2; }

  local fault="$1"; shift
  local iface="$DEFAULT_IFACE" duration=30
  local ms=100 jitter=0 percent=5 port=5432 name='' seconds=10

  while getopts ':i:d:m:j:p:P:n:s:h' opt; do
    case "$opt" in
      i) iface="$OPTARG" ;;
      d) duration="$OPTARG" ;;
      m) ms="$OPTARG" ;;
      j) jitter="$OPTARG" ;;
      p) percent="$OPTARG" ;;
      P) port="$OPTARG" ;;
      n) name="$OPTARG" ;;
      s) seconds="$OPTARG" ;;
      h) usage; exit 0 ;;
      :) die "option -${OPTARG} requires an argument" ;;
      \?) die "unknown option: -${OPTARG}" ;;
    esac
  done

  [[ "$fault" == 'reset' ]] || require_linux
  trap revert_all EXIT INT TERM

  case "$fault" in
    latency)   fault_latency "$iface" "$ms" "$jitter" "$duration" ;;
    loss)      fault_loss "$iface" "$percent" "$duration" ;;
    partition) fault_partition "$port" "$duration" ;;
    freeze)
      [[ -n "$name" ]] || die 'freeze needs -n CONTAINER_OR_PID'
      fault_freeze "$name" "$seconds"
      ;;
    reset)     fault_reset "$iface" ;;
    help|-h)   usage ;;
    *) usage >&2; die "unknown fault: ${fault}" ;;
  esac

  log_ok "fault '${fault}' finished; rules reverted"
}

main "$@"
