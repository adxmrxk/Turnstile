#!/usr/bin/env bash
# Shared shell library for the Turnstile operator tooling.
#
# Sourced, never executed. Every script that sources this inherits strict mode,
# structured logging, and a stack trace on unexpected failure.

# ---------------------------------------------------------------------------
# Strict mode
# ---------------------------------------------------------------------------
#   -e            stop on the first unhandled non-zero exit
#   -u            an unset variable is a bug, not an empty string
#   -o pipefail   a failure anywhere in a pipeline fails the pipeline, which
#                 matters here because the verifier is one long pipeline and
#                 without this a dead awk would be masked by a healthy sort
set -euo pipefail

# Word splitting on newline and tab only. Leaving space in IFS is what turns a
# filename with a space in it into two arguments.
IFS=$'\n\t'

# ---------------------------------------------------------------------------
# Colour, only when attached to a terminal
# ---------------------------------------------------------------------------
# Piping output to a file or a log collector must not embed escape codes, so
# these collapse to empty strings when stdout is not a tty. NO_COLOR is honoured
# because it is the convention operators expect.
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  readonly C_RESET=$'\033[0m'
  readonly C_RED=$'\033[0;31m'
  readonly C_GREEN=$'\033[0;32m'
  readonly C_YELLOW=$'\033[0;33m'
  readonly C_BLUE=$'\033[0;34m'
  readonly C_DIM=$'\033[2m'
  readonly C_BOLD=$'\033[1m'
else
  readonly C_RESET='' C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_DIM='' C_BOLD=''
fi

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Everything except explicit program output goes to stderr, so a script's stdout
# stays clean enough to pipe into another tool.
readonly LOG_LEVEL="${LOG_LEVEL:-info}"

_log_level_num() {
  case "$1" in
    debug) echo 10 ;;
    info)  echo 20 ;;
    warn)  echo 30 ;;
    error) echo 40 ;;
    *)     echo 20 ;;
  esac
}

_log() {
  local level="$1" colour="$2" message="$3"
  if (( $(_log_level_num "$level") < $(_log_level_num "$LOG_LEVEL") )); then
    return 0
  fi
  printf '%s%-5s%s %s%s%s %s\n' \
    "$colour" "${level^^}" "$C_RESET" \
    "$C_DIM" "$(date -u '+%H:%M:%S')" "$C_RESET" \
    "$message" >&2
}

log_debug() { _log debug "$C_DIM"    "$*"; }
log_info()  { _log info  "$C_BLUE"   "$*"; }
log_warn()  { _log warn  "$C_YELLOW" "$*"; }
log_error() { _log error "$C_RED"    "$*"; }

log_ok()   { printf '%s  ok%s   %s\n' "$C_GREEN" "$C_RESET" "$*" >&2; }
log_fail() { printf '%s fail%s   %s\n' "$C_RED" "$C_RESET" "$*" >&2; }

die() {
  log_error "$*"
  exit 1
}

# ---------------------------------------------------------------------------
# Failure diagnostics
# ---------------------------------------------------------------------------
# `set -e` tells you a script died but not where. This prints the call stack,
# which is the difference between a five second fix and reading the whole script.
_on_error() {
  local exit_code=$1 line=$2 command=$3
  log_error "command failed with exit ${exit_code} at line ${line}: ${command}"
  local frame=0
  while caller "$frame" >/dev/null 2>&1; do
    # caller prints: <line> <function> <file>
    local info
    info="$(caller "$frame")"
    printf '        at %s\n' "$info" >&2
    frame=$((frame + 1))
  done
  exit "$exit_code"
}

enable_error_trace() {
  # ERR must be inherited by functions and subshells or the trap silently does
  # nothing inside the very places failures tend to happen.
  set -o errtrace
  trap '_on_error "$?" "$LINENO" "$BASH_COMMAND"' ERR
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
# Temp files are registered rather than removed ad hoc, so an early exit or a
# Ctrl-C still cleans up. Without the EXIT trap a failed run leaves litter in
# /tmp forever.
declare -a _CLEANUP_PATHS=()

register_cleanup() {
  _CLEANUP_PATHS+=("$1")
}

_run_cleanup() {
  local path
  for path in "${_CLEANUP_PATHS[@]:-}"; do
    if [[ -n "$path" && -e "$path" ]]; then
      rm -rf -- "$path"
    fi
  done
  # An EXIT trap's return status replaces the script's own. Written as
  # `[[ ... ]] && rm` this function ended on a false test whenever there was
  # nothing to clean, returned 1, and silently turned every successful run into
  # a failure. The explicit return is not decoration; it is the fix.
  return 0
}

enable_cleanup() {
  trap _run_cleanup EXIT INT TERM
}

# Creates a temp dir and registers it for cleanup.
#
# Takes the name of a variable to assign rather than echoing the path, because
# the natural calling style for an echoing version is
#
#     work="$(make_temp_dir)"
#
# and command substitution runs the function in a subshell. register_cleanup
# would then append to the subshell's copy of _CLEANUP_PATHS, which is discarded
# the moment the substitution ends. The parent's EXIT trap finds an empty list
# and removes nothing, so every run leaks a directory into /tmp forever. That is
# not hypothetical: it leaked eight of them before this was caught.
#
# Assigning through a nameref keeps the call in the caller's own shell, so the
# registration survives.
#
#     local work; make_temp_dir work
make_temp_dir() {
  local -n _out_var="$1"
  local dir
  dir="$(mktemp -d "${TMPDIR:-/tmp}/turnstile.XXXXXXXX")"
  register_cleanup "$dir"
  _out_var="$dir"
}

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------
require_command() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || die "required command not found on PATH: ${cmd}"
  done
}

# Resolves the repository root from this file's own location, so scripts work
# regardless of the directory they are invoked from. BASH_SOURCE[0] is this
# library, not the caller, which is exactly what we want.
repo_root() {
  # The cd runs in a subshell. Without the parentheses this function would
  # change the caller's working directory as a side effect of being asked a
  # question, which then silently breaks every relative path used after it.
  ( cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd )
}
