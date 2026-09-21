#!/usr/bin/env bash
#
# verify-invariants.sh -- prove from the event log alone that no seat was
#                         oversold, using nothing but standard Unix tools.
#
# WHY THIS IS NOT A JAVA TEST
#
# The application already asserts it never oversells. But the application is the
# thing under suspicion: if a bug in the aggregate lets a seat sell twice, the
# same aggregate is unlikely to notice. An independent checker that shares no
# code, no libraries, and no assumptions with the system it audits is worth more
# than another assertion inside it.
#
# So this reads the exported log as plain text and re-derives the invariants
# with awk. It does not know what a SeatAggregate is. If it disagrees with the
# application, the application is wrong.
#
# INVARIANTS CHECKED
#
#   I1  no seat appears as SeatSold more than once
#   I2  every SeatSold refers to a hold that was actually placed on that seat
#   I3  nothing happens to a seat after it is sold
#   I4  every hold is eventually resolved: sold, released, or still open at the
#       end of the window (a hold that simply vanishes means a lost event)
#
# INPUT
#
#   NDJSON, one event per line, as written by `turnstilectl simulate` (an export
#   of a live log arrives with the Postgres store in phase 2):
#     {"seq":1,"type":"SeatHeld","seatId":"seat-1","holdId":"h1",...}
#
# EXIT CODES
#
#   0  all invariants hold
#   1  at least one invariant violated
#   2  bad usage or unreadable input

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
enable_error_trace
enable_cleanup

readonly PROGRAM="${0##*/}"

usage() {
  cat <<'USAGE'
Usage: verify-invariants.sh [-f FILE] [-q] [-s] [-h]

Verifies the Turnstile event log never oversold a seat, independently of the
application that produced it.

Options:
  -f FILE   read the log from FILE (default: stdin)
  -q        quiet: print nothing on success, only violations
  -s        also print per-seat summary statistics
  -h        show this help

Exit codes:
  0  every invariant holds
  1  at least one invariant was violated
  2  usage error or unreadable input

Examples:
  turnstilectl simulate | verify-invariants.sh
  verify-invariants.sh -f build/events.ndjson -s
USAGE
}

main() {
  local input='-' quiet=0 summary=0

  # getopts rather than hand-rolled parsing: it handles bundling (-qs), gives
  # consistent error messages, and stops at the first non-option argument.
  while getopts ':f:qsh' opt; do
    case "$opt" in
      f) input="$OPTARG" ;;
      q) quiet=1 ;;
      s) summary=1 ;;
      h) usage; exit 0 ;;
      :) usage >&2; die "option -${OPTARG} requires an argument" ;;
      \?) usage >&2; die "unknown option: -${OPTARG}" ;;
      *) usage >&2; exit 2 ;;
    esac
  done
  shift $((OPTIND - 1))

  if [[ $# -gt 0 ]]; then
    usage >&2
    die "unexpected argument: $1"
  fi

  if [[ "$input" != '-' && ! -r "$input" ]]; then
    log_error "cannot read log file: ${input}"
    exit 2
  fi

  # Resolved once, explicitly. A ${input/-/...} substitution here silently
  # corrupted any path containing a hyphen, which is most of them.
  local source_file="$input" source_label="$input"
  if [[ "$input" == '-' ]]; then
    source_file='/dev/stdin'
    source_label='stdin'
  fi

  [[ $quiet -eq 1 ]] || log_info "verifying invariants over ${source_label}"

  # A single awk pass over the log. One pass matters: a real export is millions
  # of events and this has to stay streaming rather than loading it all.
  #
  # The field extraction is deliberately tolerant rather than a real JSON parse.
  # The export format is ours and is flat, so a targeted match beats taking a
  # dependency on jq for a script whose whole point is to have no dependencies.
  local awk_status=0
  # shellcheck disable=SC2016
  awk -v quiet="$quiet" -v summary="$summary" '
    # Extracts one string field. The value pattern allows backslash escapes, so
    # a seat id containing an escaped quote is read whole instead of being
    # truncated at the first quote and dragging the rest of the line in with it.
    # Extracts one string field.
    #
    # The value pattern allows backslash escapes. Without that it stopped at the
    # first quote, so a value containing an escaped quote was truncated and the
    # remainder of the line was dragged into the next field.
    #
    # The value is returned still escaped, deliberately. It is only ever used as
    # an opaque key for equality and grouping, so consistent escaping is all the
    # comparison needs, and an unescaping pass is extra machinery that could
    # itself be wrong.
    function field(line, name,    re, hit) {
      re = "\"" name "\"[[:space:]]*:[[:space:]]*\"(\\\\.|[^\"\\\\])*\""
      if (match(line, re)) {
        hit = substr(line, RSTART, RLENGTH)
        sub(/^"[^"]*"[[:space:]]*:[[:space:]]*"/, "", hit)
        sub(/"$/, "", hit)
        return hit
      }
      return ""
    }

    function violation(code, message) {
      violations++
      printf "VIOLATION %s  line %d: %s\n", code, NR, message
    }

    /^[[:space:]]*$/ { next }

    {
      total++
      type   = field($0, "type")
      seat   = field($0, "seatId")
      hold   = field($0, "holdId")

      if (type == "" || seat == "") {
        violation("E0", "unparseable event: " substr($0, 1, 120))
        next
      }

      # I3 -- a sold seat is terminal. Any later event for it means the sale was
      # not final, which is the same class of bug as an oversell.
      if (seat in sold) {
        violation("I3", "event " type " on seat " seat " after it was sold")
      }

      if (type == "SeatHeld") {
        held[seat, hold] = NR
        openHolds[seat, hold] = 1
        holdCount[seat]++
      }
      else if (type == "HoldReleased") {
        delete openHolds[seat, hold]
        releaseCount[seat]++
      }
      else if (type == "SeatSold") {
        # I1 -- the headline invariant.
        if (seat in sold) {
          violation("I1", "seat " seat " sold a second time (first at line " sold[seat] ")")
        } else {
          sold[seat] = NR
          soldCount++
        }

        # I2 -- a sale must descend from a hold that was actually placed.
        if (!((seat, hold) in held)) {
          violation("I2", "seat " seat " sold via hold " hold " that was never placed")
        }
        delete openHolds[seat, hold]
      }
    }

    END {
      # I4 -- a hold that neither sold nor released means an event went missing
      # between the writer and this export.
      for (key in openHolds) {
        split(key, parts, SUBSEP)
        # Open holds are legitimate at the edge of the window, so this is
        # reported as informational rather than a violation.
        dangling++
        if (!quiet) {
          printf "INFO      I4  seat %s still has open hold %s at end of log\n", parts[1], parts[2]
        }
      }

      # An empty log proves nothing, and it is exactly what a failed export
      # upstream of a pipe looks like. Say so loudly rather than report a clean
      # audit of nothing. (Callers should also use pipefail.)
      if (total + 0 == 0 && !quiet) {
        printf "WARN      the log contained no events; this audited nothing\n" > "/dev/stderr"
      }

      if (summary) {
        printf "\nSUMMARY\n"
        printf "  events read      %d\n", total
        printf "  seats sold       %d\n", soldCount
        printf "  holds still open %d\n", dangling + 0
      }

      # Machine-readable result line, suppressed under -q so that quiet really
      # means silent-unless-broken and a CI step can branch on emptiness.
      if (!quiet) {
        printf "TOTALS %d %d %d\n", total, soldCount, violations + 0
      }
      exit (violations > 0 ? 1 : 0)
    }
  ' "$source_file" || awk_status=$?

  if [[ $awk_status -eq 1 ]]; then
    log_fail "invariant violations detected: the log describes an oversell"
    return 1
  elif [[ $awk_status -ne 0 ]]; then
    die "awk failed with status ${awk_status}"
  fi

  [[ $quiet -eq 1 ]] || log_ok "no seat was sold twice; every sale descends from a real hold"
  return 0
}

main "$@"
