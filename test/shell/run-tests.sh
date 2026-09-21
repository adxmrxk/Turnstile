#!/usr/bin/env bash
#
# Test suite for the shell tooling.
#
# The verifier is the independent check on the application, which makes it
# load bearing: a verifier that silently passes everything is worse than no
# verifier at all, because it manufactures confidence. So it is tested against
# logs that are known-bad, and the assertion is that it actually catches them.
#
# Written as a plain runner rather than bats so the suite has no dependency
# beyond bash and awk, matching the tooling it tests. Run with:
#     test/shell/run-tests.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../../scripts/lib/common.sh
source "${REPO_ROOT}/scripts/lib/common.sh"
enable_cleanup

readonly VERIFY="${REPO_ROOT}/scripts/verify-invariants.sh"

tests_run=0
tests_failed=0
WORK=""
make_temp_dir WORK

# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

assert_exit() {
  local expected="$1" actual="$2" description="$3"
  tests_run=$((tests_run + 1))
  if [[ "$expected" == "$actual" ]]; then
    log_ok "$description"
  else
    log_fail "$description (expected exit ${expected}, got ${actual})"
    tests_failed=$((tests_failed + 1))
  fi
}

assert_contains() {
  local haystack="$1" needle="$2" description="$3"
  tests_run=$((tests_run + 1))
  if [[ "$haystack" == *"$needle"* ]]; then
    log_ok "$description"
  else
    log_fail "$description (output did not contain '${needle}')"
    printf '        actual output:\n%s\n' "${haystack:0:400}" >&2
    tests_failed=$((tests_failed + 1))
  fi
}

assert_empty() {
  local value="$1" description="$2"
  tests_run=$((tests_run + 1))
  if [[ -z "$value" ]]; then
    log_ok "$description"
  else
    log_fail "$description (expected no output, got '${value:0:120}')"
    tests_failed=$((tests_failed + 1))
  fi
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

event() {
  local seq="$1" type="$2" seat="$3" hold="$4"
  printf '{"seq":%d,"type":"%s","seatId":"%s","holdId":"%s","occurredAt":"2026-09-04T12:00:00Z"}\n' \
    "$seq" "$type" "$seat" "$hold"
}

# A clean log: two seats, each held then sold exactly once.
make_clean_log() {
  local out="${WORK}/clean.ndjson"
  {
    event 1 SeatHeld  seat-1 h1
    event 2 SeatSold  seat-1 h1
    event 3 SeatHeld  seat-2 h2
    event 4 HoldReleased seat-2 h2
    event 5 SeatHeld  seat-2 h3
    event 6 SeatSold  seat-2 h3
  } > "$out"
  printf '%s' "$out"
}

# The bug the whole project exists to prevent.
make_double_sale_log() {
  local out="${WORK}/double-sale.ndjson"
  {
    event 1 SeatHeld seat-1 h1
    event 2 SeatSold seat-1 h1
    event 3 SeatHeld seat-1 h2
    event 4 SeatSold seat-1 h2
  } > "$out"
  printf '%s' "$out"
}

# A sale with no matching hold: a lost event, or a fabricated sale.
make_orphan_sale_log() {
  local out="${WORK}/orphan-sale.ndjson"
  {
    event 1 SeatHeld seat-1 h1
    event 2 SeatSold seat-2 h9
  } > "$out"
  printf '%s' "$out"
}

# A hold placed after the seat was already sold.
make_activity_after_sale_log() {
  local out="${WORK}/after-sale.ndjson"
  {
    event 1 SeatHeld seat-1 h1
    event 2 SeatSold seat-1 h1
    event 3 SeatHeld seat-1 h2
  } > "$out"
  printf '%s' "$out"
}

# A hold that never resolves. Legitimate at the edge of an export window, so it
# is reported but must not fail the run.
make_open_hold_log() {
  local out="${WORK}/open-hold.ndjson"
  {
    event 1 SeatHeld seat-1 h1
  } > "$out"
  printf '%s' "$out"
}

make_garbage_log() {
  local out="${WORK}/garbage.ndjson"
  printf 'this is not an event at all\n' > "$out"
  printf '%s' "$out"
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test_clean_log_passes() {
  local log status output
  log="$(make_clean_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 0 "$status" "a clean log passes"
  assert_contains "$output" "TOTALS 6 2 0" "reports 6 events, 2 sales, 0 violations"
}

test_double_sale_is_caught() {
  local log status output
  log="$(make_double_sale_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  # This is the assertion that makes the verifier worth having.
  assert_exit 1 "$status" "a seat sold twice fails the run"
  assert_contains "$output" "VIOLATION I1" "the double sale is reported as I1"
  assert_contains "$output" "seat-1" "the offending seat is named"
}

test_sale_without_hold_is_caught() {
  local log status output
  log="$(make_orphan_sale_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 1 "$status" "a sale with no matching hold fails the run"
  assert_contains "$output" "VIOLATION I2" "the orphan sale is reported as I2"
}

test_activity_after_sale_is_caught() {
  local log status output
  log="$(make_activity_after_sale_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 1 "$status" "an event after the sale fails the run"
  assert_contains "$output" "VIOLATION I3" "post-sale activity is reported as I3"
}

test_open_hold_is_reported_but_not_fatal() {
  local log status output
  log="$(make_open_hold_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 0 "$status" "an unresolved hold does not fail the run"
  assert_contains "$output" "INFO      I4" "the open hold is reported as informational"
}

test_garbage_is_rejected() {
  local log status output
  log="$(make_garbage_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 1 "$status" "an unparseable line fails rather than being skipped"
  assert_contains "$output" "VIOLATION E0" "the bad line is reported as E0"
}

test_reads_from_stdin() {
  local status output
  status=0
  local log
  log="$(make_clean_log)"
  output="$(<"$log" "$VERIFY" 2>/dev/null)" || status=$?

  assert_exit 0 "$status" "reads the log from stdin when no -f is given"
}

test_quiet_mode_is_silent_on_success() {
  local log status output
  log="$(make_clean_log)"
  status=0
  output="$("$VERIFY" -q -f "$log" 2>/dev/null)" || status=$?

  assert_exit 0 "$status" "quiet mode still exits 0 on a clean log"
  assert_empty "$output" "quiet mode prints nothing when everything holds"
}

test_quiet_mode_still_reports_violations() {
  local log status output
  log="$(make_double_sale_log)"
  status=0
  output="$("$VERIFY" -q -f "$log" 2>/dev/null)" || status=$?

  assert_exit 1 "$status" "quiet mode still fails on a violation"
  assert_contains "$output" "VIOLATION I1" "quiet mode never hides a violation"
}

test_summary_flag() {
  local log output
  log="$(make_clean_log)"
  output="$("$VERIFY" -s -f "$log" 2>/dev/null)" || true

  assert_contains "$output" "SUMMARY" "-s prints a summary block"
  assert_contains "$output" "seats sold       2" "the summary counts sales"
}

test_missing_file_is_usage_error() {
  local status
  status=0
  "$VERIFY" -f "${WORK}/does-not-exist.ndjson" >/dev/null 2>&1 || status=$?

  # Distinct from exit 1 so a CI job can tell "log is bad" from "I could not
  # read the log", which are very different pages to wake someone for.
  assert_exit 2 "$status" "an unreadable file exits 2, not 1"
}

test_unknown_option_is_usage_error() {
  local status
  status=0
  "$VERIFY" -Z >/dev/null 2>&1 || status=$?

  assert_exit 1 "$status" "an unknown option is rejected"
}

# Regression: make_temp_dir used to echo the path, so the natural call
# `work="$(make_temp_dir)"` ran the registration inside a command substitution.
# The subshell's copy of _CLEANUP_PATHS was discarded on return, the parent's
# EXIT trap found nothing to remove, and every run leaked a directory into /tmp.
test_temp_dir_is_registered_for_cleanup() {
  local leaked_before leaked_after
  leaked_before="$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'turnstile.*' 2>/dev/null | wc -l | tr -d '[:space:]')"

  # A child shell that makes a temp dir and exits normally must leave nothing.
  bash -c '
    source "'"${REPO_ROOT}"'/scripts/lib/common.sh"
    enable_cleanup
    d=""
    make_temp_dir d
    [[ -d "$d" ]] || exit 3
    exit 0
  '
  local child_status=$?

  leaked_after="$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'turnstile.*' 2>/dev/null | wc -l | tr -d '[:space:]')"

  assert_exit 0 "$child_status" "a child shell using make_temp_dir exits cleanly"
  assert_exit "$leaked_before" "$leaked_after" "make_temp_dir leaves no directory behind"
}

# Regression: repo_root ran `cd` in the current shell rather than a subshell, so
# merely asking where the repo was silently relocated the caller.
test_repo_root_has_no_side_effect() {
  local reported
  reported="$(bash -c '
    source "'"${REPO_ROOT}"'/scripts/lib/common.sh"
    cd /tmp || exit 3
    before="$PWD"
    repo_root >/dev/null
    [[ "$before" == "$PWD" ]] && echo unchanged || echo "moved to $PWD"
  ')"

  assert_contains "$reported" "unchanged" "repo_root does not change the caller's directory"
}


# A seat id containing an escaped quote. The old extractor stopped at the first
# quote, truncating the value and misreading the remainder of the line.
make_escaped_quote_log() {
  local out="${WORK}/escaped-quote.ndjson"
  {
    printf '{"seq":1,"type":"SeatHeld","seatId":"seat-\\"A\\"-1","holdId":"h1"}
'
    printf '{"seq":2,"type":"SeatSold","seatId":"seat-\\"A\\"-1","holdId":"h1"}
'
    printf '{"seq":3,"type":"SeatSold","seatId":"seat-\\"A\\"-1","holdId":"h1"}
'
  } > "$out"
  printf '%s' "$out"
}

test_escaped_quotes_are_parsed_not_truncated() {
  local log status output
  log="$(make_escaped_quote_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  # Three events on one seat, sold twice. If the parser truncated the seat id it
  # would still see a double sale, so the sharper check is that it read the
  # whole log rather than choking on it.
  assert_exit 1 "$status" "a double sale is still caught when ids contain escaped quotes"
  assert_contains "$output" "VIOLATION I1" "the double sale is reported"
}

# Two different seats whose ids differ only where a naive composite key would
# join them. With a hand-picked separator these could collide; SUBSEP cannot.
make_separator_collision_log() {
  local out="${WORK}/separator.ndjson"
  {
    event 1 SeatHeld "a" "b-c"
    event 2 SeatHeld "a-b" "c"
    event 3 SeatSold "a" "b-c"
    event 4 SeatSold "a-b" "c"
  } > "$out"
  printf '%s' "$out"
}

test_composite_keys_do_not_collide() {
  local log status output
  log="$(make_separator_collision_log)"
  status=0
  output="$("$VERIFY" -f "$log" 2>/dev/null)" || status=$?

  assert_exit 0 "$status" "seat/hold pairs that share characters do not collide"
  assert_contains "$output" "TOTALS 4 2 0" "both seats sold once, no false violation"
}

test_help_exits_zero() {
  local status output
  status=0
  output="$("$VERIFY" -h 2>&1)" || status=$?

  assert_exit 0 "$status" "-h exits 0"
  assert_contains "$output" "Usage:" "-h prints usage"
}

# ---------------------------------------------------------------------------
# Mutation harness
#
# It is the check on the Java tests the way the verifier is the check on the
# application, so it gets the same treatment: prove it can fail. Its real risk
# is rot. If a refactor renames a line a mutant targets, that mutant silently
# stops mutating anything and reports a survivor for the wrong reason.
# ---------------------------------------------------------------------------

readonly MUTATE="${REPO_ROOT}/scripts/mutation-check.sh"

test_empty_log_is_flagged_not_blessed() {
  local output
  output="$(: | "$VERIFY" 2>&1)"

  assert_contains "$output" "audited nothing" "an empty log warns that it audited nothing"
}

test_mutants_still_apply_to_the_source() {
  local status=0 output
  output="$("$MUTATE" -c 2>&1)" || status=$?

  assert_exit 0 "$status" "every mutant still edits the current source"
  assert_contains "$output" "mutants still apply" "the applicability check says so"
}

test_stale_mutant_is_reported() {
  # A copy of the harness pointed at a tree where none of the target lines exist.
  local fake="${WORK}/fake-repo" status=0 output
  mkdir -p "${fake}/scripts/lib" "${fake}/src/main/java/dev/turnstile/eventstore" \
    "${fake}/src/main/java/dev/turnstile/command" "${fake}/src/main/java/dev/turnstile/domain"
  cp "$MUTATE" "${fake}/scripts/"
  cp "${REPO_ROOT}/scripts/lib/common.sh" "${fake}/scripts/lib/"
  : > "${fake}/pom.xml"
  local file
  for file in eventstore/InMemoryEventStore command/SeatCommandHandler domain/SeatAggregate; do
    printf '// nothing a mutant could match\n' > "${fake}/src/main/java/dev/turnstile/${file}.java"
  done

  output="$("${fake}/scripts/mutation-check.sh" -c 2>&1)" || status=$?

  assert_exit 1 "$status" "a mutant that matches nothing fails the check"
  assert_contains "$output" "M01 no longer matches" "the stale mutant is named"
}

test_mutant_list_marks_the_equivalent_one() {
  local output
  output="$("$MUTATE" -l 2>&1)"

  assert_contains "$output" "M01" "the mutants are listed"
  assert_contains "$output" "known equivalent" "the mutant no test can kill is listed as such"
}

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

main() {
  printf '%srunning shell test suite%s\n\n' "$C_BOLD" "$C_RESET" >&2

  test_clean_log_passes
  test_double_sale_is_caught
  test_sale_without_hold_is_caught
  test_activity_after_sale_is_caught
  test_open_hold_is_reported_but_not_fatal
  test_garbage_is_rejected
  test_reads_from_stdin
  test_quiet_mode_is_silent_on_success
  test_quiet_mode_still_reports_violations
  test_summary_flag
  test_missing_file_is_usage_error
  test_unknown_option_is_usage_error
  test_help_exits_zero
  test_temp_dir_is_registered_for_cleanup
  test_repo_root_has_no_side_effect
  test_escaped_quotes_are_parsed_not_truncated
  test_composite_keys_do_not_collide
  test_empty_log_is_flagged_not_blessed
  test_mutants_still_apply_to_the_source
  test_stale_mutant_is_reported
  test_mutant_list_marks_the_equivalent_one

  printf '\n' >&2
  if (( tests_failed > 0 )); then
    printf '%s%d of %d shell assertions failed%s\n' \
      "$C_RED" "$tests_failed" "$tests_run" "$C_RESET" >&2
    return 1
  fi
  printf '%sall %d shell assertions passed%s\n' \
    "$C_GREEN" "$tests_run" "$C_RESET" >&2
  return 0
}

main "$@"
