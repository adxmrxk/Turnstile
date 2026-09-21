#!/usr/bin/env bash
#
# mutation-check.sh -- prove the test suite would notice if the safety logic broke.
#
# A green suite says the code passes the tests. It says nothing about whether the
# tests could ever fail. This deliberately breaks the code, one targeted change at
# a time, in a throwaway copy, and runs the suite against each. A mutant the suite
# fails on is KILLED, which is what we want. A mutant that leaves the suite green
# SURVIVED, which means the suite cannot see that bug and is not evidence for it.
#
# Each mutant is a single, plausible mistake in a line that carries the
# no-oversell guarantee: a dropped version check, a skipped idempotency lookup,
# an expiry that never expires. The project tree is never modified.
#
# EXIT CODES
#   0  every mutant was killed
#   1  at least one mutant survived, or produced no usable result
#   2  usage error or the unmutated code does not pass

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
enable_error_trace
enable_cleanup

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Hard wall-clock limit for one run of the suite. The unmutated suite takes about a
# minute; a mutant that runs past this is a hang or a crawl, and counts as caught.
readonly SUITE_LIMIT_SECONDS="${MUTATION_SUITE_LIMIT:-240}"

STORE='src/main/java/dev/turnstile/eventstore/InMemoryEventStore.java'
HANDLER='src/main/java/dev/turnstile/command/SeatCommandHandler.java'
AGGREGATE='src/main/java/dev/turnstile/domain/SeatAggregate.java'
SAGA='src/main/java/dev/turnstile/saga/PurchaseSaga.java'
PGSTORE='src/main/java/dev/turnstile/eventstore/PostgresEventStore.java'
AUDITOR='src/main/java/dev/turnstile/audit/LogAuditor.java'
SECURITY='src/main/java/dev/turnstile/security/SecurityConfig.java'
ACCESS='src/main/java/dev/turnstile/security/Access.java'
PROJECTION='src/main/java/dev/turnstile/readmodel/SeatMapProjection.java'
CONSUMER='src/main/java/dev/turnstile/messaging/KafkaProjectionConsumer.java'
RELAY='src/main/java/dev/turnstile/messaging/OutboxRelay.java'
QUERIES='src/main/java/dev/turnstile/query/SeatQueries.java'
NOTIFY='src/main/java/dev/turnstile/eventstore/NotifyingEventStore.java'

# id | file | pattern (first match only) | replacement | what the mistake is
MUTANTS=(
  "M01|$STORE|if (actualVersion != expectedVersion) {|if (false) {|append never rejects a stale version"
  "M03|$HANDLER|if (applied.isPresent()) {|if (false) {|a refusal is never checked against the idempotency record"
  "M04|$STORE|return OptionalLong.of(prior); // already consumed|return OptionalLong.empty(); // already consumed|a consumed idempotency key is claimed a second time"
  "M05|$STORE|idempotencyKeys.remove(idempotencyKey);|;|a failed append leaves its idempotency key claimed"
  "M06|$AGGREGATE|if (isExpired(now)) {|if (false) {|a sale is confirmed on an expired hold"
  "M07|$AGGREGATE|case HELD -> throw new DomainException.SeatAlreadyHeld(seatId);|case HELD -> { }|a seat that is held can be held again"
  "M08|$AGGREGATE|!holdId.equals(activeHoldId)|false|any hold id can confirm the seat"
  "M09|$AGGREGATE|!now.isBefore(holdExpiresAt)|false|holds never expire"
  "M10|$HANDLER|new AppendResult(alreadyApplied.getAsLong(), true)|new AppendResult(alreadyApplied.getAsLong(), false)|a replayed command is reported as a fresh one"
  "M11|$HANDLER|attempt <= MAX_ATTEMPTS|attempt <= 1|a conflicted command gives up instead of retrying"
  "M12|$SAGA|gateway.refund(s.orderId());|;|payment is never reversed or cancelled when it cannot be confirmed"
  "M13|$SAGA|return s.with(State.REFUNDED, cannotSell.getMessage());|return s.with(State.CONFIRMED, cannotSell.getMessage());|a refused sale is reported as confirmed"
  "M14|$SAGA|key(s, \"confirm\")|null|the sale step is not idempotent, so a replay after a crash sells twice"
  "M15|$SAGA|static final int CHARGE_ATTEMPTS = 3;|static final int CHARGE_ATTEMPTS = 1;|an unanswered charge is never retried"
  "M16|$PGSTORE|if (actual != expectedVersion) {|if (false) {|Postgres append does not check the version it was given"
  "M17|$PGSTORE|if (claimed == 0) {|if (false) {|Postgres never recognises an idempotency key it has already consumed"
  "M18|$AUDITOR|\"I3 line |\"I9 line |the Java auditor reports post-sale activity under the wrong code"
  "M19|$SECURITY|.hasRole(\"staff\")|.permitAll()|the raw log and audit are open to any caller"
  "M20|$ACCESS|if (!enforced) {|if (true) {|GraphQL field-level staff checks are skipped"
  "M21|$PROJECTION|if (version <= applied) {|if (false) {|the read model applies a redelivered event a second time"
  "M22|$CONSUMER|projection.apply(stored.streamId(), stored.version(), stored.event());|;|the Kafka consumer discards every event it reads"
  "M23|$RELAY|break; // stop here|continue; // stop here|a failed publish is skipped, so a seat's events can go out of order"
  "M24|$QUERIES|.filter(e -> !e.event().occurredAt().isAfter(instant))|.filter(e -> true)|time travel replays events from the future"
  "M25|$NOTIFY|!result.deduplicated()|false|nothing is ever announced, so the seat map and live feed go stale"
)

# Mutants no test can kill because they change no observable behaviour. Listed
# so the omission is visible and argued rather than quietly dropped. The harness
# cannot prove equivalence; this is a claim to be checked by reading the code.
KNOWN_EQUIVALENT=(
  "M02|SeatCommandHandler skipping its idempotency pre-check: a replay is then answered by the post-refusal check or by the store at append time, with the same result, so the pre-check only saves a read"
)

usage() {
  cat <<'USAGE'
Usage: mutation-check.sh [-l] [-c] [-o ID] [-h]

Breaks the code one way at a time, in a throwaway copy, and reports whether the
Java test suite notices.

Options:
  -l      list the mutants and exit
  -c      only check every mutant still applies to the source (fast, no Maven)
  -o ID   run only this mutant (e.g. M03)
  -h      show this help

Exit codes:
  0  every mutant killed
  1  a mutant survived or gave no usable result
  2  usage error, or the unmutated code does not pass
USAGE
}

# Copies just what a build needs. Never target/, so nothing stale can pass a mutant.
fresh_copy() {
  local dest="$1"
  rm -rf "$dest"
  mkdir -p "$dest"
  cp "${REPO_ROOT}/pom.xml" "$dest/"
  cp -r "${REPO_ROOT}/src" "$dest/"
  # The auditor-agreement test runs the real verifier script, so the copy needs it.
  cp -r "${REPO_ROOT}/scripts" "$dest/"
}

# Runs the suite in DIR, logging to LOG. Returns the maven exit status.
run_suite() {
  local dir="$1" log="$2"
  # A mutant can make the code hang or crawl rather than fail (a retry stuck on a
  # claim nobody will release, say), and that would stall this whole run. So the
  # suite gets a hard wall-clock limit and, past it, the whole process tree is
  # killed and the run counts as a failure, which is the right verdict: the suite
  # noticed. Surefire's own forkedProcessTimeout was tried first and observed not
  # to fire on Windows, leaving the test JVM spinning. The unmutated suite takes
  # about a minute; the baseline check fails loudly if a machine is too slow.
  (cd "$dir" && mvn -B test) >"$log" 2>&1 &
  local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if (( waited >= SUITE_LIMIT_SECONDS )); then
      kill_tree_under "$dir"
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      printf '\n[mutation-check] suite exceeded %ss and was killed: counted as a failure (hang)\n' \
        "$SUITE_LIMIT_SECONDS" >>"$log"
      return 124
    fi
    sleep 2
    waited=$((waited + 2))
  done
  wait "$pid"
}

# Kills the Maven run under DIR and everything it started.
kill_tree_under() {
  local dir="$1"
  if command -v powershell.exe >/dev/null 2>&1 && command -v cygpath >/dev/null 2>&1; then
    powershell.exe -NoProfile -ExecutionPolicy Bypass \
      -File "$(cygpath -w "${SCRIPT_DIR}/lib/kill-tree.ps1")" -Match "$(cygpath -m "$dir")" >/dev/null 2>&1 || true
  else
    pkill -f -- "$dir" 2>/dev/null || true
  fi
}

failing_tests() {
  grep -E '^\[ERROR\]   [A-Za-z]' "$1" | sed -E 's/^\[ERROR\]   //; s/:[0-9]+.*//; s/ .*//' | sort -u | head -3 | paste -sd, - || true
}

main() {
  local only='' list=0 check_only=0
  while getopts ':lco:h' opt; do
    case "$opt" in
      l) list=1 ;;
      c) check_only=1 ;;
      o) only="$OPTARG" ;;
      h) usage; exit 0 ;;
      :) usage >&2; die "option -${OPTARG} requires an argument" ;;
      \?) usage >&2; die "unknown option: -${OPTARG}" ;;
      *) usage >&2; exit 2 ;;
    esac
  done

  local entry id file pattern replacement description
  if (( list )); then
    for entry in "${MUTANTS[@]}"; do
      IFS='|' read -r id file pattern replacement description <<<"$entry"
      printf '%s  %-58s %s\n' "$id" "$description" "${file##*/}"
    done
    for entry in "${KNOWN_EQUIVALENT[@]}"; do
      printf '%s  (known equivalent, not run) %s\n' "${entry%%|*}" "${entry#*|}"
    done
    exit 0
  fi

  require_command sed cmp

  local work log_dir
  make_temp_dir work

  # No Maven: only confirms every mutant still edits its file. This is what stops
  # the table rotting, because a refactor that renames a line would otherwise turn
  # a mutant into a silent no-op that "survives" for the wrong reason.
  if (( check_only )); then
    local stale=0
    for entry in "${MUTANTS[@]}"; do
      IFS='|' read -r id file pattern replacement description <<<"$entry"
      fresh_copy "${work}/${id}"
      cp "${work}/${id}/${file}" "${work}/${id}.before"
      sed -i "0,\\#${pattern}#s##${replacement}#" "${work}/${id}/${file}"
      if cmp -s "${work}/${id}.before" "${work}/${id}/${file}"; then
        log_fail "${id} no longer matches: ${pattern}"
        stale=$((stale + 1))
      fi
    done
    if (( stale > 0 )); then
      return 1
    fi
    log_ok "all ${#MUTANTS[@]} mutants still apply"
    return 0
  fi

  require_command mvn
  log_dir="${work}/logs"
  mkdir -p "$log_dir"

  log_info "checking the unmutated code passes first"
  fresh_copy "${work}/baseline"
  if ! run_suite "${work}/baseline" "${log_dir}/baseline.log"; then
    tail -30 "${log_dir}/baseline.log" >&2
    log_error "the unmutated suite fails, so a mutant failing would prove nothing"
    exit 2
  fi
  log_ok "baseline is green"

  local killed=0 survived=0 broken=0 total=0
  local -a survivors=()
  printf '\n'

  for entry in "${MUTANTS[@]}"; do
    IFS='|' read -r id file pattern replacement description <<<"$entry"
    [[ -z "$only" || "$only" == "$id" ]] || continue
    total=$((total + 1))

    local dir="${work}/${id}" log="${log_dir}/${id}.log"
    fresh_copy "$dir"

    # sed only edits the first match, and the file must actually change: a
    # pattern that silently stops matching would report every mutant "killed".
    local before="${dir}/before.tmp"
    cp "${dir}/${file}" "$before"
    sed -i "0,\\#${pattern}#s##${replacement}#" "${dir}/${file}"
    if cmp -s "$before" "${dir}/${file}"; then
      printf '  %sBROKEN%s   %s  pattern no longer matches: %s\n' "$C_YELLOW" "$C_RESET" "$id" "$pattern"
      broken=$((broken + 1))
      continue
    fi
    rm -f "$before"

    if run_suite "$dir" "$log"; then
      printf '  %sSURVIVED%s %s  %s\n' "$C_RED" "$C_RESET" "$id" "$description"
      survived=$((survived + 1))
      survivors+=("$id")
    elif grep -q 'COMPILATION ERROR' "$log"; then
      printf '  %sBROKEN%s   %s  mutant did not compile, so it says nothing: %s\n' "$C_YELLOW" "$C_RESET" "$id" "$description"
      broken=$((broken + 1))
    else
      printf '  %skilled%s   %s  %s\n' "$C_GREEN" "$C_RESET" "$id" "$description"
      if grep -q 'was killed: counted as a failure' "$log"; then
        printf '             by the suite hanging or crawling past %ss (killed)\n' "$SUITE_LIMIT_SECONDS"
      else
        printf '             by %s\n' "$(failing_tests "$log")"
      fi
      killed=$((killed + 1))
    fi
  done

  printf '\n'
  log_info "${killed} killed, ${survived} survived, ${broken} unusable, of ${total} (${#KNOWN_EQUIVALENT[@]} known-equivalent not run; see -l)"
  if (( survived > 0 )); then
    log_fail "the suite cannot detect: ${survivors[*]}"
    return 1
  fi
  if (( broken > 0 )); then
    log_fail "some mutants gave no result; fix the mutant table before trusting this"
    return 1
  fi
  log_ok "every mutant was caught"
}

main "$@"
