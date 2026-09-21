package dev.turnstile.sim;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainEvent;
import dev.turnstile.domain.DomainException;
import dev.turnstile.eventstore.AppendResult;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.InMemoryEventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Exhaustive checks of the read, decide, append protocol.
 *
 * <p>The stress test in {@code NoOversellConcurrencyTest} samples interleavings
 * and can only ever say "it did not happen this time". These enumerate every
 * interleaving of the store calls for a small number of buyers and assert the
 * invariants on all of them, so a pass means no ordering exists that breaks them
 * (for this scenario size), and a failure prints the exact schedule.
 */
class HandlerModelCheckTest {

  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-09-21T12:00:00Z"), ZoneOffset.UTC);
  private static final Duration TTL = Duration.ofMinutes(5);
  private static final String SEAT = "seat-1";

  // ---------------------------------------------------------------------------
  // Scenarios
  // ---------------------------------------------------------------------------

  /** Every buyer wants the same seat and runs hold then confirm. */
  private static Supplier<ScheduleExplorer.Scenario> contendedSeat(
      int buyers, Supplier<EventStore> storeFactory) {
    return () -> {
      EventStore store = storeFactory.get();
      return new ScheduleExplorer.Scenario() {
        @Override
        public EventStore store() {
          return store;
        }

        @Override
        public List<Function<EventStore, String>> actors() {
          List<Function<EventStore, String>> actors = new ArrayList<>();
          for (int i = 0; i < buyers; i++) {
            String buyer = "b" + i;
            actors.add(
                gated -> {
                  SeatCommandHandler handler = new SeatCommandHandler(gated, CLOCK);
                  try {
                    handler.hold(SEAT, "h-" + buyer, buyer, TTL, "hold-" + buyer);
                    handler.confirmSale(SEAT, "h-" + buyer, "o-" + buyer, "sale-" + buyer);
                    return "SOLD";
                  } catch (DomainException refused) {
                    return "REFUSED";
                  }
                });
          }
          return actors;
        }

        @Override
        public void verify(List<String> outcomes) {
          List<DomainEvent> log = store.readAll().stream().map(StoredEvent::event).toList();
          long sold = log.stream().filter(DomainEvent.SeatSold.class::isInstance).count();

          check(sold <= 1, "seat sold " + sold + " times");
          check(
              outcomes.stream().filter("SOLD"::equals).count() == sold,
              "buyers were told " + outcomes + " but the log holds " + sold + " sale(s)");
          check(
              outcomes.stream().noneMatch(o -> o.startsWith("CRASH")),
              "a buyer neither bought nor was refused: " + outcomes);
          // Nobody can lose a race for a seat nobody else touched, and a seat
          // that was contended must end up sold to someone.
          check(sold == 1, "a seat with " + outcomes.size() + " eager buyers ended unsold");
          checkNoDoubleHold(log);
        }
      };
    };
  }

  /**
   * The same command sent twice at once with the same idempotency key, which is
   * what a double-click or a retry racing its own original looks like.
   */
  private static Supplier<ScheduleExplorer.Scenario> duplicateHold() {
    return () -> {
      EventStore store = new InMemoryEventStore();
      return new ScheduleExplorer.Scenario() {
        @Override
        public EventStore store() {
          return store;
        }

        @Override
        public List<Function<EventStore, String>> actors() {
          Function<EventStore, String> sameRequest =
              gated -> {
                try {
                  AppendResult result =
                      new SeatCommandHandler(gated, CLOCK)
                          .hold(SEAT, "h-1", "buyer", TTL, "hold-1");
                  return result.deduplicated() ? "OK-DEDUP" : "OK";
                } catch (DomainException refused) {
                  return "REFUSED " + refused.getClass().getSimpleName();
                }
              };
          return List.of(sameRequest, sameRequest);
        }

        @Override
        public void verify(List<String> outcomes) {
          long held =
              store.readAll().stream()
                  .map(StoredEvent::event)
                  .filter(DomainEvent.SeatHeld.class::isInstance)
                  .count();
          check(held == 1, "the hold was applied " + held + " times");
          // The first request succeeded, so its duplicate must not be told it failed.
          check(
              outcomes.stream().allMatch(o -> o.startsWith("OK")),
              "a duplicate of a successful request was answered " + outcomes);
        }
      };
    };
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  /** Two holds in a row with nothing ending the first means two buyers were promised the seat. */
  private static void checkNoDoubleHold(List<DomainEvent> log) {
    boolean held = false;
    for (DomainEvent event : log) {
      if (event instanceof DomainEvent.SeatHeld) {
        check(!held, "seat was held again while a hold on it was still open");
        held = true;
      } else {
        held = false;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Real store: every interleaving must satisfy the invariants
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("two buyers, one seat: no interleaving oversells")
  void two_buyers_exhaustive() {
    ScheduleExplorer.Report report =
        new ScheduleExplorer(contendedSeat(2, InMemoryEventStore::new)).exhaustive(1_000_000);

    assertThat(report.firstViolation()).as("first violation").isNull();
    assertThat(report.exhausted()).as("explored every interleaving").isTrue();
    // Every schedule the search ran was a different one, and there were enough of
    // them that it cannot have quietly explored nothing.
    assertThat(report.distinct()).as("no schedule explored twice").isEqualTo(report.schedules());
    assertThat(report.schedules()).as("schedules explored").isGreaterThan(100);
    System.out.println("two buyers, one seat: " + report.schedules() + " interleavings, all safe");
  }

  @Test
  @DisplayName("the exhaustive search and random sampling agree on how many schedules exist")
  void exhaustive_search_is_complete() {
    Supplier<ScheduleExplorer.Scenario> scenario = contendedSeat(2, InMemoryEventStore::new);
    ScheduleExplorer.Report exhaustive = new ScheduleExplorer(scenario).exhaustive(1_000_000);
    ScheduleExplorer.Report sampled = new ScheduleExplorer(scenario).sampled(15_000, 7);

    // Sampling can only ever find schedules that exist. If it reaches one the
    // depth-first search skipped, or the search reports one that sampling cannot
    // reach, the enumeration is wrong and every "all interleavings" claim with it.
    assertThat(sampled.distinct())
        .as("distinct schedules reached by 15,000 random runs")
        .isEqualTo(exhaustive.distinct());
  }

  @Test
  @DisplayName("three buyers, one seat: 5,000 seeded interleavings are safe")
  void three_buyers_sampled() {
    ScheduleExplorer.Report report =
        new ScheduleExplorer(contendedSeat(3, InMemoryEventStore::new)).sampled(5_000, 42);

    assertThat(report.firstViolation()).as("first violation").isNull();
  }

  @Test
  @DisplayName("a duplicate of a request that succeeded is never reported as a failure")
  void concurrent_duplicate_is_not_reported_as_failure() {
    ScheduleExplorer.Report report =
        new ScheduleExplorer(duplicateHold()).exhaustive(1_000_000);

    assertThat(report.firstViolation()).as("first violation").isNull();
    assertThat(report.exhausted()).isTrue();
    assertThat(report.schedules()).isGreaterThan(10);
  }

  // ---------------------------------------------------------------------------
  // Negative control: the checker has to be able to fail
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("the explorer finds the oversell in a store with no version check")
  void explorer_catches_a_broken_store() {
    ScheduleExplorer.Report report =
        new ScheduleExplorer(contendedSeat(2, LastWriteWinsStore::new)).exhaustive(1_000_000);

    assertThat(report.violations())
        .as("a checker that cannot see this bug proves nothing about the real store")
        .isGreaterThan(0);
    assertThat(report.firstViolation()).contains("schedule:");
  }

  /** The naive store the README warns about: it accepts whatever it is handed. */
  private static final class LastWriteWinsStore implements EventStore {
    private final Map<String, List<StoredEvent>> streams = new HashMap<>();
    private final Map<String, Long> keys = new HashMap<>();
    private long sequence;

    @Override
    public synchronized List<StoredEvent> load(String streamId) {
      return List.copyOf(streams.getOrDefault(streamId, List.of()));
    }

    @Override
    public synchronized AppendResult append(
        String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {
      Long seen = idempotencyKey == null ? null : keys.get(idempotencyKey);
      if (seen != null) {
        return new AppendResult(seen, true);
      }
      List<StoredEvent> stream = streams.computeIfAbsent(streamId, k -> new ArrayList<>());
      for (DomainEvent event : events) {
        stream.add(new StoredEvent(streamId, stream.size() + 1L, ++sequence, event));
      }
      if (idempotencyKey != null) {
        keys.put(idempotencyKey, (long) stream.size());
      }
      return new AppendResult(stream.size(), false);
    }

    @Override
    public synchronized OptionalLong versionForIdempotencyKey(String idempotencyKey) {
      Long seen = keys.get(idempotencyKey);
      return seen == null ? OptionalLong.empty() : OptionalLong.of(seen);
    }

    @Override
    public synchronized List<StoredEvent> readAll() {
      List<StoredEvent> all = new ArrayList<>();
      streams.values().forEach(all::addAll);
      all.sort((a, b) -> Long.compare(a.globalSequence(), b.globalSequence()));
      return all;
    }

    @Override
    public synchronized long currentVersion(String streamId) {
      return streams.getOrDefault(streamId, List.of()).size();
    }
  }
}
