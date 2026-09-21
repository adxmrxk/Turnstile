package dev.turnstile.domain;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.eventstore.AppendResult;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.InMemoryEventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The thesis of the project, in miniature.
 *
 * <p>Every buyer starts at the same instant on a latch, so the window where two
 * threads have both read a seat as free and neither has written yet is as wide
 * as it can be made in a single JVM. A store that resolved contention by
 * last-write-wins would sell seats twice here, and the assertion below would
 * catch it.
 *
 * <p>Note what is being asserted. Not "no exception was thrown", which any
 * swallowed error would satisfy. The check is on the log itself: every seat has
 * at most one SeatSold event, and the number of successful confirms reported by
 * the handler equals the number of SeatSold events written. Those two numbers
 * agreeing is what rules out both a double sale and a phantom success.
 *
 * <p>The full-scale version of this lives in the Gatling suite at 200k virtual
 * buyers against real Postgres. This one runs in a second and gates every
 * commit, because a correctness property nobody checks on every push is a
 * property that quietly stops holding.
 */
class NoOversellConcurrencyTest {

  private static final int SEATS = 200;
  private static final int BUYERS = 2_000;
  private static final Duration HOLD_TTL = Duration.ofMinutes(5);

  @Test
  @DisplayName("2000 concurrent buyers against 200 seats sell each seat exactly once")
  void concurrent_buyers_never_oversell() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    // The handler talks to the store through a decorator that pauses after every
    // read. Without it the read-to-append window is a few microseconds and the
    // race almost never fires, so the test passes even against a store that has
    // no version check at all. Widening the window is what makes contention real.
    SeatCommandHandler handler =
        new SeatCommandHandler(new SlowReadStore(store), Clock.system(ZoneOffset.UTC));

    AtomicInteger confirmedSales = new AtomicInteger();
    AtomicInteger rejectedAsTaken = new AtomicInteger();

    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(BUYERS);

    // Not try-with-resources: ExecutorService only became AutoCloseable in
    // Java 19 and this module targets 17.
    ExecutorService pool = Executors.newFixedThreadPool(64);
    try {
      for (int buyer = 0; buyer < BUYERS; buyer++) {
        // Ten buyers per seat, in consecutive blocks. Tasks are handed to the pool
        // in order, so only neighbouring buyers ever run at the same time. The
        // earlier "buyer % SEATS" spread buyers 0..63 over 64 different seats and
        // put two buyers for the same seat 200 tasks apart, which meant they
        // never overlapped and the race this test exists for could not happen.
        String seatId = "seat-" + (buyer * SEATS / BUYERS);
        String buyerId = "buyer-" + buyer;

        pool.submit(
            () -> {
              try {
                startGun.await();

                String holdId = UUID.randomUUID().toString();
                handler.hold(seatId, holdId, buyerId, HOLD_TTL, "hold-" + buyerId);
                handler.confirmSale(seatId, holdId, "order-" + buyerId, "sale-" + buyerId);
                confirmedSales.incrementAndGet();

              } catch (DomainException.SeatAlreadyHeld | DomainException.SeatAlreadySold e) {
                // The correct outcome for everyone who lost the race.
                rejectedAsTaken.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                finished.countDown();
              }
            });
      }

      startGun.countDown();
      assertThat(finished.await(60, TimeUnit.SECONDS))
          .as("all buyers finished within the timeout")
          .isTrue();
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    List<StoredEvent> log = store.readAll();

    Map<String, Long> salesPerSeat =
        log.stream()
            .map(StoredEvent::event)
            .filter(DomainEvent.SeatSold.class::isInstance)
            .map(DomainEvent.SeatSold.class::cast)
            .collect(Collectors.groupingBy(DomainEvent.SeatSold::seatId, Collectors.counting()));

    assertThat(salesPerSeat.values())
        .as("no seat may appear in the log as sold more than once")
        .allMatch(count -> count == 1L);

    assertThat(salesPerSeat)
        .as("every seat should be sold, since buyers outnumber seats ten to one")
        .hasSize(SEATS);

    // Checked per seat, in stream order, without asking the aggregate: two holds
    // placed back to back with nothing releasing the first means two buyers were
    // both told the seat was theirs, even if neither of them went on to buy it.
    // Nothing in this test can expire (the TTL is five minutes), so the only legal
    // event after a hold is its end.
    Map<String, List<StoredEvent>> byStream =
        log.stream().collect(Collectors.groupingBy(StoredEvent::streamId));
    byStream.forEach(
        (seatId, events) -> {
          boolean held = false;
          for (StoredEvent stored : events) {
            DomainEvent event = stored.event();
            if (event instanceof DomainEvent.SeatHeld) {
              assertThat(held)
                  .as("seat %s was held again while a hold on it was still open", seatId)
                  .isFalse();
              held = true;
            } else {
              held = false;
            }
          }
        });

    assertThat(confirmedSales.get())
        .as("handler-reported successes must match the events actually written")
        .isEqualTo(salesPerSeat.size());

    assertThat(confirmedSales.get() + rejectedAsTaken.get())
        .as("every buyer either bought a seat or was told it was taken; none vanished")
        .isEqualTo(BUYERS);
  }

  /** Delegates to a real store but lingers after each load, holding the stale read open. */
  private record SlowReadStore(InMemoryEventStore delegate) implements EventStore {
    @Override
    public List<StoredEvent> load(String streamId) {
      List<StoredEvent> history = delegate.load(streamId);
      LockSupport.parkNanos(200_000);
      return history;
    }

    @Override
    public AppendResult append(
        String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {
      return delegate.append(streamId, expectedVersion, events, idempotencyKey);
    }

    @Override
    public OptionalLong versionForIdempotencyKey(String idempotencyKey) {
      return delegate.versionForIdempotencyKey(idempotencyKey);
    }

    @Override
    public List<StoredEvent> readAll() {
      return delegate.readAll();
    }

    @Override
    public long currentVersion(String streamId) {
      return delegate.currentVersion(streamId);
    }
  }

  @Test
  @DisplayName("a retried command with the same idempotency key never sells twice")
  void idempotent_retry_does_not_duplicate() {
    InMemoryEventStore store = new InMemoryEventStore();
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));

    handler.hold("seat-1", "hold-1", "buyer-1", HOLD_TTL, "idem-hold-1");

    // Simulates the client timing out and retrying a request that in fact
    // succeeded, which is the ordinary case on a flaky mobile connection.
    var first = handler.confirmSale("seat-1", "hold-1", "order-1", "idem-sale-1");
    var replay = handler.confirmSale("seat-1", "hold-1", "order-1", "idem-sale-1");

    assertThat(first.deduplicated()).isFalse();
    assertThat(replay.deduplicated()).as("the retry must be recognised, not re-executed").isTrue();
    assertThat(replay.newVersion()).isEqualTo(first.newVersion());

    long soldEvents =
        store.readAll().stream().map(StoredEvent::event).filter(DomainEvent.SeatSold.class::isInstance).count();
    assertThat(soldEvents).as("exactly one sale in the log").isEqualTo(1);
  }
}
