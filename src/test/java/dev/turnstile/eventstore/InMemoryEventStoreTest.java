package dev.turnstile.eventstore;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.domain.DomainEvent;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Runs the shared contract against the in-memory store, plus the raw
 * compare-and-append race that the contract cannot express single-threaded.
 */
class InMemoryEventStoreTest extends EventStoreContractTest {

  @Override
  protected EventStore newStore() {
    return new InMemoryEventStore();
  }

  @Test
  @DisplayName("exactly one of N racing appends at the same version wins")
  void only_one_writer_wins_a_race() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    int writers = 64;

    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(writers);
    AtomicInteger succeeded = new AtomicInteger();
    AtomicInteger conflicted = new AtomicInteger();

    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      for (int i = 0; i < writers; i++) {
        String holdId = "hold-" + i;
        pool.submit(
            () -> {
              try {
                startGun.await();
                // Every writer claims the stream is empty. Only one can be right.
                store.append(
                    "seat-1",
                    0,
                    List.of(
                        new DomainEvent.SeatHeld(
                            "seat-1",
                            holdId,
                            "buyer",
                            Instant.parse("2026-09-04T12:00:00Z"),
                            Instant.parse("2026-09-04T12:02:00Z"))),
                    null);
                succeeded.incrementAndGet();
              } catch (ConcurrencyConflictException e) {
                conflicted.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }

      startGun.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(succeeded.get()).as("exactly one writer commits").isEqualTo(1);
    assertThat(conflicted.get()).isEqualTo(writers - 1);
    assertThat(store.load("seat-1")).as("one event in the log").hasSize(1);
  }

  @Test
  @DisplayName("one idempotency key is consumed once even across different streams")
  void idempotency_key_is_global_not_per_stream() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    int writers = 32;

    // Per-stream compute() serialises writers to the same stream but gives no
    // protection at all across streams, so a check-then-act on the key let two
    // commands carrying one key append to two different seats. The claim is now
    // taken before any stream is touched, which is what a unique index on the
    // key does in Postgres.
    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(writers);
    AtomicInteger applied = new AtomicInteger();
    AtomicInteger deduplicated = new AtomicInteger();

    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      for (int i = 0; i < writers; i++) {
        String streamId = "seat-" + i; // every writer targets a DIFFERENT stream
        pool.submit(
            () -> {
              try {
                startGun.await();
                AppendResult result =
                    store.append(
                        streamId,
                        0,
                        List.of(
                            new DomainEvent.SeatHeld(
                                streamId,
                                "hold",
                                "buyer",
                                Instant.parse("2026-09-04T12:00:00Z"),
                                Instant.parse("2026-09-04T12:02:00Z"))),
                        "shared-key");
                if (result.deduplicated()) {
                  deduplicated.incrementAndGet();
                } else {
                  applied.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      startGun.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(applied.get()).as("the key is consumed exactly once").isEqualTo(1);
    assertThat(deduplicated.get()).isEqualTo(writers - 1);
    assertThat(store.readAll()).as("only one event was ever written").hasSize(1);
  }

  @Test
  @DisplayName("readAll is safe while writers are appending")
  void read_all_is_safe_under_concurrent_writes() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();

    // Streams used to hold mutable ArrayLists that were handed straight to
    // readers, so a reader iterating one while a writer appended could throw
    // ConcurrentModificationException or observe a torn event. Lists are now
    // replaced wholesale and every published list is immutable.
    AtomicInteger readerFailures = new AtomicInteger();
    AtomicBoolean writing = new AtomicBoolean(true);

    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      Future<?> reader =
          pool.submit(
              () -> {
                while (writing.get()) {
                  try {
                    for (StoredEvent event : store.readAll()) {
                      // Touch the contents so a torn read would surface.
                      if (event.event().seatId() == null) {
                        readerFailures.incrementAndGet();
                      }
                    }
                    store.load("seat-5").size();
                  } catch (RuntimeException e) {
                    readerFailures.incrementAndGet();
                  }
                }
              });

      for (int i = 0; i < 400; i++) {
        String streamId = "seat-" + (i % 20);
        long version = store.currentVersion(streamId);
        try {
          store.append(
              streamId,
              version,
              List.of(
                  new DomainEvent.SeatHeld(
                      streamId,
                      "hold-" + i,
                      "buyer",
                      Instant.parse("2026-09-04T12:00:00Z"),
                      Instant.parse("2026-09-04T12:02:00Z"))),
              null);
        } catch (ConcurrencyConflictException ignored) {
          // Irrelevant here; the reader is what is under test.
        }
      }

      writing.set(false);
      reader.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(readerFailures.get()).as("no reader ever saw a torn or concurrent view").isZero();
  }

  @Test
  @DisplayName("a claim stranded by a dead writer cannot hang readers of that key")
  void a_stuck_claim_does_not_hang_readers() throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();

    // Plant the state a writer leaves behind if it dies after claiming a key and
    // before resolving it: the pending marker, never replaced or removed.
    var keys = InMemoryEventStore.class.getDeclaredField("idempotencyKeys");
    keys.setAccessible(true);
    var pending = InMemoryEventStore.class.getDeclaredField("PENDING");
    pending.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, Long> map = (java.util.Map<String, Long>) keys.get(store);
    map.put("stranded", pending.getLong(null));

    // Before the wait was bounded this spun forever, hanging the whole suite.
    var answer =
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
            java.time.Duration.ofSeconds(15), () -> store.versionForIdempotencyKey("stranded"));

    assertThat(answer).as("a stranded claim reads as unconsumed").isEqualTo(java.util.OptionalLong.empty());
  }
}
