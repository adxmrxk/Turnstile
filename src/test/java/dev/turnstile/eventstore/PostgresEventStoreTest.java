package dev.turnstile.eventstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.testsupport.TestPostgres;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The shared store contract, run unchanged against a real PostgreSQL, plus what
 * only a database can be asked: does the unique index actually arbitrate a race,
 * and does the log really refuse to be edited.
 */
class PostgresEventStoreTest extends EventStoreContractTest {

  @Override
  protected EventStore newStore() {
    TestPostgres.reset();
    return new PostgresEventStore(TestPostgres.dataSource(), new EventCodec());
  }

  private static DomainEvent held(String seatId, String holdId) {
    return new DomainEvent.SeatHeld(
        seatId, holdId, "buyer", Instant.parse("2026-09-21T12:00:00Z"),
        Instant.parse("2026-09-21T12:05:00Z"));
  }

  @Test
  @DisplayName("of 16 writers racing to append version 1, exactly one wins")
  void only_one_writer_wins_a_race() throws Exception {
    EventStore store = newStore();
    int writers = 16;
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<Boolean>> results = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        String hold = "h" + i;
        results.add(
            pool.submit(
                () -> {
                  go.await();
                  try {
                    store.append("seat-1", 0, List.of(held("seat-1", hold)), null);
                    return true;
                  } catch (ConcurrencyConflictException lost) {
                    return false;
                  }
                }));
      }
      go.countDown();
      int winners = 0;
      for (Future<Boolean> f : results) {
        if (f.get(30, TimeUnit.SECONDS)) {
          winners++;
        }
      }
      assertThat(winners).as("writers that committed").isEqualTo(1);
      assertThat(store.load("seat-1")).hasSize(1);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * The store-level race above is honest but timing-dependent: on a fast local
   * database the writers' transactions barely overlap, and with the unique
   * constraint removed that test still passes. This one does not depend on
   * timing. Transaction A inserts version 1 and stays open; B tries the same
   * version and must be held until A decides. Then it checks both outcomes.
   */
  @Test
  @DisplayName("the unique constraint holds a second writer until the first decides")
  void the_constraint_arbitrates_two_open_transactions() throws Exception {
    newStore();
    String insert =
        "INSERT INTO events (stream_id, version, type, payload, occurred_at) "
            + "VALUES ('seat-x', 1, 'SeatHeld', '{}'::jsonb, now())";

    try (var a = TestPostgres.dataSource().getConnection();
        var b = TestPostgres.dataSource().getConnection()) {
      a.setAutoCommit(false);
      b.setAutoCommit(false);
      a.createStatement().execute(insert);

      ExecutorService pool = Executors.newSingleThreadExecutor();
      try {
        Future<Object> second =
            pool.submit(
                () -> {
                  b.createStatement().execute(insert);
                  return null;
                });

        assertThatThrownBy(() -> second.get(500, TimeUnit.MILLISECONDS))
            .as("B must be blocked while A holds version 1 open")
            .isInstanceOf(java.util.concurrent.TimeoutException.class);

        a.commit();

        assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS))
            .as("once A committed, B's insert of the same version must fail")
            .hasMessageContaining("duplicate key");
      } finally {
        pool.shutdownNow();
      }
    }
  }

  @Test
  @DisplayName("the same idempotency key used concurrently on different seats applies once")
  void idempotency_key_is_claimed_once_across_streams() throws Exception {
    EventStore store = newStore();
    int writers = 8;
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<AppendResult>> results = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        String seat = "seat-" + i;
        results.add(
            pool.submit(
                () -> {
                  go.await();
                  return store.append(seat, 0, List.of(held(seat, "h")), "one-key");
                }));
      }
      go.countDown();
      long applied = 0;
      for (Future<AppendResult> f : results) {
        if (!f.get(30, TimeUnit.SECONDS).deduplicated()) {
          applied++;
        }
      }
      assertThat(applied).as("appends that were not deduplicated").isEqualTo(1);
      assertThat(store.readAll()).as("events in the log").hasSize(1);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @DisplayName("a writer whose expected version is ahead of the stream is rejected")
  void expected_version_ahead_of_stream_is_rejected() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);

    // Behind is stopped by the unique index; ahead has no index to stop it, so the
    // store has to check, or it would write version 6 after version 1.
    assertThatThrownBy(() -> store.append("seat-1", 5, List.of(held("seat-1", "h2")), null))
        .isInstanceOf(ConcurrencyConflictException.class);
    assertThat(store.load("seat-1")).hasSize(1);
  }

  @Test
  @DisplayName("a rejected append leaves no idempotency key and no outbox row behind")
  void a_failed_append_is_all_or_nothing() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);

    assertThatThrownBy(
            () -> store.append("seat-1", 0, List.of(held("seat-1", "h2")), "doomed-key"))
        .isInstanceOf(ConcurrencyConflictException.class);

    assertThat(store.versionForIdempotencyKey("doomed-key"))
        .as("the key must not survive an append that did not happen")
        .isEmpty();
    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class))
        .as("only the first, committed event was announced")
        .isEqualTo(1L);
  }

  @Test
  @DisplayName("every committed event has exactly one outbox row")
  void each_event_is_announced_in_the_same_transaction() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1"), held("seat-1", "h2")), null);

    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    assertThat(jdbc.queryForList("SELECT event_key FROM outbox ORDER BY id", String.class))
        .containsExactly("seat-1:1", "seat-1:2");
  }

  @Test
  @DisplayName("the database itself refuses to update or delete a logged event")
  void the_log_cannot_be_edited() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);
    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());

    assertThatThrownBy(() -> jdbc.update("UPDATE events SET stream_id = 'seat-9'"))
        .hasMessageContaining("append-only");
    assertThatThrownBy(() -> jdbc.update("DELETE FROM events"))
        .hasMessageContaining("append-only");
    assertThat(store.load("seat-1")).hasSize(1);
  }
}
