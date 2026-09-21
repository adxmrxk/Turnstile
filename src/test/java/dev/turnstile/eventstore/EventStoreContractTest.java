package dev.turnstile.eventstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.turnstile.domain.DomainEvent;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The contract every EventStore implementation must satisfy.
 *
 * <p>Written against the interface rather than the in-memory class on purpose.
 * When the Postgres implementation lands it extends this class, supplies a
 * Testcontainers-backed store, and inherits every assertion unchanged. If the
 * unique index on {@code (stream_id, version)} is ever dropped or the
 * idempotency constraint is weakened, these tests fail against real Postgres
 * exactly as they would here.
 */
abstract class EventStoreContractTest {

  protected abstract EventStore newStore();

  private static DomainEvent held(String seatId, String holdId) {
    return new DomainEvent.SeatHeld(
        seatId, holdId, "buyer-1", Instant.parse("2026-09-04T12:00:00Z"),
        Instant.parse("2026-09-04T12:02:00Z"));
  }

  @Test
  @DisplayName("a new stream starts at version 0 and reads back empty")
  void empty_stream() {
    EventStore store = newStore();

    assertThat(store.currentVersion("seat-1")).isZero();
    assertThat(store.load("seat-1")).isEmpty();
  }

  @Test
  void appending_to_a_new_stream_requires_expected_version_zero() {
    EventStore store = newStore();

    AppendResult result = store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);

    assertThat(result.newVersion()).isEqualTo(1);
    assertThat(result.deduplicated()).isFalse();
    assertThat(store.load("seat-1")).hasSize(1);
  }

  @Test
  @DisplayName("a stale expected version is rejected rather than merged")
  void stale_append_is_rejected() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);

    // Simulates a second writer that read the stream when it was still empty.
    assertThatThrownBy(() -> store.append("seat-1", 0, List.of(held("seat-1", "h2")), null))
        .isInstanceOf(ConcurrencyConflictException.class)
        .satisfies(
            e -> {
              ConcurrencyConflictException conflict = (ConcurrencyConflictException) e;
              assertThat(conflict.expectedVersion()).isZero();
              assertThat(conflict.actualVersion()).isEqualTo(1);
            });

    assertThat(store.load("seat-1")).as("the rejected append wrote nothing").hasSize(1);
  }

  @Test
  void versions_are_contiguous_and_per_stream() {
    EventStore store = newStore();

    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);
    store.append("seat-2", 0, List.of(held("seat-2", "h2")), null);
    store.append("seat-1", 1, List.of(held("seat-1", "h3")), null);

    assertThat(store.load("seat-1")).extracting(StoredEvent::version).containsExactly(1L, 2L);
    assertThat(store.load("seat-2")).extracting(StoredEvent::version).containsExactly(1L);
  }

  @Test
  @DisplayName("a batch is appended atomically, not event by event")
  void multi_event_append_is_one_unit() {
    EventStore store = newStore();

    AppendResult result =
        store.append(
            "seat-1", 0, List.of(held("seat-1", "h1"), held("seat-1", "h2")), null);

    assertThat(result.newVersion()).isEqualTo(2);
    assertThat(store.load("seat-1")).hasSize(2);
  }

  @Test
  void global_sequence_orders_events_across_streams() {
    EventStore store = newStore();

    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);
    store.append("seat-2", 0, List.of(held("seat-2", "h2")), null);
    store.append("seat-1", 1, List.of(held("seat-1", "h3")), null);

    assertThat(store.readAll())
        .extracting(StoredEvent::streamId)
        .as("readAll follows write order, not stream grouping")
        .containsExactly("seat-1", "seat-2", "seat-1");
  }

  @Test
  @DisplayName("an idempotency key is honoured once and only once")
  void idempotency_key_deduplicates() {
    EventStore store = newStore();

    AppendResult first = store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");
    AppendResult retry = store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");

    assertThat(first.deduplicated()).isFalse();
    assertThat(retry.deduplicated()).isTrue();
    assertThat(retry.newVersion()).isEqualTo(first.newVersion());
    assertThat(store.load("seat-1")).as("the replay wrote nothing").hasSize(1);
  }

  @Test
  @DisplayName("dedupe wins over a version conflict, so a retry is never punished")
  void idempotent_retry_beats_stale_version() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");

    // A client retrying after a timeout still carries the version it read
    // originally. It must get its original answer, not a conflict.
    AppendResult retry = store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");

    assertThat(retry.deduplicated()).isTrue();
  }

  @Test
  void different_keys_are_independent() {
    EventStore store = newStore();

    store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");
    AppendResult second = store.append("seat-1", 1, List.of(held("seat-1", "h2")), "key-2");

    assertThat(second.deduplicated()).isFalse();
    assertThat(store.load("seat-1")).hasSize(2);
  }

  @Test
  @DisplayName("a consumed key is visible before any command is decided")
  void consumed_key_is_queryable_up_front() {
    EventStore store = newStore();

    assertThat(store.versionForIdempotencyKey("key-1")).isEqualTo(OptionalLong.empty());

    store.append("seat-1", 0, List.of(held("seat-1", "h1")), "key-1");

    // The command handler relies on this being answerable without replaying the
    // aggregate, so a retry can be served from the record instead of being
    // re-judged against a state it already changed.
    assertThat(store.versionForIdempotencyKey("key-1")).isEqualTo(OptionalLong.of(1L));
    assertThat(store.versionForIdempotencyKey("never-used")).isEqualTo(OptionalLong.empty());
  }

  @Test
  void a_null_idempotency_key_is_never_treated_as_seen() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);

    assertThat(store.versionForIdempotencyKey(null)).isEqualTo(OptionalLong.empty());
  }

  @Test
  void an_empty_append_is_a_no_op() {
    EventStore store = newStore();

    AppendResult result = store.append("seat-1", 0, List.of(), null);

    assertThat(result.newVersion()).isZero();
    assertThat(store.load("seat-1")).isEmpty();
  }

  @Test
  @DisplayName("streaming the log visits exactly what readAll returns, in the same order")
  void for_each_event_matches_read_all() {
    EventStore store = newStore();
    store.append("seat-1", 0, List.of(held("seat-1", "h1")), null);
    store.append("seat-2", 0, List.of(held("seat-2", "h2")), null);
    store.append("seat-1", 1, List.of(held("seat-1", "h3")), null);

    List<StoredEvent> streamed = new java.util.ArrayList<>();
    store.forEachEvent(streamed::add);

    assertThat(streamed).isNotEmpty().isEqualTo(store.readAll());
  }
}
