package dev.turnstile.eventstore;

import dev.turnstile.domain.DomainEvent;
import java.util.List;
import java.util.OptionalLong;

/**
 * Append-only log with compare-and-append semantics.
 *
 * <p>The interface is deliberately tiny, because every implementation has to
 * make the same two guarantees atomically and there is nowhere to hide extra
 * behaviour:
 *
 * <ol>
 *   <li>append succeeds only if the stream is still at {@code expectedVersion}
 *   <li>an idempotency key is consumed at most once, ever
 * </ol>
 *
 * <p>In Postgres both fall out of one insert: a unique index on
 * {@code (stream_id, version)} and another on {@code idempotency_key}. The
 * in-memory implementation exists so the domain tests run in milliseconds with
 * no container; the Testcontainers suite runs the identical contract against
 * real Postgres.
 */
public interface EventStore {

  /** Full history of one stream, in version order. */
  List<StoredEvent> load(String streamId);

  /**
   * Appends atomically, or fails.
   *
   * @param expectedVersion version the caller's decision was based on; 0 for a
   *     stream that must not exist yet
   * @param idempotencyKey caller-supplied dedupe key, may be null
   * @throws ConcurrencyConflictException if the stream has moved on
   */
  AppendResult append(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey);

  /**
   * Stream version produced by a previously consumed idempotency key, if any.
   *
   * <p>This must be answerable <em>before</em> a command is decided, not only at
   * append time. A retry of a command that already succeeded has to return the
   * original outcome. If the retry is allowed to replay the aggregate first, the
   * aggregate correctly observes the seat is already sold and refuses, and the
   * caller is told their purchase failed when it actually went through.
   */
  OptionalLong versionForIdempotencyKey(String idempotencyKey);

  /**
   * Visits every event in global order without holding the log in memory. The
   * default falls back to {@link #readAll}; a durable store should stream. Export,
   * audit and read-model rebuild use this, so their memory does not grow with the
   * log.
   */
  default void forEachEvent(java.util.function.Consumer<StoredEvent> sink) {
    readAll().forEach(sink);
  }

  /** Every event across every stream in global order. Feeds projections and the verifier. */
  List<StoredEvent> readAll();

  /** Current version of a stream, 0 when it does not exist. */
  long currentVersion(String streamId);
}
