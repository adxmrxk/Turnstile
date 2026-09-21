package dev.turnstile.eventstore;

import dev.turnstile.domain.DomainEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reference implementation of the {@link EventStore} contract.
 *
 * <p>Exists so the domain suite, including the 2000-buyer contention test, runs
 * in milliseconds without a container. The Postgres implementation must pass the
 * identical contract test, so this doubles as the executable specification of
 * what those unique indexes are required to do.
 *
 * <p>Two things are load bearing and both were originally wrong.
 *
 * <p><b>Streams hold immutable lists.</b> An earlier version stored a mutable
 * {@code ArrayList} per stream and handed it to readers. A reader iterating that
 * list while a writer appended inside {@code compute} is a data race, and
 * {@code readAll} could throw {@code ConcurrentModificationException} or observe
 * a half-written event. Replacing the list wholesale on every append makes every
 * published list immutable and every read safe with no lock at all. Appending is
 * O(n) in stream length as a result, which is the right trade here because seat
 * streams are a handful of events; a real store appends to a table instead.
 *
 * <p><b>Idempotency keys are claimed atomically and globally.</b> Per-stream
 * {@code compute} serialises writers to the same stream but gives no protection
 * across streams, so two commands carrying the same key against different seats
 * could both pass a check-then-act and both append. The key is now claimed with
 * {@code putIfAbsent} before any stream is touched, which is the same
 * all-or-nothing claim a unique index on the key gives you in Postgres.
 */
public final class InMemoryEventStore implements EventStore {

  /**
   * Marks a key claimed by a writer that has not yet finished. A reader that
   * sees it waits for the real version rather than reporting a nonsense one.
   */
  private static final long PENDING = Long.MIN_VALUE;

  /** How long a reader waits on a pending claim before deciding its writer is gone. */
  private static final long STUCK_CLAIM_NANOS = 2_000_000_000L;

  private final Map<String, List<StoredEvent>> streams = new ConcurrentHashMap<>();
  private final Map<String, Long> idempotencyKeys = new ConcurrentHashMap<>();
  private final AtomicLong globalSequence = new AtomicLong(0);

  @Override
  public List<StoredEvent> load(String streamId) {
    // Already immutable, so it can be published directly. No defensive copy.
    return streams.getOrDefault(streamId, List.of());
  }

  @Override
  public long currentVersion(String streamId) {
    return load(streamId).size();
  }

  @Override
  public OptionalLong versionForIdempotencyKey(String idempotencyKey) {
    if (idempotencyKey == null) {
      return OptionalLong.empty();
    }
    Long seen = idempotencyKeys.get(idempotencyKey);
    // A key mid-flight is waited on, not reported as absent. Its writer has
    // already appended, so the events are visible while the key still reads as
    // unconsumed; answering "absent" here lets a duplicate of a request that is
    // succeeding right now conclude it failed. The wait ends when the writer
    // publishes a version, or removes the claim after a failed append.
    //
    // The wait is bounded. An append does no I/O, so a healthy writer resolves its
    // claim in microseconds; a claim still pending after this long belongs to a
    // writer that died holding it, and waiting on it forever would hang every
    // reader of the key. Past the bound the key is reported as absent, which is
    // what readers did before they waited at all.
    long giveUpAt = System.nanoTime() + STUCK_CLAIM_NANOS;
    while (seen != null && seen == PENDING && System.nanoTime() < giveUpAt) {
      Thread.onSpinWait();
      seen = idempotencyKeys.get(idempotencyKey);
    }
    if (seen == null || seen == PENDING) {
      return OptionalLong.empty();
    }
    return OptionalLong.of(seen);
  }

  @Override
  public AppendResult append(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {

    if (events.isEmpty()) {
      return new AppendResult(currentVersion(streamId), false);
    }

    boolean claimed = false;
    if (idempotencyKey != null) {
      OptionalLong alreadyConsumed = claimOrResolve(idempotencyKey);
      if (alreadyConsumed.isPresent()) {
        return new AppendResult(alreadyConsumed.getAsLong(), true);
      }
      claimed = true;
    }

    try {
      long newVersion = appendToStream(streamId, expectedVersion, events);
      if (claimed) {
        idempotencyKeys.put(idempotencyKey, newVersion);
      }
      return new AppendResult(newVersion, false);

    } catch (RuntimeException | Error failure) {
      // Errors too: an OutOfMemoryError mid-append must not strand the claim.
      // The claim must not outlive a failed append, or a legitimate retry of a
      // command that never landed would be told it had already been applied.
      if (claimed) {
        idempotencyKeys.remove(idempotencyKey);
      }
      throw failure;
    }
  }

  /**
   * Claims the key for this writer, or resolves the version a previous writer
   * produced.
   *
   * @return empty when the caller now owns the key and should proceed, otherwise
   *     the version the earlier append produced
   */
  private OptionalLong claimOrResolve(String idempotencyKey) {
    long giveUpAt = System.nanoTime() + STUCK_CLAIM_NANOS;
    while (true) {
      Long prior = idempotencyKeys.putIfAbsent(idempotencyKey, PENDING);

      if (prior == null) {
        return OptionalLong.empty(); // claimed
      }
      if (prior != PENDING) {
        return OptionalLong.of(prior); // already consumed
      }

      // Another writer holds the claim and has not resolved it. Wait for it to
      // either publish a version or release the claim after a failure, then
      // re-evaluate. A healthy writer does no I/O and resolves in microseconds.
      if (System.nanoTime() >= giveUpAt) {
        // Unlike a read, an append cannot guess: proceeding might apply a command
        // that already took effect, and giving up silently might drop one that
        // did not. Failing loudly is the only safe answer for a dead writer.
        throw new IllegalStateException(
            "idempotency key " + idempotencyKey + " is held by a writer that never finished");
      }
      Thread.onSpinWait();
    }
  }

  private long appendToStream(String streamId, long expectedVersion, List<DomainEvent> events) {
    // Boxed so the lambda can report back out. compute() runs single-threaded
    // per key, so no synchronisation is needed on it.
    long[] newVersion = {0L};

    streams.compute(
        streamId,
        (key, existing) -> {
          List<StoredEvent> current = existing == null ? List.of() : existing;

          long actualVersion = current.size();
          if (actualVersion != expectedVersion) {
            throw new ConcurrencyConflictException(streamId, expectedVersion, actualVersion);
          }

          List<StoredEvent> updated = new ArrayList<>(current.size() + events.size());
          updated.addAll(current);
          for (DomainEvent event : events) {
            updated.add(
                new StoredEvent(
                    streamId, updated.size() + 1L, globalSequence.incrementAndGet(), event));
          }

          newVersion[0] = updated.size();
          return List.copyOf(updated);
        });

    return newVersion[0];
  }

  @Override
  public List<StoredEvent> readAll() {
    // Safe without locking: the map view is weakly consistent and every list it
    // yields is immutable, so this can never observe a partially written stream.
    List<StoredEvent> all = new ArrayList<>();
    for (List<StoredEvent> stream : streams.values()) {
      all.addAll(stream);
    }
    all.sort((a, b) -> Long.compare(a.globalSequence(), b.globalSequence()));
    return List.copyOf(all);
  }
}
