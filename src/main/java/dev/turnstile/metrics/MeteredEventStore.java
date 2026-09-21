package dev.turnstile.metrics;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.AppendResult;
import dev.turnstile.eventstore.ConcurrencyConflictException;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.StoredEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.OptionalLong;

/**
 * Times every store operation and counts how each append ended.
 *
 * <p>The append outcome is the number that matters most operationally:
 * {@code conflict} is contention (someone else moved the seat first, and the
 * handler will re-judge), {@code deduplicated} is a retry being recognised, and a
 * rising conflict share is the earliest sign of a hot seat. It wraps the store
 * rather than changing it, so the contract tests and the model checker keep
 * exercising the unwrapped implementation.
 */
public final class MeteredEventStore implements EventStore {

  private final EventStore delegate;
  private final MeterRegistry registry;
  private final Timer load;
  private final Timer idempotencyLookup;

  public MeteredEventStore(EventStore delegate, MeterRegistry registry) {
    this.delegate = delegate;
    this.registry = registry;
    this.load = Timer.builder("turnstile.store.load").description("Time to load one seat's stream").register(registry);
    this.idempotencyLookup =
        Timer.builder("turnstile.store.idempotency.lookup").description("Time to look up an idempotency key").register(registry);
  }

  private Timer appendTimer(String outcome) {
    return Timer.builder("turnstile.store.append")
        .description("Time to append events, by how it ended")
        .tag("outcome", outcome)
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);
  }

  @Override
  public AppendResult append(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {
    long start = System.nanoTime();
    String outcome = "error";
    try {
      AppendResult result = delegate.append(streamId, expectedVersion, events, idempotencyKey);
      outcome = result.deduplicated() ? "deduplicated" : "ok";
      return result;
    } catch (ConcurrencyConflictException conflict) {
      outcome = "conflict";
      throw conflict;
    } finally {
      appendTimer(outcome).record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public List<StoredEvent> load(String streamId) {
    return load.record(() -> delegate.load(streamId));
  }

  @Override
  public OptionalLong versionForIdempotencyKey(String idempotencyKey) {
    return idempotencyLookup.record(() -> delegate.versionForIdempotencyKey(idempotencyKey));
  }

  @Override
  public List<StoredEvent> readAll() {
    return delegate.readAll();
  }

  @Override
  public void forEachEvent(java.util.function.Consumer<StoredEvent> sink) {
    delegate.forEachEvent(sink);
  }

  @Override
  public long currentVersion(String streamId) {
    return delegate.currentVersion(streamId);
  }
}
