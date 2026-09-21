package dev.turnstile.eventstore;

import dev.turnstile.domain.DomainEvent;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Wraps any {@link EventStore} and tells listeners about each append that was
 * actually written.
 *
 * <p>It decorates rather than changing the store, so the contract, the model
 * checker and the mutation harness all keep exercising the undecorated store, and
 * the same wrapper serves memory and Postgres alike. A deduplicated append wrote
 * nothing, so it announces nothing.
 */
public final class NotifyingEventStore implements EventStore {

  /** Called after an append is durable. Must not throw into the writer's path. */
  public interface AppendListener {
    void appended(String streamId, long firstVersion, List<DomainEvent> events);
  }

  private final EventStore delegate;
  private final List<AppendListener> listeners = new CopyOnWriteArrayList<>();

  public NotifyingEventStore(EventStore delegate) {
    this.delegate = delegate;
  }

  public void subscribe(AppendListener listener) {
    listeners.add(listener);
  }

  @Override
  public AppendResult append(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {
    AppendResult result = delegate.append(streamId, expectedVersion, events, idempotencyKey);
    if (!result.deduplicated() && !events.isEmpty()) {
      for (AppendListener listener : listeners) {
        try {
          listener.appended(streamId, expectedVersion + 1, events);
        } catch (RuntimeException listenerFailure) {
          // A broken read model must never fail a write that already committed.
        }
      }
    }
    return result;
  }

  @Override
  public List<StoredEvent> load(String streamId) {
    return delegate.load(streamId);
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
