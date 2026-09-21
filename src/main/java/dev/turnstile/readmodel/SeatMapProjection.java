package dev.turnstile.readmodel;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * The seat map: a read model folded from the event log.
 *
 * <p>Feeding it is idempotent and order-tolerant, because the feeds it will
 * eventually get (in-process notifications, or Kafka delivering at least once)
 * make no stronger promise:
 *
 * <ul>
 *   <li>an event at or below the version already applied is a duplicate and is
 *       ignored;
 *   <li>an event that skips ahead means this projection missed something, so the
 *       stream is re-read from the log and refolded rather than guessed at.
 * </ul>
 *
 * <p>That second rule is what makes the projection self-healing, and what lets
 * {@link #rebuild} be a plain replay of the whole log.
 */
public final class SeatMapProjection {

  private final EventStore log;
  private final Clock clock;
  private final Map<String, SeatView> seats = new ConcurrentHashMap<>();
  private final List<Consumer<SeatView>> listeners = new CopyOnWriteArrayList<>();

  public SeatMapProjection(EventStore log, Clock clock) {
    this.log = log;
    this.clock = clock;
  }

  public void onChange(Consumer<SeatView> listener) {
    listeners.add(listener);
  }

  /** Discards everything and refolds the entire log. */
  public synchronized void rebuild() {
    seats.clear();
    for (StoredEvent stored : log.readAll()) {
      seats.compute(stored.streamId(), (id, current) -> fold(current, stored.version(), stored.event()));
    }
  }

  /** Applies one event. Safe to call twice with the same event. */
  public void apply(String streamId, long version, DomainEvent event) {
    SeatView changed;
    synchronized (this) {
      SeatView current = seats.get(streamId);
      long applied = current == null ? 0 : current.version();
      if (version <= applied) {
        return; // duplicate delivery
      }
      if (version > applied + 1) {
        refold(streamId); // missed something; the log is the source of truth
        changed = seats.get(streamId);
      } else {
        changed = fold(current, version, event);
        seats.put(streamId, changed);
      }
    }
    if (changed != null) {
      for (Consumer<SeatView> listener : listeners) {
        listener.accept(changed);
      }
    }
  }

  private void refold(String streamId) {
    SeatView view = null;
    for (StoredEvent stored : log.load(streamId)) {
      view = fold(view, stored.version(), stored.event());
    }
    if (view != null) {
      seats.put(streamId, view);
    }
  }

  private static SeatView fold(SeatView current, long version, DomainEvent event) {
    String seatId = event.seatId();
    if (event instanceof DomainEvent.SeatHeld held) {
      return new SeatView(seatId, "HELD", held.holdId(), held.expiresAt(), version);
    } else if (event instanceof DomainEvent.HoldReleased) {
      return new SeatView(seatId, "AVAILABLE", null, null, version);
    } else if (event instanceof DomainEvent.SeatSold sold) {
      return new SeatView(seatId, "SOLD", sold.holdId(), null, version);
    }
    throw new IllegalStateException("unhandled event: " + event);
  }

  public SeatView get(String seatId) {
    SeatView view = seats.get(seatId);
    return view == null ? null : view.effectiveAt(clock.instant());
  }

  /** Every seat that has ever had an event, as of now. */
  public List<SeatView> all() {
    List<SeatView> out = new ArrayList<>();
    for (SeatView view : seats.values()) {
      out.add(view.effectiveAt(clock.instant()));
    }
    out.sort((a, b) -> a.seatId().compareTo(b.seatId()));
    return out;
  }

  public Map<String, Long> counts() {
    Map<String, Long> counts = new java.util.TreeMap<>();
    for (SeatView view : all()) {
      counts.merge(view.status(), 1L, Long::sum);
    }
    return counts;
  }
}
