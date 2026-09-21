package dev.turnstile.query;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.domain.SeatAggregate;
import dev.turnstile.domain.SeatStatus;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Reads that go to the log itself rather than the read model, so the answer is
 * authoritative and can be asked about any moment in the past.
 */
public final class SeatQueries {

  /** A seat's state at some instant, derived by replaying its own history up to it. */
  public record SeatState(String seatId, SeatStatus status, String holdId, long eventsApplied) {}

  private final EventStore log;
  private final Clock clock;

  public SeatQueries(EventStore log, Clock clock) {
    this.log = log;
    this.clock = clock;
  }

  public List<StoredEvent> history(String seatId) {
    return log.load(seatId);
  }

  public SeatState current(String seatId) {
    return asOf(seatId, clock.instant());
  }

  /**
   * Time travel. Only events that had happened by {@code instant} are replayed,
   * and the clock the aggregate consults is pinned to that instant, so a hold
   * that had lapsed by then reads as available, exactly as it would have then.
   */
  public SeatState asOf(String seatId, Instant instant) {
    List<DomainEvent> events =
        log.load(seatId).stream()
            .filter(e -> !e.event().occurredAt().isAfter(instant))
            .map(StoredEvent::event)
            .toList();
    SeatAggregate seat = SeatAggregate.replay(seatId, events, Clock.fixed(instant, ZoneOffset.UTC));
    SeatStatus status = seat.effectiveStatus(instant);
    String hold = status == SeatStatus.AVAILABLE ? null : seat.activeHoldId();
    return new SeatState(seatId, status, hold, events.size());
  }
}
