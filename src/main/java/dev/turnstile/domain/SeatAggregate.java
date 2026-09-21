package dev.turnstile.domain;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The seat state machine, and the only place a seat's fate is decided.
 *
 * <pre>
 *   AVAILABLE --hold--&gt; HELD --confirm--&gt; SOLD   (terminal)
 *       ^                 |
 *       +---release/------+
 *           expire
 * </pre>
 *
 * <p>Two design decisions carry the whole no-oversell guarantee.
 *
 * <p><b>Expiry is evaluated, not scheduled.</b> There is no background sweeper
 * deciding when a hold dies. A hold is expired if {@code now > expiresAt} at the
 * moment a command is handled, computed from an injected {@link Clock}. A
 * sweeper is an optimisation for freeing inventory promptly; it is never load
 * bearing for correctness. That distinction matters, because a correctness
 * guarantee that depends on a cron job firing on time is not a guarantee.
 *
 * <p><b>The aggregate is pure.</b> {@code decide} reads state and returns the
 * events it wants appended. It writes nothing and mutates nothing. All the
 * atomicity lives one layer out, in the event store's compare-and-append. That
 * is what lets the same logic be tested single-threaded and then trusted under
 * 200k concurrent buyers: the aggregate never had a race to lose.
 */
public final class SeatAggregate {

  private final String seatId;
  private final Clock clock;

  private SeatStatus status = SeatStatus.AVAILABLE;
  private String activeHoldId;
  private String activeBuyerId;
  private Instant holdExpiresAt;

  private SeatAggregate(String seatId, Clock clock) {
    this.seatId = Objects.requireNonNull(seatId, "seatId");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /** Folds the event log into current state. This is the only way to build one. */
  public static SeatAggregate replay(String seatId, List<DomainEvent> history, Clock clock) {
    SeatAggregate seat = new SeatAggregate(seatId, clock);
    for (DomainEvent event : history) {
      seat.apply(event);
    }
    return seat;
  }

  /**
   * Folds one event into state.
   *
   * <p>Written with instanceof patterns because pattern matching for switch is
   * still a preview feature on Java 17, which is what this build targets. On 21
   * this becomes an exhaustive switch over the sealed interface and the trailing
   * throw disappears, because the compiler takes over the job of proving every
   * case is handled. Until then the throw is doing that work at runtime, which
   * is strictly worse and is one of the reasons to move to 21.
   */
  private void apply(DomainEvent event) {
    if (event instanceof DomainEvent.SeatHeld held) {
      status = SeatStatus.HELD;
      activeHoldId = held.holdId();
      activeBuyerId = held.buyerId();
      holdExpiresAt = held.expiresAt();

    } else if (event instanceof DomainEvent.HoldReleased) {
      status = SeatStatus.AVAILABLE;
      activeHoldId = null;
      activeBuyerId = null;
      holdExpiresAt = null;

    } else if (event instanceof DomainEvent.SeatSold sold) {
      status = SeatStatus.SOLD;
      activeHoldId = sold.holdId();
      activeBuyerId = sold.buyerId();
      holdExpiresAt = null;

    } else {
      throw new IllegalStateException("unhandled event type: " + event.getClass().getName());
    }
  }

  /**
   * Places a hold. Succeeds when the seat is free, or when the standing hold has
   * already expired, in which case the expiry is recorded as its own event first
   * so the log explains why the seat changed hands.
   */
  public List<DomainEvent> hold(String holdId, String buyerId, Duration ttl) {
    Instant now = clock.instant();
    List<DomainEvent> emitted = new ArrayList<>(2);

    switch (effectiveStatus(now)) {
      case SOLD -> throw new DomainException.SeatAlreadySold(seatId);
      case HELD -> throw new DomainException.SeatAlreadyHeld(seatId);
      case AVAILABLE -> {
        // A hold that lapsed is retired explicitly rather than being silently
        // overwritten, so the log stays a complete account of the seat.
        if (status == SeatStatus.HELD) {
          emitted.add(
              new DomainEvent.HoldReleased(
                  seatId, activeHoldId, now, DomainEvent.ReleaseReason.EXPIRED));
        }
        emitted.add(new DomainEvent.SeatHeld(seatId, holdId, buyerId, now, now.plus(ttl)));
      }
    }
    return List.copyOf(emitted);
  }

  /** Turns a live hold into a sale. The only transition into {@code SOLD}. */
  public List<DomainEvent> confirmSale(String holdId, String orderId) {
    Instant now = clock.instant();

    if (status == SeatStatus.SOLD) {
      throw new DomainException.SeatAlreadySold(seatId);
    }
    if (status != SeatStatus.HELD || !holdId.equals(activeHoldId)) {
      throw new DomainException.HoldNotFound(seatId, holdId);
    }
    // Payment that lands after the hold lapsed must not produce a sale, or the
    // seat could already belong to somebody else. The saga compensates by
    // refunding; the domain simply refuses.
    if (isExpired(now)) {
      throw new DomainException.HoldExpired(seatId, holdId);
    }
    return List.of(new DomainEvent.SeatSold(seatId, holdId, orderId, activeBuyerId, now));
  }

  /** Buyer walked away. Idempotent against an already-lapsed hold. */
  public List<DomainEvent> releaseHold(String holdId) {
    Instant now = clock.instant();

    if (status == SeatStatus.SOLD) {
      throw new DomainException.SeatAlreadySold(seatId);
    }
    if (status != SeatStatus.HELD || !holdId.equals(activeHoldId)) {
      throw new DomainException.HoldNotFound(seatId, holdId);
    }
    DomainEvent.ReleaseReason reason =
        isExpired(now) ? DomainEvent.ReleaseReason.EXPIRED : DomainEvent.ReleaseReason.ABANDONED;
    return List.of(new DomainEvent.HoldReleased(seatId, holdId, now, reason));
  }

  /** Status as of {@code now}, treating a lapsed hold as free inventory. */
  public SeatStatus effectiveStatus(Instant now) {
    if (status == SeatStatus.HELD && isExpired(now)) {
      return SeatStatus.AVAILABLE;
    }
    return status;
  }

  private boolean isExpired(Instant now) {
    return holdExpiresAt != null && !now.isBefore(holdExpiresAt);
  }

  public String seatId() {
    return seatId;
  }

  public SeatStatus status() {
    return status;
  }

  public String activeHoldId() {
    return activeHoldId;
  }
}
