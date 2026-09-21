package dev.turnstile.domain;

/**
 * A command that the current state legitimately refuses. These are expected
 * outcomes of a race, not bugs: when two buyers reach for the same seat, one of
 * them has to be told no. Distinct from ConcurrencyConflictException, which
 * means "your read was stale, try again" rather than "no".
 */
public class DomainException extends RuntimeException {

  public DomainException(String message) {
    super(message);
  }

  public static final class SeatAlreadyHeld extends DomainException {
    public SeatAlreadyHeld(String seatId) {
      super("seat " + seatId + " is already held by another buyer");
    }
  }

  public static final class SeatAlreadySold extends DomainException {
    public SeatAlreadySold(String seatId) {
      super("seat " + seatId + " has already been sold");
    }
  }

  public static final class HoldNotFound extends DomainException {
    public HoldNotFound(String seatId, String holdId) {
      super("hold " + holdId + " is not the active hold on seat " + seatId);
    }
  }

  public static final class HoldExpired extends DomainException {
    public HoldExpired(String seatId, String holdId) {
      super("hold " + holdId + " on seat " + seatId + " expired before payment");
    }
  }
}
