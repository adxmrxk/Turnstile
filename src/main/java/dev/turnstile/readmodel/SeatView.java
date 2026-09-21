package dev.turnstile.readmodel;

import java.time.Instant;

/**
 * A seat as the read side reports it. {@code status} is what the log says;
 * whether a HELD seat has since lapsed is decided at query time from
 * {@code expiresAt}, because expiry is evaluated, not scheduled.
 */
public record SeatView(
    String seatId, String status, String holdId, Instant expiresAt, long version) {

  public SeatView effectiveAt(Instant now) {
    if ("HELD".equals(status) && expiresAt != null && !now.isBefore(expiresAt)) {
      return new SeatView(seatId, "AVAILABLE", null, null, version);
    }
    return this;
  }
}
