package dev.turnstile.domain;

import java.time.Instant;

/**
 * Everything that can happen to a seat. The event log is the source of truth;
 * seat state is always a fold over these, never a mutable row.
 *
 * <p>Sealed so the compiler forces every switch over an event to stay
 * exhaustive. Adding a new event type breaks the build at every point that
 * needs updating, which is exactly what you want in an event-sourced system
 * where a missed case silently corrupts a projection.
 */
public sealed interface DomainEvent {

  String seatId();

  Instant occurredAt();

  /** A buyer has taken this seat off the market until {@code expiresAt}. */
  record SeatHeld(
      String seatId,
      String holdId,
      String buyerId,
      Instant occurredAt,
      Instant expiresAt)
      implements DomainEvent {}

  /**
   * A hold ended without a sale. {@code reason} distinguishes a buyer walking
   * away from a hold that timed out, which matters for the funnel metrics the
   * read model builds later.
   */
  record HoldReleased(
      String seatId,
      String holdId,
      Instant occurredAt,
      ReleaseReason reason)
      implements DomainEvent {}

  /** Terminal. A seat that has been sold can never be held or sold again. */
  record SeatSold(
      String seatId,
      String holdId,
      String orderId,
      String buyerId,
      Instant occurredAt)
      implements DomainEvent {}

  enum ReleaseReason {
    ABANDONED,
    EXPIRED
  }
}
