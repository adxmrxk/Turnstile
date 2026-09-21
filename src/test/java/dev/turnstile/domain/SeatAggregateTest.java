package dev.turnstile.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * State machine tests. Every one drives a controllable clock rather than
 * sleeping, so hold expiry is tested deterministically and the suite stays fast.
 */
class SeatAggregateTest {

  private static final Instant T0 = Instant.parse("2026-09-04T12:00:00Z");
  private static final Duration TTL = Duration.ofMinutes(2);
  private static final String SEAT = "seat-A1";

  /** A clock the test moves by hand. Real time never enters these tests. */
  private static final class TestClock extends Clock {
    private Instant now;

    TestClock(Instant start) {
      this.now = start;
    }

    void advance(Duration by) {
      now = now.plus(by);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }
  }

  private final TestClock clock = new TestClock(T0);
  private final List<DomainEvent> history = new ArrayList<>();

  private SeatAggregate seat() {
    return SeatAggregate.replay(SEAT, history, clock);
  }

  private void given(List<DomainEvent> events) {
    history.addAll(events);
  }

  @Nested
  @DisplayName("holding")
  class Holding {

    @Test
    void an_available_seat_can_be_held() {
      List<DomainEvent> events = seat().hold("hold-1", "buyer-1", TTL);

      assertThat(events).hasSize(1);
      assertThat(events.get(0)).isInstanceOf(DomainEvent.SeatHeld.class);

      DomainEvent.SeatHeld held = (DomainEvent.SeatHeld) events.get(0);
      assertThat(held.holdId()).isEqualTo("hold-1");
      assertThat(held.buyerId()).isEqualTo("buyer-1");
      assertThat(held.expiresAt()).isEqualTo(T0.plus(TTL));
    }

    @Test
    void a_seat_held_by_someone_else_is_refused() {
      given(seat().hold("hold-1", "buyer-1", TTL));

      assertThatThrownBy(() -> seat().hold("hold-2", "buyer-2", TTL))
          .isInstanceOf(DomainException.SeatAlreadyHeld.class)
          .hasMessageContaining(SEAT);
    }

    @Test
    @DisplayName("a lapsed hold frees the seat and is retired in the log")
    void an_expired_hold_releases_the_seat() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      clock.advance(TTL.plusSeconds(1));

      List<DomainEvent> events = seat().hold("hold-2", "buyer-2", TTL);

      // The expiry is recorded rather than implied, so the log alone explains
      // why the seat changed hands.
      assertThat(events).hasSize(2);
      assertThat(events.get(0)).isInstanceOf(DomainEvent.HoldReleased.class);
      assertThat(((DomainEvent.HoldReleased) events.get(0)).reason())
          .isEqualTo(DomainEvent.ReleaseReason.EXPIRED);
      assertThat(events.get(1)).isInstanceOf(DomainEvent.SeatHeld.class);
    }

    @Test
    void a_hold_one_instant_before_expiry_still_holds() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      clock.advance(TTL.minusMillis(1));

      assertThatThrownBy(() -> seat().hold("hold-2", "buyer-2", TTL))
          .isInstanceOf(DomainException.SeatAlreadyHeld.class);
    }

    @Test
    void a_sold_seat_can_never_be_held_again() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      given(seat().confirmSale("hold-1", "order-1"));

      assertThatThrownBy(() -> seat().hold("hold-2", "buyer-2", TTL))
          .isInstanceOf(DomainException.SeatAlreadySold.class);
    }
  }

  @Nested
  @DisplayName("confirming a sale")
  class Confirming {

    @Test
    void a_live_hold_can_be_converted() {
      given(seat().hold("hold-1", "buyer-1", TTL));

      List<DomainEvent> events = seat().confirmSale("hold-1", "order-1");

      assertThat(events).hasSize(1);
      DomainEvent.SeatSold sold = (DomainEvent.SeatSold) events.get(0);
      assertThat(sold.orderId()).isEqualTo("order-1");
      assertThat(sold.buyerId()).as("the buyer is carried from the hold").isEqualTo("buyer-1");
    }

    @Test
    void an_available_seat_cannot_be_sold() {
      assertThatThrownBy(() -> seat().confirmSale("hold-1", "order-1"))
          .isInstanceOf(DomainException.HoldNotFound.class);
    }

    @Test
    void a_hold_belonging_to_another_buyer_cannot_be_converted() {
      given(seat().hold("hold-1", "buyer-1", TTL));

      assertThatThrownBy(() -> seat().confirmSale("hold-2", "order-2"))
          .isInstanceOf(DomainException.HoldNotFound.class);
    }

    @Test
    @DisplayName("payment landing after expiry is refused, not honoured")
    void an_expired_hold_cannot_be_converted() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      clock.advance(TTL.plusSeconds(1));

      // The saga compensates with a refund. The domain must not sell a seat
      // that has already returned to the pool.
      assertThatThrownBy(() -> seat().confirmSale("hold-1", "order-1"))
          .isInstanceOf(DomainException.HoldExpired.class);
    }

    @Test
    void a_seat_cannot_be_sold_twice() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      given(seat().confirmSale("hold-1", "order-1"));

      assertThatThrownBy(() -> seat().confirmSale("hold-1", "order-2"))
          .isInstanceOf(DomainException.SeatAlreadySold.class);
    }
  }

  @Nested
  @DisplayName("releasing")
  class Releasing {

    @Test
    void a_buyer_can_walk_away() {
      given(seat().hold("hold-1", "buyer-1", TTL));

      List<DomainEvent> events = seat().releaseHold("hold-1");

      assertThat(((DomainEvent.HoldReleased) events.get(0)).reason())
          .isEqualTo(DomainEvent.ReleaseReason.ABANDONED);
    }

    @Test
    void releasing_after_expiry_is_recorded_as_an_expiry() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      clock.advance(TTL.plusSeconds(1));

      List<DomainEvent> events = seat().releaseHold("hold-1");

      assertThat(((DomainEvent.HoldReleased) events.get(0)).reason())
          .isEqualTo(DomainEvent.ReleaseReason.EXPIRED);
    }

    @Test
    void the_seat_returns_to_the_pool_after_release() {
      given(seat().hold("hold-1", "buyer-1", TTL));
      given(seat().releaseHold("hold-1"));

      assertThat(seat().effectiveStatus(clock.instant())).isEqualTo(SeatStatus.AVAILABLE);
      assertThat(seat().hold("hold-2", "buyer-2", TTL)).hasSize(1);
    }
  }

  @Test
  @DisplayName("replaying the same log twice yields the same state")
  void replay_is_deterministic() {
    given(seat().hold("hold-1", "buyer-1", TTL));
    given(seat().releaseHold("hold-1"));
    given(seat().hold("hold-2", "buyer-2", TTL));
    given(seat().confirmSale("hold-2", "order-2"));

    SeatAggregate first = SeatAggregate.replay(SEAT, history, clock);
    SeatAggregate second = SeatAggregate.replay(SEAT, history, clock);

    assertThat(first.status()).isEqualTo(second.status()).isEqualTo(SeatStatus.SOLD);
    assertThat(first.activeHoldId()).isEqualTo(second.activeHoldId()).isEqualTo("hold-2");
  }
}
