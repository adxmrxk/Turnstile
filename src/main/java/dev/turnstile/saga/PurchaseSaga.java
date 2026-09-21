package dev.turnstile.saga;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainException;
import dev.turnstile.payment.PaymentGateway;
import dev.turnstile.saga.SagaRecord.State;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Buys a seat: hold it, take payment, confirm the sale, and undo whatever is
 * necessary when a step cannot finish.
 *
 * <pre>
 *   STARTED --hold--> HELD --charge--> PAID --confirm--> CONFIRMED
 *      |                |                |
 *   REFUSED         DECLINED          REFUNDED
 *  (seat gone)   (hold released)   (money returned)
 * </pre>
 *
 * <p>Three properties make this safe, and each is tested rather than asserted:
 *
 * <p><b>Every step is idempotent.</b> The hold, the sale and the release carry
 * deterministic idempotency keys derived from the saga id, and the gateway charges
 * an order once however often it is asked. So a step can be repeated after a
 * crash without effect, which is what allows the next property.
 *
 * <p><b>State is saved after each step, and a crash can fall between an effect
 * and its save.</b> Resuming from the last saved state re-runs that one step,
 * which is harmless because of the first property. The worst place to die is
 * after the money moved and before "PAID" was written; resuming charges again,
 * gets the same receipt, and carries on.
 *
 * <p><b>Compensation never assumes.</b> When payment gives no answer, the saga does
 * not guess that it failed. It retries (safe, because charging is idempotent),
 * and if it still cannot tell, it refunds the <em>order</em>, which reverses the
 * charge if one landed and blocks one that arrives late.
 */
public final class PurchaseSaga {

  static final int CHARGE_ATTEMPTS = 3;

  /** Test hook: throw from here to simulate the process dying at a precise point. */
  public interface CrashPoint {
    void reached(String point);

    CrashPoint NEVER = point -> {};
  }

  private final SeatCommandHandler handler;
  private final PaymentGateway gateway;
  private final SagaLog log;
  private final Duration holdTtl;
  private final CrashPoint crash;

  public PurchaseSaga(
      SeatCommandHandler handler,
      PaymentGateway gateway,
      SagaLog log,
      Duration holdTtl,
      CrashPoint crash) {
    this.handler = handler;
    this.gateway = gateway;
    this.log = log;
    this.holdTtl = holdTtl;
    this.crash = crash;
  }

  public PurchaseSaga(
      SeatCommandHandler handler, PaymentGateway gateway, SagaLog log, Duration holdTtl) {
    this(handler, gateway, log, holdTtl, CrashPoint.NEVER);
  }

  /**
   * Starts a purchase, or returns the existing one for this id. Safe to call
   * repeatedly with the same id: a client retrying gets the same purchase, never
   * a second one. Only the caller that created the saga drives it; a duplicate
   * waits for that driver instead of racing it.
   */
  public SagaRecord start(String sagaId, String seatId, String buyerId, long amountCents) {
    SagaRecord fresh =
        new SagaRecord(
            sagaId, seatId, buyerId, "hold-" + sagaId, "order-" + sagaId, amountCents,
            State.STARTED, null);
    if (log.insert(fresh)) {
      return advance(fresh);
    }
    return awaitTerminal(sagaId);
  }

  /** Continues every saga a previous process left unfinished. */
  public List<SagaRecord> recoverIncomplete() {
    List<SagaRecord> finished = new ArrayList<>();
    for (SagaRecord stuck : log.incomplete()) {
      finished.add(advance(stuck));
    }
    return finished;
  }

  public Optional<SagaRecord> find(String sagaId) {
    return log.find(sagaId);
  }

  private SagaRecord awaitTerminal(String sagaId) {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    SagaRecord latest = log.find(sagaId).orElseThrow();
    while (!latest.state().terminal() && System.nanoTime() < deadline) {
      java.util.concurrent.locks.LockSupport.parkNanos(2_000_000);
      latest = log.find(sagaId).orElse(latest);
    }
    return latest;
  }

  private SagaRecord advance(SagaRecord saga) {
    SagaRecord current = saga;
    while (!current.state().terminal()) {
      current = step(current);
      log.update(current);
    }
    return current;
  }

  private SagaRecord step(SagaRecord s) {
    return switch (s.state()) {
      case STARTED -> hold(s);
      case HELD -> pay(s);
      case PAID -> confirm(s);
      default -> throw new IllegalStateException("terminal saga has no next step: " + s);
    };
  }

  private SagaRecord hold(SagaRecord s) {
    try {
      handler.hold(s.seatId(), s.holdId(), s.buyerId(), holdTtl, key(s, "hold"));
    } catch (DomainException unavailable) {
      return s.with(State.REFUSED, unavailable.getMessage());
    }
    crash.reached("after-hold");
    return s.with(State.HELD, null);
  }

  private SagaRecord pay(SagaRecord s) {
    String failure = "payment unconfirmed after " + CHARGE_ATTEMPTS + " attempts";
    for (int attempt = 1; attempt <= CHARGE_ATTEMPTS; attempt++) {
      try {
        gateway.charge(s.orderId(), s.buyerId(), s.amountCents());
        crash.reached("after-charge");
        return s.with(State.PAID, null);
      } catch (PaymentGateway.PaymentDeclined declined) {
        failure = declined.getMessage();
        break;
      } catch (PaymentGateway.PaymentUncertain uncertain) {
        failure = uncertain.getMessage();
      }
    }

    // Reverse-or-cancel the order first: if the charge did land, this returns the
    // money; if it did not, it stops one that is still in flight from landing.
    gateway.refund(s.orderId());
    releaseHold(s);
    return s.with(State.DECLINED, failure);
  }

  private SagaRecord confirm(SagaRecord s) {
    try {
      handler.confirmSale(s.seatId(), s.holdId(), s.orderId(), key(s, "confirm"));
    } catch (DomainException cannotSell) {
      // The money is taken and the seat cannot be sold: the hold lapsed while
      // payment ran, or the seat went to someone else. Give the money back.
      gateway.refund(s.orderId());
      return s.with(State.REFUNDED, cannotSell.getMessage());
    }
    crash.reached("after-confirm");
    return s.with(State.CONFIRMED, null);
  }

  private void releaseHold(SagaRecord s) {
    try {
      handler.releaseHold(s.seatId(), s.holdId(), key(s, "release"));
    } catch (DomainException alreadyGone) {
      // The hold expired, or was released already. Either way the seat is free.
    }
  }

  private static String key(SagaRecord s, String step) {
    return "saga:" + s.sagaId() + ":" + step;
  }
}
