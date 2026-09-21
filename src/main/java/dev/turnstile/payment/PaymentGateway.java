package dev.turnstile.payment;

/**
 * The slow, unreliable thing a hold has to survive. Two properties are required
 * of any implementation, because the saga's correctness leans on them:
 *
 * <ul>
 *   <li>{@link #charge} is idempotent per {@code orderId}: charging the same order
 *       twice moves money once and returns the same receipt. Without that a retry
 *       after a timeout could bill a buyer twice.
 *   <li>{@link #refund} works on the order, not on a receipt the caller might
 *       never have received. If the order was charged it is reversed; if it was
 *       not, it is cancelled so a charge that arrives late is refused.
 * </ul>
 */
public interface PaymentGateway {

  /** @return a receipt id */
  String charge(String orderId, String buyerId, long amountCents);

  /** Idempotent. Reverses a charge, or cancels the order if nothing was charged. */
  void refund(String orderId);

  /** The card was refused. Definitely no money moved. */
  final class PaymentDeclined extends RuntimeException {
    public PaymentDeclined(String message) {
      super(message);
    }
  }

  /**
   * No answer. The charge may or may not have happened, and the caller must not
   * assume either.
   */
  final class PaymentUncertain extends RuntimeException {
    public PaymentUncertain(String message) {
      super(message);
    }
  }
}
