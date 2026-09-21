package dev.turnstile.saga;

/** Where one purchase is. Persisted after every step. */
public record SagaRecord(
    String sagaId,
    String seatId,
    String buyerId,
    String holdId,
    String orderId,
    long amountCents,
    State state,
    String detail) {

  public enum State {
    /** Nothing done yet. */
    STARTED,
    /** The seat is held; payment not yet confirmed. */
    HELD,
    /** Money is taken; the sale is not yet recorded. */
    PAID,
    /** Terminal: the buyer has the seat. */
    CONFIRMED,
    /** Terminal: the seat was not available. */
    REFUSED,
    /** Terminal: payment failed, the hold was released. */
    DECLINED,
    /** Terminal: money was taken but the seat could not be sold, so it was returned. */
    REFUNDED;

    public boolean terminal() {
      return this == CONFIRMED || this == REFUSED || this == DECLINED || this == REFUNDED;
    }
  }

  public SagaRecord with(State next, String why) {
    return new SagaRecord(sagaId, seatId, buyerId, holdId, orderId, amountCents, next, why);
  }
}
