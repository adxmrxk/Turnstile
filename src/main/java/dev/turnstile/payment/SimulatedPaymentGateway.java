package dev.turnstile.payment;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * A payment provider that misbehaves on request, standing in for a real one.
 *
 * <p>It fails the ways real ones do: it declines cards, it takes long enough
 * that a hold expires underneath the buyer, it drops a request before processing
 * it, and, the nasty one, it processes a charge and then loses the response, so
 * the caller is told "timeout" for money that moved. Every outcome is recorded in
 * a ledger so a test can check that money and seats balance, not merely that no
 * exception escaped.
 */
public final class SimulatedPaymentGateway implements PaymentGateway {

  public enum Entry {
    CHARGED,
    REFUNDED,
    CANCELLED
  }

  private record Ledger(Entry state, long cents, String receipt) {}

  private final double declineRate;
  private final double dropRate;
  private final double lostResponseRate;
  private final long maxLatencyNanos;
  private final Random random;
  private final Map<String, Ledger> ledger = new ConcurrentHashMap<>();

  public SimulatedPaymentGateway(
      double declineRate,
      double dropRate,
      double lostResponseRate,
      long maxLatencyMillis,
      long seed) {
    this.declineRate = declineRate;
    this.dropRate = dropRate;
    this.lostResponseRate = lostResponseRate;
    this.maxLatencyNanos = maxLatencyMillis * 1_000_000L;
    this.random = new Random(seed);
  }

  /** A well-behaved provider. */
  public static SimulatedPaymentGateway reliable() {
    return new SimulatedPaymentGateway(0, 0, 0, 0, 1);
  }

  private synchronized double roll() {
    return random.nextDouble();
  }

  @Override
  public String charge(String orderId, String buyerId, long amountCents) {
    if (maxLatencyNanos > 0) {
      LockSupport.parkNanos((long) (roll() * maxLatencyNanos));
    }
    Ledger existing = ledger.get(orderId);
    if (existing != null) {
      if (existing.state() == Entry.CHARGED) {
        return existing.receipt(); // idempotent: same order, same receipt, money moves once
      }
      throw new PaymentDeclined("order " + orderId + " was " + existing.state());
    }
    if (roll() < dropRate) {
      throw new PaymentUncertain("request dropped before processing");
    }
    if (roll() < declineRate) {
      throw new PaymentDeclined("card declined");
    }
    String receipt = "rcpt-" + orderId;
    Ledger raced = ledger.putIfAbsent(orderId, new Ledger(Entry.CHARGED, amountCents, receipt));
    if (raced != null && raced.state() != Entry.CHARGED) {
      throw new PaymentDeclined("order " + orderId + " was " + raced.state());
    }
    if (roll() < lostResponseRate) {
      throw new PaymentUncertain("charged, but the response was lost");
    }
    return receipt;
  }

  @Override
  public void refund(String orderId) {
    ledger.compute(
        orderId,
        (id, current) -> {
          if (current == null) {
            return new Ledger(Entry.CANCELLED, 0, null);
          }
          if (current.state() == Entry.CHARGED) {
            return new Ledger(Entry.REFUNDED, current.cents(), current.receipt());
          }
          return current;
        });
  }

  /** Money currently held: charged and not refunded. */
  public long netCents() {
    return ledger.values().stream()
        .filter(l -> l.state() == Entry.CHARGED)
        .mapToLong(Ledger::cents)
        .sum();
  }

  /** Orders whose money is currently held. */
  public Set<String> chargedOrders() {
    Set<String> out = new TreeSet<>();
    ledger.forEach(
        (order, l) -> {
          if (l.state() == Entry.CHARGED) {
            out.add(order);
          }
        });
    return out;
  }

  public Entry stateOf(String orderId) {
    Ledger l = ledger.get(orderId);
    return l == null ? null : l.state();
  }
}
