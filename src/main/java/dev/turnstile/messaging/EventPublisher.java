package dev.turnstile.messaging;

/** Where announced events go. Kafka in production, anything in a test. */
public interface EventPublisher {

  /**
   * Publishes one event and returns only once the broker has acknowledged it.
   * Throws if it cannot be sure it was delivered.
   *
   * @param partitionKey the seat, so all of a seat's events land on one partition
   *     and stay in order
   * @param eventKey unique per event; consumers use it to drop duplicates
   */
  void publish(String partitionKey, String eventKey, String envelopeJson);
}
