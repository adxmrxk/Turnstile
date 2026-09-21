package dev.turnstile.messaging;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes to Kafka and waits for the acknowledgement. Waiting matters: the
 * relay marks an outbox row published only after this returns, so an
 * unacknowledged send must surface as a failure and be retried, never assumed.
 */
public final class KafkaEventPublisher implements EventPublisher {

  private final KafkaTemplate<String, String> kafka;
  private final String topic;

  public KafkaEventPublisher(KafkaTemplate<String, String> kafka, String topic) {
    this.kafka = kafka;
    this.topic = topic;
  }

  @Override
  public void publish(String partitionKey, String eventKey, String envelopeJson) {
    try {
      kafka.send(topic, partitionKey, envelopeJson).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted publishing " + eventKey, e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("kafka did not acknowledge " + eventKey, e);
    }
  }
}
