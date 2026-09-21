package dev.turnstile.messaging;

import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.readmodel.SeatMapProjection;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Feeds the seat map from the topic, the way a separate read service would.
 *
 * <p>Delivery is at-least-once, so this must tolerate the same event twice. It
 * does, because {@link SeatMapProjection#apply} ignores any version it has
 * already applied and re-reads the log if one is missing.
 */
public final class KafkaProjectionConsumer {

  private final SeatMapProjection projection;
  private final EventCodec codec;

  public KafkaProjectionConsumer(SeatMapProjection projection, EventCodec codec) {
    this.projection = projection;
    this.codec = codec;
  }

  @KafkaListener(topics = "${turnstile.kafka.topic:turnstile.seat-events}", groupId = "turnstile-seat-map")
  public void onEvent(String envelope) {
    StoredEvent stored = codec.fromEnvelope(envelope);
    projection.apply(stored.streamId(), stored.version(), stored.event());
  }
}
