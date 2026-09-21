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

  /**
   * Every instance needs <em>every</em> event, because each keeps its own seat map.
   * A shared consumer group would do the opposite: Kafka would split the topic's
   * partitions between the instances, and each would see only a slice of the log
   * and serve a wrong map. So each instance gets its own group, unique to this
   * process. The cost is that groups are never reused (a restart replays the topic
   * from the start, which is harmless because {@code apply} ignores duplicates),
   * and stale groups pile up on the broker until Kafka expires them.
   */
  @KafkaListener(
      topics = "${turnstile.kafka.topic:turnstile.seat-events}",
      groupId = "turnstile-seat-map-#{T(java.util.UUID).randomUUID().toString()}")
  public void onEvent(String envelope) {
    StoredEvent stored = codec.fromEnvelope(envelope);
    projection.apply(stored.streamId(), stored.version(), stored.event());
  }
}
