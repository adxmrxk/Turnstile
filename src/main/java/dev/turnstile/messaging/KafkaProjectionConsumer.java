package dev.turnstile.messaging;

import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.readmodel.SeatMapProjection;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;

/**
 * Feeds the seat map from the topic, the way a separate read service would.
 *
 * <p>Delivery is at-least-once, so this must tolerate the same event twice. It
 * does, because {@link SeatMapProjection#apply} ignores any version it has
 * already applied and re-reads the log if one is missing.
 *
 * <p><b>It does not replay history.</b> On startup the projection has already been
 * rebuilt from the log itself, so every event committed before that moment is
 * accounted for. Re-reading the whole topic on top of that only feeds it
 * duplicates, and does so on every restart, for longer each time the topic grows.
 * Instead each partition is positioned at the time the rebuild began, less a small
 * margin. Anything committed after that is published after that (an event cannot
 * reach Kafka before it commits), so nothing is missed. The margin only covers
 * clock differences between the servers that publish; an event that still slipped
 * through is repaired the next time its seat changes, because a skipped version
 * makes the projection re-read that seat from the log.
 */
public final class KafkaProjectionConsumer extends AbstractConsumerSeekAware {

  private final SeatMapProjection projection;
  private final EventCodec codec;
  private final Duration margin;

  public KafkaProjectionConsumer(SeatMapProjection projection, EventCodec codec, Duration margin) {
    this.projection = projection;
    this.codec = codec;
    this.margin = margin;
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    super.onPartitionsAssigned(assignments, callback);
    long from = projection.rebuildStartedAt().minus(margin).toEpochMilli();
    for (TopicPartition partition : assignments.keySet()) {
      callback.seekToTimestamp(partition.topic(), partition.partition(), from);
    }
  }

  /**
   * Every instance needs <em>every</em> event, because each keeps its own seat map.
   * A shared consumer group would do the opposite: Kafka would split the topic's
   * partitions between the instances, and each would see only a slice of the log
   * and serve a wrong map. So each instance gets its own group, unique to this
   * process. Groups are never reused, so stale ones pile up on the broker until it
   * expires them.
   */
  @KafkaListener(
      id = SeatMapGroupCleanup.LISTENER_ID,
      topics = "${turnstile.kafka.topic:turnstile.seat-events}",
      // Pinned here, not left to the global setting. If nothing on a partition is newer
      // than the seek time there is no offset to seek to, and the consumer falls back to
      // this policy: "earliest" would then replay the entire topic, which is exactly what
      // the seek exists to avoid. History is covered by the rebuild from the log.
      properties = "auto.offset.reset=latest",
      groupId = "turnstile-seat-map-#{T(java.util.UUID).randomUUID().toString()}")
  public void onEvent(String envelope) {
    StoredEvent stored = codec.fromEnvelope(envelope);
    projection.apply(stored.streamId(), stored.version(), stored.event());
  }
}
