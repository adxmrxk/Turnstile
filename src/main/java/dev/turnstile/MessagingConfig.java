package dev.turnstile;

import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.messaging.EventPublisher;
import dev.turnstile.messaging.KafkaEventPublisher;
import dev.turnstile.messaging.KafkaProjectionConsumer;
import dev.turnstile.messaging.OutboxRelay;
import dev.turnstile.readmodel.SeatMapProjection;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Kafka, the outbox relay and the read-model consumer. Off unless
 * {@code turnstile.kafka.enabled=true}, and it needs the Postgres store because
 * the outbox is a table written in the same transaction as the events.
 */
@Configuration
@EnableKafka
@ConditionalOnProperty(name = "turnstile.kafka.enabled", havingValue = "true")
public class MessagingConfig {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingConfig.class);

  private final ScheduledExecutorService poller =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "outbox-relay");
            t.setDaemon(true);
            return t;
          });

  MessagingConfig(@Value("${turnstile.store:memory}") String store) {
    if (!"postgres".equals(store)) {
      throw new IllegalStateException(
          "turnstile.kafka.enabled=true needs turnstile.store=postgres: the outbox is a table"
              + " written in the same transaction as the events, and the in-memory store has none");
    }
  }

  @Bean
  NewTopic seatEventsTopic(@Value("${turnstile.kafka.topic:turnstile.seat-events}") String topic) {
    return TopicBuilder.name(topic).partitions(6).replicas(1).build();
  }

  @Bean
  EventPublisher eventPublisher(
      KafkaTemplate<String, String> kafka,
      @Value("${turnstile.kafka.topic:turnstile.seat-events}") String topic) {
    return new KafkaEventPublisher(kafka, topic);
  }

  @Bean
  OutboxRelay outboxRelay(DataSource dataSource, EventPublisher publisher, io.micrometer.core.instrument.MeterRegistry metrics) {
    OutboxRelay relay = new OutboxRelay(dataSource, publisher, 200);
    io.micrometer.core.instrument.Gauge.builder("turnstile.outbox.pending", relay, OutboxRelay::pending)
        .description("Events committed but not yet published to Kafka")
        .register(metrics);
    poller.scheduleWithFixedDelay(
        () -> {
          try {
            int n;
            while ((n = relay.pollOnce()) > 0) {
              metrics.counter("turnstile.outbox.published").increment(n);
              // keep draining while there is a backlog
            }
          } catch (RuntimeException e) {
            LOG.warn("outbox relay pass failed, will retry: {}", e.toString());
          }
        },
        500,
        200,
        TimeUnit.MILLISECONDS);
    return relay;
  }

  @Bean
  KafkaProjectionConsumer kafkaProjectionConsumer(SeatMapProjection projection, EventCodec codec) {
    return new KafkaProjectionConsumer(projection, codec);
  }

  @PreDestroy
  void stop() {
    poller.shutdownNow();
  }
}
