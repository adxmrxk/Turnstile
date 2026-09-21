package dev.turnstile;

import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.query.SeatQueries;
import dev.turnstile.readmodel.SeatMapProjection;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The query side: a read model folded from the log, and reads that go to the log. */
@Configuration
public class ReadSideConfig {

  /**
   * The seat map. Rebuilt from the log at startup, so a restart over a durable
   * store comes back with the right picture. By default it hears about appends
   * in-process; with Kafka enabled it is fed by the consumer instead, so it
   * follows the log the way a separate read service would.
   */
  @Bean
  SeatMapProjection seatMap(
      NotifyingEventStore store,
      Clock clock,
      @Value("${turnstile.kafka.enabled:false}") boolean viaKafka,
      io.micrometer.core.instrument.MeterRegistry metrics) {
    SeatMapProjection projection = new SeatMapProjection(store, clock);
    projection.instrument(metrics);
    projection.rebuild();
    if (!viaKafka) {
      store.subscribe(
          (streamId, firstVersion, events) -> {
            long version = firstVersion;
            for (var event : events) {
              projection.apply(streamId, version++, event);
            }
          });
    }
    return projection;
  }

  @Bean
  SeatQueries seatQueries(NotifyingEventStore store, Clock clock) {
    return new SeatQueries(store, clock);
  }
}
