package dev.turnstile;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.InMemoryEventStore;
import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.metrics.MeteredEventStore;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.ZoneOffset;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * The only place Spring knows about the domain layers. The domain, event store
 * and command packages stay framework-free; this class assembles them.
 */
@Configuration
@EnableConfigurationProperties(HoldProperties.class)
public class TurnstileConfig {

  /** The default store: fast, and gone when the process exits. */
  @Bean(name = "rawEventStore")
  @ConditionalOnProperty(name = "turnstile.store", havingValue = "memory", matchIfMissing = true)
  EventStore inMemoryEventStore() {
    return new InMemoryEventStore();
  }

  /**
   * What the rest of the application sees: the configured store, wrapped so the
   * read model and live feeds hear about every committed append.
   */
  @Bean
  @Primary
  NotifyingEventStore eventStore(@Qualifier("rawEventStore") EventStore raw, MeterRegistry metrics) {
    return new NotifyingEventStore(new MeteredEventStore(raw, metrics));
  }

  @Bean
  EventCodec eventCodec() {
    return new EventCodec();
  }

  @Bean
  Clock clock() {
    return Clock.system(ZoneOffset.UTC);
  }

  @Bean
  SeatCommandHandler seatCommandHandler(NotifyingEventStore eventStore, Clock clock) {
    return new SeatCommandHandler(eventStore, clock);
  }
}
