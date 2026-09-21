package dev.turnstile;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.payment.PaymentGateway;
import dev.turnstile.payment.SimulatedPaymentGateway;
import dev.turnstile.saga.InMemorySagaLog;
import dev.turnstile.saga.PurchaseSaga;
import dev.turnstile.saga.SagaLog;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Purchases: payment, the saga, and finishing whatever a previous run left half done. */
@Configuration
@EnableConfigurationProperties({PaymentProperties.class, PricingProperties.class})
public class SagaConfig {

  /**
   * A simulated provider. It is a stand-in for a real payment service and says so
   * in its name; nothing here talks to a real one. Failure rates default to zero
   * and can be turned up to watch the compensation paths work.
   */
  @Bean
  PaymentGateway paymentGateway(PaymentProperties p) {
    return new SimulatedPaymentGateway(
        p.declineRate(), p.dropRate(), p.lostResponseRate(), p.maxLatencyMillis(), p.seed());
  }

  @Bean
  @ConditionalOnProperty(name = "turnstile.store", havingValue = "memory", matchIfMissing = true)
  SagaLog inMemorySagaLog() {
    return new InMemorySagaLog();
  }

  @Bean
  PurchaseSaga purchaseSaga(
      io.micrometer.core.instrument.MeterRegistry metrics,
      SeatCommandHandler handler, PaymentGateway gateway, SagaLog log, HoldProperties hold) {
    io.micrometer.core.instrument.Gauge.builder("turnstile.saga.incomplete", log, l -> l.incomplete().size())
        .description("Purchases that have started but not finished")
        .register(metrics);
    return new PurchaseSaga(handler, gateway, log, hold.ttl());
  }

  /**
   * Finishes purchases whose driver died, on a schedule and not only at startup.
   * Without this a crashed server left buyers mid-purchase until the next restart.
   */
  @Bean(destroyMethod = "shutdownNow")
  java.util.concurrent.ScheduledExecutorService sagaReaper(
      PurchaseSaga saga,
      io.micrometer.core.instrument.MeterRegistry metrics,
      @org.springframework.beans.factory.annotation.Value("${turnstile.saga.reaper.interval:PT30S}") java.time.Duration every,
      @org.springframework.beans.factory.annotation.Value("${turnstile.saga.reaper.stale-after:PT1M}") java.time.Duration staleAfter) {
    var reaper =
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "saga-reaper");
              t.setDaemon(true);
              return t;
            });
    reaper.scheduleWithFixedDelay(
        () -> {
          try {
            int recovered = saga.recoverStale(staleAfter).size();
            if (recovered > 0) {
              metrics.counter("turnstile.saga.recovered").increment(recovered);
            }
          } catch (RuntimeException e) {
            org.slf4j.LoggerFactory.getLogger(SagaConfig.class).warn("saga reaper pass failed: {}", e.toString());
          }
        },
        every.toMillis(),
        every.toMillis(),
        java.util.concurrent.TimeUnit.MILLISECONDS);
    return reaper;
  }

  /** On startup, finish purchases the last process was in the middle of. */
  @Bean
  ApplicationRunner recoverSagas(PurchaseSaga saga) {
    return args -> saga.recoverIncomplete();
  }
}
