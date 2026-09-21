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
      SeatCommandHandler handler, PaymentGateway gateway, SagaLog log, HoldProperties hold) {
    return new PurchaseSaga(handler, gateway, log, hold.ttl());
  }

  /** On startup, finish purchases the last process was in the middle of. */
  @Bean
  ApplicationRunner recoverSagas(PurchaseSaga saga) {
    return args -> saga.recoverIncomplete();
  }
}
