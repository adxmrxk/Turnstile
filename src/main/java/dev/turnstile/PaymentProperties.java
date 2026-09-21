package dev.turnstile;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Binds {@code turnstile.payment.*}: how badly the simulated provider behaves. */
@ConfigurationProperties("turnstile.payment")
public record PaymentProperties(
    @DefaultValue("0") double declineRate,
    @DefaultValue("0") double dropRate,
    @DefaultValue("0") double lostResponseRate,
    @DefaultValue("0") long maxLatencyMillis,
    @DefaultValue("1") long seed) {}
