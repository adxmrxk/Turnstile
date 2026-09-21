package dev.turnstile;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Binds {@code turnstile.pricing.*}. */
@ConfigurationProperties("turnstile.pricing")
public record PricingProperties(@DefaultValue("5000") long seatPriceCents) {}
