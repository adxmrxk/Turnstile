package dev.turnstile;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Binds {@code turnstile.postgres.*}. */
@ConfigurationProperties("turnstile.postgres")
public record PostgresProperties(
    String url, String username, String password, @DefaultValue("10") int poolSize) {}
