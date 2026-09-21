package dev.turnstile;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Binds {@code turnstile.hold.*} from application.yaml. */
@ConfigurationProperties("turnstile.hold")
public record HoldProperties(Duration ttl) {

  public HoldProperties {
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("turnstile.hold.ttl must be a positive duration");
    }
  }
}
