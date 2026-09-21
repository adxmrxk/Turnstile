package dev.turnstile;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainException;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Loads the real application context, so it fails if the pieces stop being
 * assembled or if application.yaml stops being read. Every other test builds its
 * objects by hand and would never notice.
 */
@SpringBootTest
class ApplicationWiringTest {

  @Autowired SeatCommandHandler handler;
  @Autowired HoldProperties holdProperties;

  @Test
  @DisplayName("the hold TTL comes from application.yaml")
  void ttl_is_bound_from_configuration() {
    assertThat(holdProperties.ttl()).isEqualTo(Duration.ofMinutes(2));
  }

  @Test
  @DisplayName("the wired handler is functional end to end")
  void wired_handler_holds_and_refuses_a_second_buyer() {
    handler.hold("wiring-seat", "h1", "b1", holdProperties.ttl(), "wiring-k1");

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> handler.hold("wiring-seat", "h2", "b2", holdProperties.ttl(), "wiring-k2"))
        .isInstanceOf(DomainException.SeatAlreadyHeld.class);
  }
}
