package dev.turnstile.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

/** The numbers an operator would alert on are actually exported, and move when things happen. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// Spring Boot switches metrics export off in tests unless asked, which hides the endpoint.
@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
class MetricsTest {

  @Autowired TestRestTemplate http;

  private void buy(String seat, String buyer) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    http.postForEntity(
        "/api/purchases",
        new HttpEntity<>("{\"seatId\":\"" + seat + "\",\"buyerId\":\"" + buyer + "\"}", headers),
        String.class);
  }

  @Test
  @DisplayName("purchases, store appends and projection events show up in the Prometheus scrape")
  void the_scrape_reports_what_happened() {
    buy("metrics-seat", "alice");
    buy("metrics-seat", "bob"); // refused: the seat is taken

    String scrape = http.getForObject("/actuator/prometheus", String.class);

    assertThat(scrape)
        .contains("turnstile_purchase_seconds_count{state=\"CONFIRMED\"")
        .contains("turnstile_purchase_seconds_count{state=\"REFUSED\"")
        .contains("turnstile_store_append_seconds_count{outcome=\"ok\"")
        .contains("turnstile_store_load_seconds_count")
        .contains("turnstile_projection_events_total{result=\"applied\"")
        .contains("turnstile_saga_incomplete");
  }

  @Test
  @DisplayName("a lost race is counted as a conflict, separately from a successful append")
  void conflicts_are_counted_separately() {
    var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    var store = new MeteredEventStore(new dev.turnstile.eventstore.InMemoryEventStore(), registry);
    var event =
        new dev.turnstile.domain.DomainEvent.SeatHeld(
            "s", "h", "b", java.time.Instant.now(), java.time.Instant.now().plusSeconds(60));
    store.append("s", 0, java.util.List.of(event), null);

    try {
      store.append("s", 0, java.util.List.of(event), null); // stale: the seat moved
    } catch (dev.turnstile.eventstore.ConcurrencyConflictException expected) {
      // that is the point
    }

    assertThat(registry.get("turnstile.store.append").tag("outcome", "ok").timer().count()).isEqualTo(1);
    assertThat(registry.get("turnstile.store.append").tag("outcome", "conflict").timer().count()).isEqualTo(1);
  }
}
