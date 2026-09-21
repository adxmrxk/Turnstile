package dev.turnstile.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turnstile.audit.LogAuditor;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * The real server on a real port, driven over HTTP. The application used to start
 * and exit; this is the test that it now stays up and does its job end to end.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpApiTest {

  @Autowired TestRestTemplate http;

  private final ObjectMapper json = new ObjectMapper();

  private ResponseEntity<String> buy(String seat, String buyer, String key) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    if (key != null) {
      headers.set("Idempotency-Key", key);
    }
    String body = "{\"seatId\":\"" + seat + "\",\"buyerId\":\"" + buyer + "\"}";
    return http.exchange("/api/purchases", HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
  }

  private static String unique(String prefix) {
    return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  @DisplayName("a purchase sells the seat, and a second buyer is refused with 409")
  void purchase_and_conflict() throws Exception {
    String seat = unique("seat");

    ResponseEntity<String> first = buy(seat, "alice", unique("k"));
    ResponseEntity<String> second = buy(seat, "bob", unique("k"));

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(json.readTree(first.getBody()).get("state").asText()).isEqualTo("CONFIRMED");
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    assertThat(json.readTree(second.getBody()).get("state").asText()).isEqualTo("REFUSED");
  }

  @Test
  @DisplayName("retrying with the same Idempotency-Key returns the same purchase and sells once")
  void idempotent_retry_over_http() throws Exception {
    String seat = unique("seat");
    String key = unique("k");

    ResponseEntity<String> original = buy(seat, "alice", key);
    ResponseEntity<String> retry = buy(seat, "alice", key);

    assertThat(retry.getStatusCode()).as("the retry is answered as the original was").isEqualTo(original.getStatusCode());
    assertThat(json.readTree(retry.getBody())).isEqualTo(json.readTree(original.getBody()));

    ResponseEntity<String> history = http.getForEntity("/api/seats/" + seat + "/history", String.class);
    long sales = 0;
    for (JsonNode e : json.readTree(history.getBody())) {
      if (e.get("type").asText().equals("SeatSold")) {
        sales++;
      }
    }
    assertThat(sales).as("exactly one sale in the log").isEqualTo(1);
  }

  @Test
  @DisplayName("the same key from a different buyer is a different purchase, not a leak")
  void keys_are_scoped_to_the_buyer() throws Exception {
    String key = unique("shared");

    ResponseEntity<String> alice = buy(unique("seat"), "alice", key);
    ResponseEntity<String> mallory = buy(unique("seat"), "mallory", key);

    assertThat(alice.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(mallory.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(json.readTree(mallory.getBody()).get("seatId"))
        .as("mallory got her own purchase, not alice's")
        .isNotEqualTo(json.readTree(alice.getBody()).get("seatId"));
  }

  @Test
  @DisplayName("time travel: a seat read as of before it was sold is available")
  void as_of_reads_the_past() throws Exception {
    String seat = unique("seat");
    Instant before = Instant.now().minusSeconds(60);
    buy(seat, "alice", unique("k"));

    JsonNode now = json.readTree(http.getForObject("/api/seats/" + seat, String.class));
    JsonNode then = json.readTree(http.getForObject("/api/seats/" + seat + "?asOf=" + before, String.class));

    assertThat(now.get("status").asText()).isEqualTo("SOLD");
    assertThat(then.get("status").asText()).isEqualTo("AVAILABLE");
    assertThat(then.get("eventsApplied").asInt()).isZero();
  }

  @Test
  void malformed_input_is_a_400_not_a_500() {
    assertThat(buy("bad seat id!", "alice", null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(buy("ok-seat", "bad buyer;drop", null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(http.getForEntity("/api/seats/x?asOf=yesterday", String.class).getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  @DisplayName("the exported log, fed to the independent auditor, is clean")
  void export_and_audit_agree_the_log_is_clean() throws Exception {
    for (int i = 0; i < 5; i++) {
      buy(unique("seat"), "buyer" + i, unique("k"));
    }

    ResponseEntity<String> export = http.getForEntity("/api/export", String.class);
    assertThat(export.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(export.getHeaders().getContentType().toString()).contains("ndjson");

    List<String> lines = Arrays.asList(export.getBody().split("\n"));
    LogAuditor.Report report = new LogAuditor().audit(lines);
    assertThat(report.clean()).as("violations: %s", report.violations()).isTrue();
    assertThat(report.seatsSold()).isGreaterThanOrEqualTo(5);

    Map<?, ?> live = http.getForObject("/api/audit", Map.class);
    assertThat(live.get("clean")).isEqualTo(true);
  }

  @Test
  void the_seat_map_and_stats_reflect_sales() {
    String seat = unique("seat");
    buy(seat, "alice", unique("k"));

    String seats = http.getForObject("/api/seats", String.class);
    Map<?, ?> stats = http.getForObject("/api/stats", Map.class);

    assertThat(seats).contains(seat);
    assertThat(((Number) stats.get("SOLD")).longValue()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void the_server_reports_healthy() {
    ResponseEntity<String> health = http.getForEntity("/actuator/health", String.class);
    assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(health.getBody()).contains("UP");
  }
}
