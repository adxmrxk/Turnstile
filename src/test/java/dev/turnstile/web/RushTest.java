package dev.turnstile.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/** The live rush: a crowd, real sagas, failing payments, and an audit at the end. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RushTest {

  @Autowired TestRestTemplate http;
  private final ObjectMapper json = new ObjectMapper();

  @Test
  @DisplayName("20,000 buyers rush 400 seats: every seat sold at most once, money balances, audit clean")
  void a_rush_is_audited_clean() throws Exception {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    ResponseEntity<String> started =
        http.postForEntity("/api/demo/rush", new HttpEntity<>("{\"seats\":400,\"buyers\":20000}", headers), String.class);
    assertThat(started.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    String runId = json.readTree(started.getBody()).at("/progress/runId").asText();

    ResponseEntity<String> second =
        http.postForEntity("/api/demo/rush", new HttpEntity<>("{\"seats\":5,\"buyers\":5}", headers), String.class);
    assertThat(second.getStatusCode()).as("only one rush at a time").isEqualTo(HttpStatus.CONFLICT);

    JsonNode result = null;
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      result = json.readTree(http.getForObject("/api/demo/rush/" + runId, String.class));
      if (result.at("/progress/finished").asBoolean()) {
        break;
      }
      Thread.sleep(200);
    }
    assertThat(result.at("/progress/finished").asBoolean()).as("the rush finished: %s", result).isTrue();

    int sold = result.at("/progress/sold").asInt();
    int total = result.at("/progress/refused").asInt() + result.at("/progress/declined").asInt()
        + result.at("/progress/refunded").asInt() + sold;
    assertThat(total).as("every buyer ended somewhere").isEqualTo(20000);
    assertThat(sold).as("never more sales than seats").isLessThanOrEqualTo(400);
    assertThat(result.at("/progress/declined").asInt() + result.at("/progress/refunded").asInt())
        .as("the simulated provider really failed some payments")
        .isGreaterThan(0);
    assertThat(result.at("/audit/clean").asBoolean()).as("audit: %s", result.at("/audit")).isTrue();
    assertThat(result.at("/audit/seatsSold").asInt()).as("audit agrees with the buyers' count").isEqualTo(sold);
    assertThat(result.at("/money/balanced").asBoolean()).as("money: %s", result.at("/money")).isTrue();
  }
}
