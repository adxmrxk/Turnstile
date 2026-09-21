package dev.turnstile.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turnstile.testsupport.TestPostgres;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The operator workflow, over HTTP, on a real database that then gets tampered with. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"turnstile.store=postgres", "turnstile.postgres.username=postgres", "turnstile.postgres.password="})
class LogIntegrityApiTest {

  @BeforeAll
  static void clean() {
    TestPostgres.reset();
  }

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    registry.add("turnstile.postgres.url", TestPostgres::jdbcUrl);
  }

  @Autowired TestRestTemplate http;
  private final ObjectMapper json = new ObjectMapper();

  @Test
  @DisplayName("checkpoint, then tamper, then verify: the edit is reported over the API")
  void checkpoint_verify_and_tamper() throws Exception {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    for (int i = 0; i < 4; i++) {
      http.postForEntity(
          "/api/purchases",
          new HttpEntity<>("{\"seatId\":\"api-seat-" + i + "\",\"buyerId\":\"buyer-" + i + "\"}", headers),
          String.class);
    }

    String checkpoint = http.getForObject("/api/log/checkpoint", String.class);
    JsonNode cp = json.readTree(checkpoint);
    assertThat(cp.get("heads").size()).isEqualTo(4);

    JsonNode clean = json.readTree(http.postForObject("/api/log/verify", new HttpEntity<>(checkpoint, headers), String.class));
    assertThat(clean.get("intact").asBoolean()).as("before tampering: %s", clean).isTrue();

    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    jdbc.execute("ALTER TABLE events DISABLE TRIGGER events_are_append_only");
    try {
      jdbc.update("DELETE FROM events WHERE stream_id = 'api-seat-2' AND version = 2");
    } finally {
      jdbc.execute("ALTER TABLE events ENABLE TRIGGER events_are_append_only");
    }

    JsonNode dirty = json.readTree(http.postForObject("/api/log/verify", new HttpEntity<>(checkpoint, headers), String.class));
    assertThat(dirty.get("intact").asBoolean()).as("after deleting a sale: %s", dirty).isFalse();
    assertThat(dirty.toString()).contains("api-seat-2");
  }
}
