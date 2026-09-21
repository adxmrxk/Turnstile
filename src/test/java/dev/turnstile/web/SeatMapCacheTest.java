package dev.turnstile.web;

import static org.assertj.core.api.Assertions.assertThat;

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

/** The cached seat map is fast, but must never be wrong. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SeatMapCacheTest {

  @Autowired TestRestTemplate http;

  private ResponseEntity<String> read(String ifNoneMatch) {
    HttpHeaders headers = new HttpHeaders();
    if (ifNoneMatch != null) {
      headers.set("If-None-Match", ifNoneMatch);
    }
    return http.exchange("/api/seats", HttpMethod.GET, new HttpEntity<>(headers), String.class);
  }

  private void buy(String seat) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    http.postForEntity("/api/purchases", new HttpEntity<>("{\"seatId\":\"" + seat + "\",\"buyerId\":\"u\"}", headers), String.class);
  }

  @Test
  @DisplayName("an unchanged map is a 304; a change is reflected immediately with a new ETag")
  void etag_and_freshness() {
    String first = "cache-" + UUID.randomUUID().toString().substring(0, 8);
    buy(first);

    ResponseEntity<String> a = read(null);
    String etag = a.getHeaders().getETag();
    assertThat(a.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(etag).isNotBlank();
    assertThat(a.getBody()).contains(first);

    ResponseEntity<String> unchanged = read(etag);
    assertThat(unchanged.getStatusCode()).as("nothing changed").isEqualTo(HttpStatus.NOT_MODIFIED);
    assertThat(unchanged.getBody()).as("a 304 carries no body").isNull();

    String second = "cache-" + UUID.randomUUID().toString().substring(0, 8);
    buy(second);

    ResponseEntity<String> changed = read(etag);
    assertThat(changed.getStatusCode()).as("the old ETag no longer matches").isEqualTo(HttpStatus.OK);
    assertThat(changed.getHeaders().getETag()).isNotEqualTo(etag);
    assertThat(changed.getBody()).as("no lag on a real change").contains(second);
  }
}
