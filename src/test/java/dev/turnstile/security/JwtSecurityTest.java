package dev.turnstile.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Role-based access with real bearer tokens: signed with an RSA key, checked by
 * the same Nimbus decoder a production issuer's tokens would go through. The
 * tokens carry roles where Keycloak puts them, in {@code realm_access.roles}.
 *
 * <p>This proves the application enforces roles correctly given a valid token. It
 * does not prove anything about Keycloak itself: no Keycloak runs here.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "turnstile.security.mode=jwt")
@Import(JwtSecurityTest.Keys.class)
class JwtSecurityTest {

  private static final KeyPair TRUSTED = generate();
  private static final KeyPair ATTACKER = generate();

  @TestConfiguration
  static class Keys {
    @Bean
    JwtDecoder jwtDecoder() {
      return NimbusJwtDecoder.withPublicKey((RSAPublicKey) TRUSTED.getPublic()).build();
    }
  }

  @Autowired TestRestTemplate http;
  private final ObjectMapper json = new ObjectMapper();

  private static KeyPair generate() {
    try {
      KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
      gen.initialize(2048);
      return gen.generateKeyPair();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String token(KeyPair signer, String subject, List<String> roles, Instant expires)
      throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject(subject)
            .issueTime(new Date())
            .expirationTime(Date.from(expires))
            .claim("realm_access", Map.of("roles", roles))
            .build();
    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
    jwt.sign(new RSASSASigner((RSAPrivateKey) signer.getPrivate()));
    return jwt.serialize();
  }

  private static String buyerToken(String subject) throws Exception {
    return token(TRUSTED, subject, List.of("buyer"), Instant.now().plusSeconds(300));
  }

  private static String staffToken() throws Exception {
    return token(TRUSTED, "venue-staff", List.of("staff"), Instant.now().plusSeconds(300));
  }

  private ResponseEntity<String> call(HttpMethod method, String path, String bearer, String body) {
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    if (bearer != null) {
      headers.setBearerAuth(bearer);
    }
    return http.exchange(path, method, new HttpEntity<>(body, headers), String.class);
  }

  private static String seat() {
    return "sec-" + UUID.randomUUID().toString().substring(0, 8);
  }

  @Test
  @DisplayName("the seat map is public, so a buyer can choose a seat before signing in")
  void seat_map_is_public() {
    assertThat(call(HttpMethod.GET, "/api/seats", null, null).getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(call(HttpMethod.GET, "/actuator/health", null, null).getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  @DisplayName("buying needs a token")
  void purchase_requires_authentication() {
    ResponseEntity<String> anonymous =
        call(HttpMethod.POST, "/api/purchases", null, "{\"seatId\":\"" + seat() + "\"}");

    assertThat(anonymous.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  @DisplayName("a buyer is who the token says, whatever the request body claims")
  void the_token_decides_the_buyer() throws Exception {
    String seat = seat();

    ResponseEntity<String> bought =
        call(HttpMethod.POST, "/api/purchases", buyerToken("real-buyer"),
            "{\"seatId\":\"" + seat + "\",\"buyerId\":\"someone-else\"}");
    assertThat(bought.getStatusCode()).isEqualTo(HttpStatus.CREATED);

    ResponseEntity<String> history = call(HttpMethod.GET, "/api/seats/" + seat + "/history", staffToken(), null);
    assertThat(history.getBody()).contains("real-buyer").doesNotContain("someone-else");
  }

  @Test
  @DisplayName("a buyer cannot read the raw log, the audit, or a seat's history; staff can")
  void staff_only_endpoints() throws Exception {
    String buyer = buyerToken("buyer-1");
    String staff = staffToken();

    for (String path : List.of("/api/export", "/api/audit", "/api/seats/x/history")) {
      assertThat(call(HttpMethod.GET, path, buyer, null).getStatusCode()).as("buyer on %s", path).isEqualTo(HttpStatus.FORBIDDEN);
      assertThat(call(HttpMethod.GET, path, null, null).getStatusCode()).as("anonymous on %s", path).isEqualTo(HttpStatus.UNAUTHORIZED);
      assertThat(call(HttpMethod.GET, path, staff, null).getStatusCode()).as("staff on %s", path).isEqualTo(HttpStatus.OK);
    }
  }

  @Test
  @DisplayName("a token signed by anyone else's key is rejected")
  void forged_signature_is_rejected() throws Exception {
    String forged = token(ATTACKER, "buyer-1", List.of("staff"), Instant.now().plusSeconds(300));

    assertThat(call(HttpMethod.GET, "/api/export", forged, null).getStatusCode())
        .isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  @DisplayName("an expired token is rejected")
  void expired_token_is_rejected() throws Exception {
    String expired = token(TRUSTED, "buyer-1", List.of("buyer"), Instant.now().minusSeconds(3600));

    assertThat(call(HttpMethod.POST, "/api/purchases", expired, "{\"seatId\":\"" + seat() + "\"}").getStatusCode())
        .isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  @DisplayName("a token whose payload was edited after signing is rejected")
  void tampered_payload_is_rejected() throws Exception {
    String[] parts = buyerToken("buyer-1").split("\\.");
    String staffClaims =
        java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
            "{\"sub\":\"buyer-1\",\"exp\":9999999999,\"realm_access\":{\"roles\":[\"staff\"]}}".getBytes());
    String tampered = parts[0] + "." + staffClaims + "." + parts[2];

    assertThat(call(HttpMethod.GET, "/api/export", tampered, null).getStatusCode())
        .isEqualTo(HttpStatus.UNAUTHORIZED);
  }

  @Test
  @DisplayName("a valid token with no roles can do nothing that needs one")
  void no_roles_no_access() throws Exception {
    String roleless = token(TRUSTED, "nobody", List.of(), Instant.now().plusSeconds(300));

    assertThat(call(HttpMethod.POST, "/api/purchases", roleless, "{\"seatId\":\"" + seat() + "\"}").getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  @DisplayName("one buyer cannot look up another buyer's purchase by guessing its key")
  void purchases_are_private() throws Exception {
    String key = "key-" + UUID.randomUUID().toString().substring(0, 8);
    HttpHeaders alice = new HttpHeaders();
    alice.setBearerAuth(buyerToken("alice"));
    alice.setContentType(MediaType.APPLICATION_JSON);
    alice.set("Idempotency-Key", key);
    http.exchange("/api/purchases", HttpMethod.POST,
        new HttpEntity<>("{\"seatId\":\"" + seat() + "\"}", alice), String.class);

    assertThat(call(HttpMethod.GET, "/api/purchases/" + key, buyerToken("alice"), null).getStatusCode())
        .isEqualTo(HttpStatus.CREATED);
    assertThat(call(HttpMethod.GET, "/api/purchases/" + key, buyerToken("mallory"), null).getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);
  }

  @Test
  @DisplayName("GraphQL: seats are public, history and audit need staff")
  void graphql_field_level_access() throws Exception {
    String seat = seat();
    call(HttpMethod.POST, "/api/purchases", buyerToken("gq-buyer"), "{\"seatId\":\"" + seat + "\"}");

    String publicQuery = "{\"query\":\"{ seat(id: \\\"" + seat + "\\\") { id status } }\"}";
    String historyQuery = "{\"query\":\"{ seat(id: \\\"" + seat + "\\\") { history { type } } }\"}";
    String auditQuery = "{\"query\":\"{ audit { clean } }\"}";

    JsonNode open = json.readTree(call(HttpMethod.POST, "/graphql", null, publicQuery).getBody());
    assertThat(open.at("/data/seat/status").asText()).isEqualTo("SOLD");

    JsonNode buyerHistory = json.readTree(call(HttpMethod.POST, "/graphql", buyerToken("gq-buyer"), historyQuery).getBody());
    assertThat(buyerHistory.has("errors")).as("a buyer asking for history: %s", buyerHistory).isTrue();

    JsonNode staffHistory = json.readTree(call(HttpMethod.POST, "/graphql", staffToken(), historyQuery).getBody());
    assertThat(staffHistory.has("errors")).as(staffHistory.toString()).isFalse();
    assertThat(staffHistory.at("/data/seat/history/0/type").asText()).isEqualTo("SeatHeld");

    JsonNode buyerAudit = json.readTree(call(HttpMethod.POST, "/graphql", buyerToken("gq-buyer"), auditQuery).getBody());
    assertThat(buyerAudit.has("errors")).isTrue();
    JsonNode staffAudit = json.readTree(call(HttpMethod.POST, "/graphql", staffToken(), auditQuery).getBody());
    assertThat(staffAudit.at("/data/audit/clean").asBoolean()).isTrue();
  }
}
