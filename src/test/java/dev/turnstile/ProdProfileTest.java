package dev.turnstile;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import dev.turnstile.testsupport.TestPostgres;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Boots the application exactly as production would, with the {@code prod}
 * profile, and checks the properties that profile exists to guarantee: the store is
 * Postgres, security is enforced, the demo is off, and what a buyer does really
 * lands in the database.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.profiles.active=prod",
      "turnstile.postgres.username=postgres",
      "turnstile.postgres.password=",
      "spring.security.oauth2.resourceserver.jwt.jwk-set-uri=http://localhost:1/never-fetched",
    })
@Import(ProdProfileTest.Keys.class)
class ProdProfileTest {

  private static final KeyPair KEYS = generate();

  @TestConfiguration
  static class Keys {
    @Bean
    JwtDecoder jwtDecoder() {
      return NimbusJwtDecoder.withPublicKey((RSAPublicKey) KEYS.getPublic()).build();
    }
  }

  @BeforeAll
  static void cleanDatabase() {
    TestPostgres.reset();
  }

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    registry.add("turnstile.postgres.url", TestPostgres::jdbcUrl);
  }

  @Autowired TestRestTemplate http;
  @Autowired ApplicationContext context;

  private static KeyPair generate() {
    try {
      KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
      gen.initialize(2048);
      return gen.generateKeyPair();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String token(String subject, List<String> roles) throws Exception {
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .subject(subject)
            .expirationTime(new Date(System.currentTimeMillis() + 300_000))
            .claim("realm_access", Map.of("roles", roles))
            .build();
    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
    jwt.sign(new RSASSASigner((RSAPrivateKey) KEYS.getPrivate()));
    return jwt.serialize();
  }

  @Test
  @DisplayName("prod: Postgres, enforced security, no demo, and a purchase lands in the database")
  void prod_profile_is_locked_down_and_durable() throws Exception {
    assertThat(context.containsBean("rushService")).as("the rush demo is disabled in prod").isFalse();

    HttpHeaders anon = new HttpHeaders();
    anon.setContentType(MediaType.APPLICATION_JSON);
    assertThat(
            http.exchange("/api/purchases", HttpMethod.POST,
                new HttpEntity<>("{\"seatId\":\"P1\"}", anon), String.class).getStatusCode())
        .as("anonymous purchase in prod")
        .isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(http.getForEntity("/api/export", String.class).getStatusCode())
        .as("anonymous export in prod")
        .isEqualTo(HttpStatus.UNAUTHORIZED);

    HttpHeaders buyer = new HttpHeaders();
    buyer.setContentType(MediaType.APPLICATION_JSON);
    buyer.setBearerAuth(token("prod-buyer", List.of("buyer")));
    assertThat(
            http.exchange("/api/purchases", HttpMethod.POST,
                new HttpEntity<>("{\"seatId\":\"P1\"}", buyer), String.class).getStatusCode())
        .isEqualTo(HttpStatus.CREATED);

    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    assertThat(jdbc.queryForList("SELECT type FROM events WHERE stream_id = 'P1' ORDER BY version", String.class))
        .as("the purchase is in PostgreSQL, not in memory")
        .containsExactly("SeatHeld", "SeatSold");
    assertThat(jdbc.queryForObject("SELECT state FROM sagas LIMIT 1", String.class))
        .as("and the saga state is durable too")
        .isEqualTo("CONFIRMED");
  }
}
