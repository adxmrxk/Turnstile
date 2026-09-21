package dev.turnstile.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Buyer and venue-staff roles, from an OIDC provider's access tokens.
 *
 * <p>Two modes, chosen by {@code turnstile.security.mode}:
 *
 * <ul>
 *   <li>{@code off} (the default): everything is open. That is right for a laptop
 *       and wrong anywhere else, so it says so at startup, every time.
 *   <li>{@code jwt}: bearer tokens are validated (signature, expiry, issuer when
 *       configured) and roles are read from the {@code realm_access.roles} claim,
 *       which is where Keycloak puts them.
 * </ul>
 *
 * <p>The seat map is public because it is what a buyer needs to choose a seat.
 * Buying needs a buyer or staff token. The raw log, the audit and per-seat
 * history need staff, since they expose every buyer's activity.
 */
@Configuration
public class SecurityConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityConfig.class);

  @Bean
  @ConditionalOnProperty(name = "turnstile.security.mode", havingValue = "off", matchIfMissing = true)
  SecurityFilterChain open(HttpSecurity http) throws Exception {
    LOG.warn(
        "SECURITY IS OFF: every endpoint, including the raw event log, is open to anyone who can"
            + " reach this port. Set turnstile.security.mode=jwt before exposing this.");
    return http.csrf(AbstractHttpConfigurer::disable)
        .authorizeHttpRequests(a -> a.anyRequest().permitAll())
        .build();
  }

  @Bean
  @ConditionalOnProperty(name = "turnstile.security.mode", havingValue = "jwt")
  SecurityFilterChain protectedApi(HttpSecurity http) throws Exception {
    return http.csrf(AbstractHttpConfigurer::disable)
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(
            a ->
                a.requestMatchers("/actuator/health/**", "/actuator/health", "/", "/index.html", "/favicon.ico", "/ws", "/ws/**")
                    .permitAll()
                    .requestMatchers(HttpMethod.GET, "/api/seats", "/api/seats/*", "/api/stats")
                    .permitAll()
                    .requestMatchers("/api/purchases", "/api/purchases/**")
                    .hasAnyRole("buyer", "staff")
                    .requestMatchers("/api/export", "/api/audit", "/api/log/**", "/api/seats/*/history", "/api/demo/**")
                    .hasRole("staff")
                    .requestMatchers("/graphql", "/graphiql")
                    .permitAll() // per-field checks live in the resolvers
                    .anyRequest()
                    .denyAll())
        .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(rolesFromToken())))
        .build();
  }

  /** Keycloak: {"realm_access": {"roles": ["buyer"]}} becomes ROLE_buyer. */
  static Converter<Jwt, AbstractAuthenticationToken> rolesFromToken() {
    JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(SecurityConfig::authoritiesOf);
    return converter;
  }

  static Collection<GrantedAuthority> authoritiesOf(Jwt jwt) {
    Object realm = jwt.getClaim("realm_access");
    if (!(realm instanceof Map<?, ?> map) || !(map.get("roles") instanceof List<?> roles)) {
      return List.of();
    }
    return roles.stream()
        .map(Object::toString)
        .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
        .toList();
  }

}
