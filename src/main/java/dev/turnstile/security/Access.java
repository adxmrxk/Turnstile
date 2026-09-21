package dev.turnstile.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Role checks for places the URL rules cannot reach, such as individual GraphQL
 * fields behind a single {@code /graphql} endpoint. When security is off there is
 * nobody to check, matching the rest of the application.
 */
@Component
public class Access {

  private final boolean enforced;

  public Access(@Value("${turnstile.security.mode:off}") String mode) {
    this.enforced = "jwt".equals(mode);
  }

  public void requireStaff() {
    if (!enforced) {
      return;
    }
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    boolean staff =
        auth != null
            && auth.isAuthenticated()
            && auth.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals("ROLE_staff"));
    if (!staff) {
      throw new AccessDeniedException("staff role required");
    }
  }
}
