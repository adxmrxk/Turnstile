package dev.turnstile;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;

/**
 * The JDBC and Flyway auto-configuration is switched off on purpose. The store is
 * chosen by {@code turnstile.store}: with the default {@code memory} there is no
 * database to connect to, and Boot's auto-configuration would refuse to start
 * without one. {@link PostgresConfig} builds the pool and runs the migrations
 * itself, and only when Postgres is asked for.
 */
@SpringBootApplication(
    exclude = {
      DataSourceAutoConfiguration.class,
      DataSourceTransactionManagerAutoConfiguration.class,
      JdbcTemplateAutoConfiguration.class,
      FlywayAutoConfiguration.class,
      UserDetailsServiceAutoConfiguration.class
    })
public class TurnstileApplication {

  public static void main(String[] args) {
    SpringApplication.run(TurnstileApplication.class, args);
  }
}
