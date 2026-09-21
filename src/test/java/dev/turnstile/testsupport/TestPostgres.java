package dev.turnstile.testsupport;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * One real PostgreSQL server per test JVM, started in-process from the official
 * binaries. Not H2 in a compatibility mode and not a mock: the unique
 * constraints, {@code ON CONFLICT}, {@code FOR UPDATE SKIP LOCKED} and the
 * append-only trigger under test are the real thing.
 *
 * <p>Flyway migrates the schema once. {@link #reset()} empties the tables between
 * tests; TRUNCATE is used because it, unlike DELETE, is not blocked by the
 * append-only row trigger, which is itself under test.
 */
public final class TestPostgres {

  private static EmbeddedPostgres server;
  private static DataSource dataSource;

  private TestPostgres() {}

  public static synchronized DataSource dataSource() {
    if (dataSource == null) {
      try {
        server = EmbeddedPostgres.builder().start();
      } catch (IOException e) {
        throw new IllegalStateException("could not start embedded PostgreSQL", e);
      }
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      server.close();
                    } catch (IOException ignored) {
                      // best effort at JVM exit
                    }
                  }));
      dataSource = server.getPostgresDatabase();
      Flyway.configure().dataSource(dataSource).load().migrate();
    }
    return dataSource;
  }

  /** For tests that boot the whole application against this database. */
  public static String jdbcUrl() {
    dataSource();
    return server.getJdbcUrl("postgres", "postgres");
  }

  public static void reset() {
    try (Connection c = dataSource().getConnection();
        Statement s = c.createStatement()) {
      s.execute("TRUNCATE events, idempotency_keys, outbox, sagas RESTART IDENTITY");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
