package dev.turnstile;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.turnstile.eventstore.ChainVerifier;
import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.PostgresEventStore;
import dev.turnstile.saga.JdbcSagaLog;
import dev.turnstile.saga.SagaLog;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Everything that exists only when {@code turnstile.store=postgres}. */
@Configuration
@ConditionalOnProperty(name = "turnstile.store", havingValue = "postgres")
@EnableConfigurationProperties(PostgresProperties.class)
public class PostgresConfig {

  @Bean(destroyMethod = "close")
  DataSource dataSource(PostgresProperties props) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(props.url());
    config.setUsername(props.username());
    config.setPassword(props.password());
    config.setMaximumPoolSize(props.poolSize());
    return new HikariDataSource(config);
  }

  /** Migrates before anything can use the store. */
  @Bean
  Flyway flyway(DataSource dataSource) {
    Flyway flyway = Flyway.configure().dataSource(dataSource).load();
    flyway.migrate();
    return flyway;
  }

  @Bean
  SagaLog jdbcSagaLog(DataSource dataSource, Flyway migrated) {
    return new JdbcSagaLog(dataSource);
  }

  @Bean
  ChainVerifier chainVerifier(DataSource dataSource, Flyway migrated) {
    return new ChainVerifier(dataSource);
  }

  @Bean(name = "rawEventStore")
  EventStore postgresEventStore(DataSource dataSource, Flyway migrated, EventCodec codec) {
    return new PostgresEventStore(dataSource, codec);
  }
}
