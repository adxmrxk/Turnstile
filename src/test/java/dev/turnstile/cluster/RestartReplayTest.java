package dev.turnstile.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.TurnstileApplication;
import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import dev.turnstile.testsupport.TestPostgres;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * A restarted server must catch up without re-reading the whole topic.
 *
 * <p>Before, every restart began a fresh consumer group at the start of the topic
 * and re-fed the projection every event ever published, all of them duplicates,
 * for longer each time. Now the projection is rebuilt from the log and the consumer
 * only picks up from about when that rebuild began.
 */
class RestartReplayTest {

  private static final HttpClient HTTP = HttpClient.newHttpClient();
  private static EmbeddedKafkaKraftBroker kafka;
  private static ConfigurableApplicationContext writer;

  @BeforeAll
  static void start() {
    TestPostgres.reset();
    kafka = new EmbeddedKafkaKraftBroker(1, 3, "turnstile.seat-events");
    kafka.afterPropertiesSet();
    writer = node();
  }

  @AfterAll
  static void stop() {
    writer.close();
    kafka.destroy();
  }

  private static ConfigurableApplicationContext node() {
    return new SpringApplicationBuilder(TurnstileApplication.class)
        .properties(
            Map.of(
                "server.port", "0",
                "turnstile.store", "postgres",
                "turnstile.kafka.enabled", "true",
                "turnstile.kafka.replay-margin", "PT1S",
                "turnstile.demo.enabled", "false",
                "turnstile.postgres.url", TestPostgres.jdbcUrl(),
                "turnstile.postgres.username", "postgres",
                "turnstile.postgres.password", "",
                // The harsh setting: it made a replay happen in the benchmark, so the
                // test must run with it or it proves nothing about that case.
                "spring.kafka.consumer.auto-offset-reset", "earliest",
                "spring.kafka.bootstrap-servers", kafka.getBrokersAsString()))
        .run();
  }

  private static void buy(ConfigurableApplicationContext node, String prefix, int from, int to) throws Exception {
    String base = "http://localhost:" + node.getEnvironment().getProperty("local.server.port");
    for (int i = from; i < to; i++) {
      HTTP.send(
          HttpRequest.newBuilder(URI.create(base + "/api/purchases"))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString("{\"seatId\":\"" + prefix + i + "\",\"buyerId\":\"u" + i + "\"}"))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }
  }

  private static long consumed(ConfigurableApplicationContext node) {
    MeterRegistry m = node.getBean(MeterRegistry.class);
    long total = 0;
    for (String result : List.of("applied", "duplicate", "refold")) {
      var c = m.find("turnstile.projection.events").tag("result", result).counter();
      total += c == null ? 0 : (long) c.count();
    }
    return total;
  }

  private static void awaitMatchesLog(ConfigurableApplicationContext node) throws Exception {
    SeatMapProjection truth = new SeatMapProjection(node.getBean(NotifyingEventStore.class), Clock.systemUTC());
    truth.rebuild();
    List<SeatView> expected = truth.all();
    SeatMapProjection map = node.getBean(SeatMapProjection.class);
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline && !map.all().equals(expected)) {
      Thread.sleep(200);
    }
    assertThat(map.all()).isEqualTo(expected);
  }

  @Test
  @DisplayName("a restarted server rebuilds from the log and does not replay the topic")
  void restart_does_not_replay_history() throws Exception {
    ConfigurableApplicationContext reader = node();
    buy(writer, "rr-", 0, 200);
    awaitMatchesLog(reader);
    long published = new JdbcTemplate(TestPostgres.dataSource()).queryForObject("SELECT count(*) FROM events", Long.class);
    assertThat(published).isGreaterThanOrEqualTo(400);

    reader.close();
    buy(writer, "rr-", 200, 250); // written while the reader is down
    Thread.sleep(3_000); // comfortably older than the replay margin

    ConfigurableApplicationContext restarted = node();
    Thread.sleep(4_000); // give a consumer time to (wrongly) replay
    long replayed = consumed(restarted);

    assertThat(replayed)
        .as("messages the restarted server re-read from Kafka (a full replay would be %d)", published + 100)
        .isLessThan(25);
    awaitMatchesLog(restarted); // and it is still correct, including events written while it was down

    // The live path still works after the seek.
    buy(writer, "rr-", 250, 280);
    awaitMatchesLog(restarted);
    assertThat(consumed(restarted)).as("live events are consumed").isGreaterThan(replayed);
    restarted.close();
  }
}
