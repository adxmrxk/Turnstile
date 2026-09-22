package dev.turnstile.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.TurnstileApplication;
import dev.turnstile.testsupport.TestPostgres;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Each server process gets its own consumer group, so every restart used to leave a
 * group behind on the broker, holding offsets, until Kafka expired it. A server that
 * shuts down cleanly now deletes its own group.
 */
class ConsumerGroupTest {

  private static List<String> ourGroups(Admin admin) throws Exception {
    return admin.listConsumerGroups().all().get().stream()
        .map(ConsumerGroupListing::groupId)
        .filter(g -> g.startsWith("turnstile-seat-map-"))
        .toList();
  }

  @Test
  @DisplayName("a server that shuts down cleanly deletes its consumer group")
  void group_is_deleted_on_shutdown() throws Exception {
    TestPostgres.reset();
    EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 3, "turnstile.seat-events");
    kafka.afterPropertiesSet();
    Properties props = new Properties();
    props.put("bootstrap.servers", kafka.getBrokersAsString());
    try (Admin admin = Admin.create(props)) {
      ConfigurableApplicationContext node =
          new SpringApplicationBuilder(TurnstileApplication.class)
              .properties(
                  Map.of(
                      "server.port", "0",
                      "turnstile.store", "postgres",
                      "turnstile.kafka.enabled", "true",
                      "turnstile.demo.enabled", "false",
                      "turnstile.postgres.url", TestPostgres.jdbcUrl(),
                      "turnstile.postgres.username", "postgres",
                      "turnstile.postgres.password", "",
                      "spring.kafka.bootstrap-servers", kafka.getBrokersAsString()))
              .run();
      try {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ourGroups(admin).isEmpty()) {
          Thread.sleep(200);
        }
        assertThat(ourGroups(admin)).as("while running, the server's group exists").hasSize(1);
      } finally {
        node.close();
      }

      long deadline = System.currentTimeMillis() + 15_000;
      while (System.currentTimeMillis() < deadline && !ourGroups(admin).isEmpty()) {
        Thread.sleep(300);
      }
      assertThat(ourGroups(admin)).as("after a clean shutdown the group is gone").isEmpty();
    } finally {
      kafka.destroy();
    }
  }
}
