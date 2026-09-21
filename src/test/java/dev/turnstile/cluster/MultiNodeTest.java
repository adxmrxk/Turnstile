package dev.turnstile.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.TurnstileApplication;
import dev.turnstile.audit.LogAuditor;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import dev.turnstile.testsupport.TestPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Two application instances, one database, one Kafka.
 *
 * <p>Every other test runs the system inside a single application. But the
 * guarantee is that PostgreSQL arbitrates between <em>servers</em>, and a second
 * server has its own connection pool, its own handler, its own in-memory state and
 * its own read model, and shares nothing with the first except the database and
 * the broker. This starts two such instances, sends buyers to both, and asks the
 * questions that only a second node can get wrong.
 */
class MultiNodeTest {

  private static final String TOPIC = "turnstile.seat-events";
  private static EmbeddedKafkaKraftBroker kafka;
  private static ConfigurableApplicationContext nodeA;
  private static ConfigurableApplicationContext nodeB;

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @BeforeAll
  static void startCluster() {
    TestPostgres.reset();
    kafka = new EmbeddedKafkaKraftBroker(1, 3, TOPIC);
    kafka.afterPropertiesSet();
    nodeA = startNode();
    nodeB = startNode();
  }

  @AfterAll
  static void stopCluster() {
    if (nodeB != null) {
      nodeB.close();
    }
    if (nodeA != null) {
      nodeA.close();
    }
    if (kafka != null) {
      kafka.destroy();
    }
  }

  private static ConfigurableApplicationContext startNode() {
    return new SpringApplicationBuilder(TurnstileApplication.class)
        .properties(
            Map.of(
                "server.port", "0",
                "turnstile.store", "postgres",
                "turnstile.kafka.enabled", "true",
                "turnstile.demo.enabled", "false",
                "turnstile.postgres.url", TestPostgres.jdbcUrl(),
                "turnstile.postgres.username", "postgres",
                "turnstile.postgres.password", "",
                "spring.kafka.bootstrap-servers", kafka.getBrokersAsString(),
                "spring.kafka.consumer.auto-offset-reset", "earliest"))
        .run();
  }

  private static String base(ConfigurableApplicationContext node) {
    return "http://localhost:" + node.getEnvironment().getProperty("local.server.port");
  }

  private static HttpResponse<String> post(ConfigurableApplicationContext node, String seat, String buyer, String key)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(base(node) + "/api/purchases"))
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", key)
            .POST(HttpRequest.BodyPublishers.ofString("{\"seatId\":\"" + seat + "\",\"buyerId\":\"" + buyer + "\"}"))
            .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static String get(ConfigurableApplicationContext node, String path) throws Exception {
    return HTTP.send(HttpRequest.newBuilder(URI.create(base(node) + path)).build(), HttpResponse.BodyHandlers.ofString())
        .body();
  }

  @Test
  @DisplayName("buyers split across two servers never oversell, and both servers' seat maps match the log")
  void two_nodes_one_database() throws Exception {
    int buyers = 400;
    int seats = 40;
    ConfigurableApplicationContext[] nodes = {nodeA, nodeB};

    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(24);
    AtomicInteger created = new AtomicInteger();
    AtomicInteger unexpected = new AtomicInteger();
    try {
      List<Future<?>> work = new ArrayList<>();
      for (int i = 0; i < buyers; i++) {
        int n = i;
        // Neighbouring buyers share a seat and alternate servers, so the same seat
        // is contended by both nodes at the same moment.
        String seat = "cluster-seat-" + (n * seats / buyers);
        work.add(
            pool.submit(
                () -> {
                  go.await();
                  HttpResponse<String> r = post(nodes[n % 2], seat, "buyer-" + n, "key-" + n);
                  if (r.statusCode() == 201) {
                    created.incrementAndGet();
                  } else if (r.statusCode() != 409 && r.statusCode() != 402) {
                    unexpected.incrementAndGet();
                  }
                  return null;
                }));
      }
      go.countDown();
      for (Future<?> f : work) {
        f.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    assertThat(unexpected.get()).as("responses other than bought / taken / payment failed").isZero();

    // 1. The database, not either server, decided who got each seat.
    List<String> lines = Arrays.asList(get(nodeA, "/api/export").split("\n"));
    LogAuditor.Report audit = new LogAuditor().audit(lines);
    assertThat(audit.clean()).as("audit of the shared log: %s", audit.violations()).isTrue();
    assertThat(audit.seatsSold()).as("sales in the log == purchases the servers reported").isEqualTo(created.get());
    assertThat(audit.seatsSold()).as("a seat is sold at most once across both servers").isLessThanOrEqualTo(seats);

    // 2. Each server's read model, fed from Kafka, must match the log. A node that
    // only ever saw part of the stream would show seats in the wrong state.
    SeatMapProjection truth = new SeatMapProjection(nodeA.getBean(dev.turnstile.eventstore.NotifyingEventStore.class), Clock.systemUTC());
    truth.rebuild();
    List<SeatView> expected = truth.all();
    for (ConfigurableApplicationContext node : nodes) {
      SeatMapProjection map = node.getBean(SeatMapProjection.class);
      long deadline = System.currentTimeMillis() + 45_000;
      while (System.currentTimeMillis() < deadline && !map.all().equals(expected)) {
        Thread.sleep(250);
      }
      assertThat(map.all()).as("seat map on %s", base(node)).isEqualTo(expected);
    }
  }

  @Test
  @DisplayName("a retry that lands on the other server gets the original purchase, not a second one")
  void idempotency_holds_across_servers() throws Exception {
    String seat = "cluster-idem-seat";

    HttpResponse<String> first = post(nodeA, seat, "carol", "carols-key");
    HttpResponse<String> retryElsewhere = post(nodeB, seat, "carol", "carols-key");

    assertThat(first.statusCode()).isEqualTo(201);
    assertThat(retryElsewhere.statusCode()).as("the retry on the other server").isEqualTo(201);
    assertThat(retryElsewhere.body()).isEqualTo(first.body());

    long sales =
        Arrays.stream(get(nodeB, "/api/export").split("\n"))
            .filter(l -> l.contains("\"" + seat + "\"") && l.contains("SeatSold"))
            .count();
    assertThat(sales).as("sales of that seat in the log").isEqualTo(1);
  }
}
