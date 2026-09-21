package dev.turnstile.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import dev.turnstile.saga.PurchaseSaga;
import dev.turnstile.testsupport.TestPostgres;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The whole write-to-read path, for real: purchases commit to PostgreSQL, the
 * outbox relay publishes them to a Kafka broker, and the seat map is built only
 * from what a consumer reads off the topic. Nothing in the read path is
 * in-process, so if any hop drops or reorders an event the map will not match the
 * log and this fails.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.MOCK,
    properties = {
      "turnstile.store=postgres",
      "turnstile.kafka.enabled=true",
      "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
      "spring.kafka.consumer.auto-offset-reset=earliest",
      "turnstile.payment.decline-rate=0.1",
      "turnstile.payment.drop-rate=0.03",
      "turnstile.payment.lost-response-rate=0.05",
      "turnstile.postgres.username=postgres",
      "turnstile.postgres.password="
    })
@EmbeddedKafka(partitions = 3, topics = "turnstile.seat-events", kraft = true)
// Closes the context afterwards: its outbox relay keeps polling the shared test
// database, and would otherwise steal rows from the other outbox tests.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaPipelineTest {

  @BeforeAll
  static void cleanDatabase() {
    TestPostgres.reset();
  }

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    registry.add("turnstile.postgres.url", TestPostgres::jdbcUrl);
  }

  @Autowired PurchaseSaga saga;
  @Autowired SeatMapProjection seatMap;
  @Autowired NotifyingEventStore store;
  @Autowired OutboxRelay relay;
  @Autowired EmbeddedKafkaBroker broker;

  @Test
  @DisplayName("purchases on Postgres reach the seat map through Kafka, matching the log exactly")
  void write_to_read_through_a_real_broker() throws Exception {
    int buyers = 240;
    int seats = 30;
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(16);
    try {
      List<Future<?>> work = new ArrayList<>();
      for (int i = 0; i < buyers; i++) {
        String id = "kp-" + i;
        String seat = "kseat-" + (i * seats / buyers);
        work.add(
            pool.submit(
                () -> {
                  go.await();
                  return saga.start(id, seat, "buyer-" + id, 5_000);
                }));
      }
      go.countDown();
      for (Future<?> f : work) {
        f.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    long logSize = store.readAll().size();
    assertThat(logSize).as("the purchases wrote events").isGreaterThan(seats);

    // The read model must converge on exactly what the log says.
    SeatMapProjection truth = new SeatMapProjection(store, Clock.systemUTC());
    truth.rebuild();
    List<SeatView> expected = truth.all();
    List<SeatView> actual = List.of();
    long deadline = System.currentTimeMillis() + 60_000;
    while (System.currentTimeMillis() < deadline) {
      actual = seatMap.all();
      if (relay.pending() == 0 && actual.equals(expected)) {
        break;
      }
      Thread.sleep(200);
    }
    assertThat(relay.pending()).as("outbox fully published").isZero();
    assertThat(actual).as("seat map built from Kafka == seat map folded from the log").isEqualTo(expected);
    assertThat(expected).hasSize(seats);

    // And the broker really holds every event: read the topic independently.
    Set<String> keysOnTopic = new HashSet<>();
    var props = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "true", broker);
    props.put("auto.offset.reset", "earliest");
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(List.of("turnstile.seat-events"));
      EventCodec codec = new EventCodec();
      long stop = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < stop && keysOnTopic.size() < logSize) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          var e = codec.fromEnvelope(record.value());
          keysOnTopic.add(e.streamId() + ":" + e.version());
        }
      }
    }
    assertThat(keysOnTopic).as("distinct events on the topic").hasSize((int) logSize);
  }
}
