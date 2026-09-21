package dev.turnstile.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainException;
import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.PostgresEventStore;
import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import dev.turnstile.testsupport.TestPostgres;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The outbox under a broker that fails. No Kafka here on purpose: a fake broker
 * can fail on command, in the nastiest ways, which a real one will not do
 * reliably enough to test against.
 */
class OutboxRelayTest {

  private static final Clock CLOCK = Clock.system(ZoneOffset.UTC);

  /** Records deliveries, and misbehaves the way networks do. */
  private static final class FlakyBroker implements EventPublisher {
    final List<String> delivered = new CopyOnWriteArrayList<>();
    final Random random = new Random(99);
    final double failBeforeDelivery;
    final double failAfterDelivery;
    int failures;

    FlakyBroker(double failBeforeDelivery, double failAfterDelivery) {
      this.failBeforeDelivery = failBeforeDelivery;
      this.failAfterDelivery = failAfterDelivery;
    }

    @Override
    public synchronized void publish(String partitionKey, String eventKey, String envelopeJson) {
      if (random.nextDouble() < failBeforeDelivery) {
        failures++;
        throw new IllegalStateException("broker unreachable");
      }
      delivered.add(envelopeJson);
      if (random.nextDouble() < failAfterDelivery) {
        // The broker has the message but the acknowledgement never arrives, which
        // is exactly a process dying between publish and commit. The relay cannot
        // tell this from a failed send, so it must send the event again.
        failures++;
        throw new IllegalStateException("acknowledgement lost");
      }
    }
  }

  private static PostgresEventStore freshStore() {
    TestPostgres.reset();
    return new PostgresEventStore(TestPostgres.dataSource(), new EventCodec());
  }

  private static void generateTraffic(PostgresEventStore store) {
    SeatCommandHandler handler = new SeatCommandHandler(store, CLOCK);
    for (int seat = 0; seat < 20; seat++) {
      for (int buyer = 0; buyer < 3; buyer++) {
        String id = seat + "-" + buyer;
        try {
          handler.hold("seat-" + seat, "h-" + id, "b" + id, Duration.ofMinutes(5), "hold-" + id);
          if (buyer != 1) { // one buyer per seat abandons, so releases are in the mix too
            handler.confirmSale("seat-" + seat, "h-" + id, "o-" + id, "sale-" + id);
          } else {
            handler.releaseHold("seat-" + seat, "h-" + id, "rel-" + id);
          }
        } catch (DomainException refused) {
          // expected for the buyers who lose
        }
      }
    }
  }

  @Test
  @DisplayName("every committed event is delivered even when the broker drops requests and acknowledgements")
  void nothing_is_lost_and_order_holds_under_faults() {
    PostgresEventStore store = freshStore();
    generateTraffic(store);
    List<StoredEvent> log = store.readAll();

    FlakyBroker broker = new FlakyBroker(0.25, 0.25);
    OutboxRelay relay = new OutboxRelay(TestPostgres.dataSource(), broker, 7);

    int passes = 0;
    while (relay.pending() > 0 && passes++ < 5_000) {
      relay.pollOnce();
    }

    assertThat(relay.pending()).as("outbox drained despite the failures").isZero();
    assertThat(broker.failures).as("the fault paths really ran").isGreaterThan(5);

    EventCodec codec = new EventCodec();
    Set<String> distinct = new HashSet<>();
    Map<String, Long> lastFirstSeen = new HashMap<>();
    for (String envelope : broker.delivered) {
      StoredEvent e = codec.fromEnvelope(envelope);
      String key = e.streamId() + ":" + e.version();
      if (distinct.add(key)) {
        // First sighting of each event: per seat, versions must arrive in order
        // with no gaps, or a projection would see a seat's history scrambled.
        long previous = lastFirstSeen.getOrDefault(e.streamId(), 0L);
        assertThat(e.version()).as("first delivery order for %s", e.streamId()).isEqualTo(previous + 1);
        lastFirstSeen.put(e.streamId(), e.version());
      }
    }
    assertThat(distinct).as("distinct events delivered").hasSize(log.size());
    assertThat(broker.delivered.size())
        .as("at-least-once means duplicates happen; if there are none the faults did nothing")
        .isGreaterThan(log.size());
  }

  @Test
  @DisplayName("an idempotent consumer turns at-least-once into exactly-once effect")
  void deduplicating_consumer_reproduces_the_log_exactly() {
    PostgresEventStore store = freshStore();
    generateTraffic(store);

    FlakyBroker broker = new FlakyBroker(0.2, 0.3);
    OutboxRelay relay = new OutboxRelay(TestPostgres.dataSource(), broker, 5);
    int passes = 0;
    while (relay.pending() > 0 && passes++ < 5_000) {
      relay.pollOnce();
    }

    // A read model fed only from the (duplicated, retried) broker stream...
    EventCodec codec = new EventCodec();
    SeatMapProjection fromBroker = new SeatMapProjection(store, CLOCK);
    for (String envelope : broker.delivered) {
      StoredEvent e = codec.fromEnvelope(envelope);
      fromBroker.apply(e.streamId(), e.version(), e.event());
    }
    // ...must equal one folded straight from the log.
    SeatMapProjection fromLog = new SeatMapProjection(store, CLOCK);
    fromLog.rebuild();

    assertThat(broker.delivered.size()).isGreaterThan(store.readAll().size());
    assertThat(fromBroker.all()).containsExactlyElementsOf(fromLog.all());
    assertThat(fromBroker.all()).isNotEmpty();
  }

  @Test
  @DisplayName("two relays running at once do not interleave a seat's events")
  void only_one_relay_drains_at_a_time() throws Exception {
    PostgresEventStore store = freshStore();
    generateTraffic(store);
    List<String> delivered = new CopyOnWriteArrayList<>();
    EventPublisher slow =
        (key, eventKey, json) -> {
          delivered.add(eventKey);
          java.util.concurrent.locks.LockSupport.parkNanos(200_000);
        };
    OutboxRelay a = new OutboxRelay(TestPostgres.dataSource(), slow, 50);
    OutboxRelay b = new OutboxRelay(TestPostgres.dataSource(), slow, 50);

    Thread ta = new Thread(() -> drain(a));
    Thread tb = new Thread(() -> drain(b));
    ta.start();
    tb.start();
    ta.join(60_000);
    tb.join(60_000);

    assertThat(delivered).as("each event exactly once when nothing fails").doesNotHaveDuplicates();
    assertThat(delivered).hasSize(store.readAll().size());

    Map<String, Long> last = new HashMap<>();
    for (String key : delivered) {
      String stream = key.substring(0, key.lastIndexOf(':'));
      long version = Long.parseLong(key.substring(key.lastIndexOf(':') + 1));
      assertThat(version).as("order within %s", stream).isEqualTo(last.getOrDefault(stream, 0L) + 1);
      last.put(stream, version);
    }
  }

  private static void drain(OutboxRelay relay) {
    int guard = 0;
    while (relay.pending() > 0 && guard++ < 10_000) {
      relay.pollOnce();
    }
  }


  @Test
  @DisplayName("pruning deletes old published rows and never an unpublished or recent one")
  void pruning_is_safe() {
    PostgresEventStore store = freshStore();
    generateTraffic(store);
    OutboxRelay relay = new OutboxRelay(TestPostgres.dataSource(), (key, eventKey, json) -> {}, 50);
    while (relay.pending() > 0) {
      relay.pollOnce();
    }
    var jdbc = new org.springframework.jdbc.core.JdbcTemplate(TestPostgres.dataSource());
    long total = jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class);

    // Half of them were published long ago, half just now.
    jdbc.update("UPDATE outbox SET published_at = now() - interval '2 hours' WHERE id % 2 = 0");
    long old = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at < now() - interval '1 hour'", Long.class);
    // And one event is committed but not yet published, and has been for two hours.
    new SeatCommandHandler(store, CLOCK).hold("late-seat", "hl", "bl", Duration.ofMinutes(5), "kl");
    jdbc.update("UPDATE outbox SET created_at = now() - interval '2 hours' WHERE published_at IS NULL");

    int deleted = relay.prune(Duration.ofHours(1));

    assertThat(deleted).as("only the rows published more than an hour ago").isEqualTo((int) old);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class)).isEqualTo(total + 1 - old);
    assertThat(relay.pending()).as("an announcement still owed is never pruned, however old").isEqualTo(1);
  }
}
