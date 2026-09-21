package dev.turnstile.saga;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.InMemoryEventStore;
import dev.turnstile.eventstore.PostgresEventStore;
import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.payment.PaymentGateway;
import dev.turnstile.payment.SimulatedPaymentGateway;
import dev.turnstile.saga.SagaRecord.State;
import dev.turnstile.testsupport.TestPostgres;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The purchase saga, path by path, then under randomised failure with the check
 * that matters for a payment flow: money and seats balance. A saga that merely
 * finishes without throwing could still have charged a buyer for a seat they did
 * not get, so the assertions are about the ledger and the log, not about
 * exceptions.
 */
class PurchaseSagaTest {

  private static final long PRICE = 5_000;
  private static final Duration LONG_TTL = Duration.ofMinutes(5);

  /** Everything one scenario needs, built over a chosen store. */
  private static final class World {
    final EventStore store;
    final SagaLog log;
    final SeatCommandHandler handler;

    World(EventStore store, SagaLog log) {
      this.store = store;
      this.log = log;
      this.handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    }

    PurchaseSaga saga(PaymentGateway gateway, Duration ttl) {
      return new PurchaseSaga(handler, gateway, log, ttl);
    }
  }

  private static World memoryWorld() {
    return new World(new InMemoryEventStore(), new InMemorySagaLog());
  }

  private static World postgresWorld() {
    TestPostgres.reset();
    return new World(
        new PostgresEventStore(TestPostgres.dataSource(), new EventCodec()),
        new JdbcSagaLog(TestPostgres.dataSource()));
  }

  // ---------------------------------------------------------------------------
  // Paths
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("a reliable payment sells the seat")
  void happy_path() {
    World w = memoryWorld();
    SimulatedPaymentGateway gateway = SimulatedPaymentGateway.reliable();

    SagaRecord result = w.saga(gateway, LONG_TTL).start("s1", "seat-1", "alice", PRICE);

    assertThat(result.state()).isEqualTo(State.CONFIRMED);
    assertThat(gateway.stateOf("order-s1")).isEqualTo(SimulatedPaymentGateway.Entry.CHARGED);
    assertThat(soldSeats(w)).containsExactly("seat-1");
  }

  @Test
  @DisplayName("a declined card releases the hold, so the next buyer can have the seat")
  void declined_payment_frees_the_seat() {
    World w = memoryWorld();
    SimulatedPaymentGateway declining = new SimulatedPaymentGateway(1, 0, 0, 0, 1);

    SagaRecord first = w.saga(declining, LONG_TTL).start("s1", "seat-1", "alice", PRICE);
    SagaRecord second =
        w.saga(SimulatedPaymentGateway.reliable(), LONG_TTL).start("s2", "seat-1", "bob", PRICE);

    assertThat(first.state()).isEqualTo(State.DECLINED);
    assertThat(declining.netCents()).as("no money kept from the declined buyer").isZero();
    assertThat(second.state()).as("the seat went back on sale").isEqualTo(State.CONFIRMED);
  }

  @Test
  @DisplayName("a seat that is already sold refuses the second buyer without charging them")
  void second_buyer_is_refused_and_not_charged() {
    World w = memoryWorld();
    SimulatedPaymentGateway gateway = SimulatedPaymentGateway.reliable();
    w.saga(gateway, LONG_TTL).start("s1", "seat-1", "alice", PRICE);

    SagaRecord second = w.saga(gateway, LONG_TTL).start("s2", "seat-1", "bob", PRICE);

    assertThat(second.state()).isEqualTo(State.REFUSED);
    assertThat(gateway.stateOf("order-s2")).as("bob was never charged").isNull();
  }

  @Test
  @DisplayName("payment that lands after the hold expired is refunded, not sold")
  void late_payment_is_refunded() {
    World w = memoryWorld();
    SimulatedPaymentGateway inner = SimulatedPaymentGateway.reliable();
    PaymentGateway slow =
        new PaymentGateway() {
          @Override
          public String charge(String orderId, String buyerId, long cents) {
            sleep(60);
            return inner.charge(orderId, buyerId, cents);
          }

          @Override
          public void refund(String orderId) {
            inner.refund(orderId);
          }
        };

    SagaRecord result = w.saga(slow, Duration.ofMillis(10)).start("s1", "seat-1", "alice", PRICE);

    assertThat(result.state()).isEqualTo(State.REFUNDED);
    assertThat(inner.stateOf("order-s1")).isEqualTo(SimulatedPaymentGateway.Entry.REFUNDED);
    assertThat(inner.netCents()).isZero();
    assertThat(soldSeats(w)).as("the seat was not sold on an expired hold").isEmpty();
  }

  @Test
  @DisplayName("a charge whose response is lost is not billed twice, and the sale completes")
  void lost_response_is_recovered_by_retrying_an_idempotent_charge() {
    World w = memoryWorld();
    SimulatedPaymentGateway losesResponses = new SimulatedPaymentGateway(0, 0, 1, 0, 1);

    SagaRecord result = w.saga(losesResponses, LONG_TTL).start("s1", "seat-1", "alice", PRICE);

    assertThat(result.state()).isEqualTo(State.CONFIRMED);
    assertThat(losesResponses.netCents()).as("charged exactly once").isEqualTo(PRICE);
  }

  @Test
  @DisplayName("a request that never gets processed ends declined and cannot land later")
  void dropped_request_is_cancelled_so_it_cannot_land_late() {
    World w = memoryWorld();
    SimulatedPaymentGateway dropsEverything = new SimulatedPaymentGateway(0, 1, 0, 0, 1);

    SagaRecord result = w.saga(dropsEverything, LONG_TTL).start("s1", "seat-1", "alice", PRICE);

    assertThat(result.state()).isEqualTo(State.DECLINED);
    assertThat(dropsEverything.stateOf("order-s1"))
        .as("the order is cancelled, so a delayed charge would be refused")
        .isEqualTo(SimulatedPaymentGateway.Entry.CANCELLED);
  }

  @Test
  @DisplayName("the same purchase id sent twice is one purchase")
  void duplicate_start_is_the_same_purchase() {
    World w = memoryWorld();
    SimulatedPaymentGateway gateway = SimulatedPaymentGateway.reliable();
    PurchaseSaga saga = w.saga(gateway, LONG_TTL);

    SagaRecord first = saga.start("s1", "seat-1", "alice", PRICE);
    SagaRecord again = saga.start("s1", "seat-1", "alice", PRICE);

    assertThat(again).isEqualTo(first);
    assertThat(gateway.netCents()).as("billed once").isEqualTo(PRICE);
    assertThat(soldSeats(w)).hasSize(1);
  }

  // ---------------------------------------------------------------------------
  // Crashes
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("a crash at every point between an effect and its save recovers to a consistent state")
  void crash_at_every_point_recovers() {
    for (String point : List.of("after-hold", "after-charge", "after-confirm")) {
      for (double lostResponse : List.of(0.0, 1.0)) {
        World w = memoryWorld();
        SimulatedPaymentGateway gateway = new SimulatedPaymentGateway(0, 0, lostResponse, 0, 1);
        PurchaseSaga.CrashPoint dies =
            reached -> {
              if (reached.equals(point)) {
                throw new IllegalStateException("simulated crash at " + reached);
              }
            };

        try {
          new PurchaseSaga(w.handler, gateway, w.log, LONG_TTL, dies)
              .start("s1", "seat-1", "alice", PRICE);
          throw new AssertionError("the crash point " + point + " was never reached");
        } catch (IllegalStateException expected) {
          assertThat(expected.getMessage()).contains(point);
        }
        assertThat(w.log.find("s1").orElseThrow().state().terminal())
            .as("the process died mid-saga at %s", point)
            .isFalse();

        // A fresh process, same durable state.
        List<SagaRecord> recovered = w.saga(gateway, LONG_TTL).recoverIncomplete();

        String context = "crash at " + point + ", lost-response rate " + lostResponse;
        assertThat(recovered).as(context).hasSize(1);
        assertThat(recovered.get(0).state()).as(context).isEqualTo(State.CONFIRMED);
        assertThat(gateway.netCents()).as("billed exactly once, " + context).isEqualTo(PRICE);
        assertThat(soldSeats(w)).as("sold exactly once, " + context).containsExactly("seat-1");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Money and seats balance under randomised failure
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("in-memory: 300 concurrent purchases under random payment failures balance")
  void balances_under_failure_in_memory() throws Exception {
    balancesUnderFailure(PurchaseSagaTest::memoryWorld, LONG_TTL, 11);
  }

  @Test
  @DisplayName("in-memory: the same, with holds so short that most payments land late")
  void balances_when_most_holds_expire() throws Exception {
    balancesUnderFailure(PurchaseSagaTest::memoryWorld, Duration.ofMillis(8), 12);
  }

  @Test
  @DisplayName("on real PostgreSQL: 200 concurrent purchases under random failures balance")
  void balances_under_failure_on_postgres() throws Exception {
    balancesUnderFailure(PurchaseSagaTest::postgresWorld, LONG_TTL, 13, 200, 25);
  }

  private void balancesUnderFailure(Supplier<World> factory, Duration ttl, long seed)
      throws Exception {
    balancesUnderFailure(factory, ttl, seed, 300, 30);
  }

  private void balancesUnderFailure(
      Supplier<World> factory, Duration ttl, long seed, int buyers, int seats) throws Exception {
    World w = factory.get();
    // 10% declined, 5% dropped, 10% charged-but-response-lost, up to 15ms latency.
    SimulatedPaymentGateway gateway = new SimulatedPaymentGateway(0.10, 0.05, 0.10, 15, seed);
    PurchaseSaga saga = w.saga(gateway, ttl);

    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(32);
    List<Future<SagaRecord>> futures = new ArrayList<>();
    try {
      for (int i = 0; i < buyers; i++) {
        String id = "s" + i;
        String seat = "seat-" + (i * seats / buyers); // neighbours share a seat
        futures.add(
            pool.submit(
                () -> {
                  go.await();
                  return saga.start(id, seat, "buyer-" + id, PRICE);
                }));
      }
      go.countDown();

      Map<String, SagaRecord> results = new HashMap<>();
      for (Future<SagaRecord> f : futures) {
        SagaRecord r = f.get(120, TimeUnit.SECONDS);
        results.put(r.sagaId(), r);
      }
      assertBalanced(w, gateway, results.values(), seats);
    } finally {
      pool.shutdownNow();
    }
  }

  private static void assertBalanced(
      World w, SimulatedPaymentGateway gateway, Iterable<SagaRecord> sagas, int seats) {
    List<DomainEvent> log = w.store.readAll().stream().map(StoredEvent::event).toList();
    Map<String, Long> soldPerSeat =
        log.stream()
            .filter(DomainEvent.SeatSold.class::isInstance)
            .map(DomainEvent.SeatSold.class::cast)
            .collect(Collectors.groupingBy(DomainEvent.SeatSold::seatId, Collectors.counting()));
    Set<String> soldOrders =
        log.stream()
            .filter(DomainEvent.SeatSold.class::isInstance)
            .map(e -> ((DomainEvent.SeatSold) e).orderId())
            .collect(Collectors.toCollection(TreeSet::new));

    assertThat(soldPerSeat.values()).as("no seat sold twice").allMatch(n -> n == 1L);
    assertThat(soldPerSeat.size()).as("never more sales than seats").isLessThanOrEqualTo(seats);

    Set<String> confirmed = new TreeSet<>();
    for (SagaRecord s : sagas) {
      assertThat(s.state().terminal()).as("saga %s finished", s.sagaId()).isTrue();
      if (s.state() == State.CONFIRMED) {
        confirmed.add(s.orderId());
      } else {
        assertThat(gateway.stateOf(s.orderId()))
            .as("saga %s ended %s, so its buyer must not be out of pocket", s.sagaId(), s.state())
            .isNotEqualTo(SimulatedPaymentGateway.Entry.CHARGED);
      }
    }

    assertThat(confirmed).as("orders the sagas call sold == orders in the log").isEqualTo(soldOrders);
    assertThat(gateway.chargedOrders())
        .as("orders whose money is held == orders that got a seat")
        .isEqualTo(soldOrders);
    assertThat(gateway.netCents())
        .as("money held == seats sold x price")
        .isEqualTo(soldOrders.size() * PRICE);
  }

  private static Set<String> soldSeats(World w) {
    return w.store.readAll().stream()
        .map(StoredEvent::event)
        .filter(DomainEvent.SeatSold.class::isInstance)
        .map(DomainEvent::seatId)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
