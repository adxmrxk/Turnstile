package dev.turnstile.web;

import dev.turnstile.HoldProperties;
import dev.turnstile.PricingProperties;
import dev.turnstile.audit.LogAuditor;
import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.payment.SimulatedPaymentGateway;
import dev.turnstile.saga.InMemorySagaLog;
import dev.turnstile.saga.PurchaseSaga;
import dev.turnstile.saga.SagaRecord;
import jakarta.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Releases a crowd of buyers at once and audits the result.
 *
 * <p>This is the project's thesis as something you can watch. Every buyer runs the
 * real purchase saga against the real store, so holds contend, payments fail in
 * the ways the simulator fails them (declines, dropped requests, lost responses),
 * refunds happen, and the seat map updates live. When the crowd has settled, the
 * run's own events are exported and re-checked by the independent auditor, and
 * the payment ledger is checked against the seats sold.
 *
 * <p>The run uses its own seats (prefixed with a run id) so it never touches real
 * inventory, and only one run may be in flight at a time.
 */
@Service
@ConditionalOnProperty(name = "turnstile.demo.enabled", havingValue = "true", matchIfMissing = true)
public class RushService {

  public record Progress(
      String runId, int total, int done, int sold, int refused, int declined, int refunded, boolean finished) {}

  public record Audit(boolean clean, long events, long seatsSold, List<String> violations) {}

  public record Money(long heldCents, long expectedCents, boolean balanced) {}

  public record Result(
      Progress progress, int seats, long millis, Audit audit, Money money, String seatPrefix) {}

  private final NotifyingEventStore store;
  private final SeatCommandHandler handler;
  private final HoldProperties hold;
  private final PricingProperties pricing;
  private final SimpMessagingTemplate messaging;

  private final AtomicBoolean busy = new AtomicBoolean();
  private final Map<String, Result> results = new ConcurrentHashMap<>();
  private final ExecutorService runner =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r, "rush-runner");
            t.setDaemon(true);
            return t;
          });

  public RushService(
      NotifyingEventStore store,
      SeatCommandHandler handler,
      HoldProperties hold,
      PricingProperties pricing,
      SimpMessagingTemplate messaging) {
    this.store = store;
    this.handler = handler;
    this.hold = hold;
    this.pricing = pricing;
    this.messaging = messaging;
  }

  /** Starts a run and returns immediately. @throws IllegalStateException if one is running */
  public Result start(int seats, int buyers) {
    if (seats < 1 || seats > 1_000 || buyers < 1 || buyers > 20_000) {
      throw new IllegalArgumentException("seats must be 1-1000 and buyers 1-20000");
    }
    if (!busy.compareAndSet(false, true)) {
      throw new IllegalStateException("a rush is already running");
    }
    String runId = UUID.randomUUID().toString().substring(0, 6);
    String prefix = "demo-" + runId + "-";
    Result pending =
        new Result(new Progress(runId, buyers, 0, 0, 0, 0, 0, false), seats, 0, null, null, prefix);
    results.put(runId, pending);
    runner.submit(
        () -> {
          try {
            results.put(runId, run(runId, prefix, seats, buyers));
          } catch (RuntimeException failure) {
            results.put(
                runId,
                new Result(
                    new Progress(runId, buyers, buyers, 0, 0, 0, 0, true),
                    seats,
                    0,
                    new Audit(false, 0, 0, List.of("run failed: " + failure)),
                    null,
                    prefix));
          } finally {
            busy.set(false);
          }
        });
    return pending;
  }

  public Result result(String runId) {
    return results.get(runId);
  }

  private Result run(String runId, String prefix, int seats, int buyers) {
    // A provider that misbehaves a little, so refunds and releases are on screen.
    SimulatedPaymentGateway gateway = new SimulatedPaymentGateway(0.08, 0.02, 0.04, 6, runId.hashCode());
    PurchaseSaga saga = new PurchaseSaga(handler, gateway, new InMemorySagaLog(), hold.ttl());

    AtomicInteger sold = new AtomicInteger();
    AtomicInteger refused = new AtomicInteger();
    AtomicInteger declined = new AtomicInteger();
    AtomicInteger refunded = new AtomicInteger();
    AtomicInteger done = new AtomicInteger();

    ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
    ticker.scheduleWithFixedDelay(
        () -> publish(progress(runId, buyers, done, sold, refused, declined, refunded, false)),
        0, 120, TimeUnit.MILLISECONDS);

    CountDownLatch go = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(buyers);
    ExecutorService pool = Executors.newFixedThreadPool(64);
    long started;
    try {
      for (int i = 0; i < buyers; i++) {
        String id = "rush-" + runId + "-" + i;
        // Neighbours share a seat, so the crowd genuinely collides on each one.
        String seat = prefix + String.format("%04d", (long) i * seats / buyers);
        pool.submit(
            () -> {
              try {
                go.await();
                SagaRecord r = saga.start(id, seat, "buyer-" + id, pricing.seatPriceCents());
                switch (r.state()) {
                  case CONFIRMED -> sold.incrementAndGet();
                  case REFUSED -> refused.incrementAndGet();
                  case DECLINED -> declined.incrementAndGet();
                  case REFUNDED -> refunded.incrementAndGet();
                  default -> refused.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.incrementAndGet();
                finished.countDown();
              }
            });
      }
      started = System.nanoTime();
      go.countDown();
      finished.await(180, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } finally {
      pool.shutdownNow();
      ticker.shutdownNow();
    }
    long millis = (System.nanoTime() - started) / 1_000_000;

    Audit audit = auditRun(prefix);
    java.util.Set<String> soldOrders = new TreeSet<>();
    for (StoredEvent e : store.readAll()) {
      if (e.event() instanceof DomainEvent.SeatSold s && s.seatId().startsWith(prefix)) {
        soldOrders.add(s.orderId());
      }
    }
    long expected = soldOrders.size() * pricing.seatPriceCents();
    Money money =
        new Money(
            gateway.netCents(),
            expected,
            gateway.netCents() == expected && gateway.chargedOrders().equals(soldOrders));

    Progress last = progress(runId, buyers, done, sold, refused, declined, refunded, true);
    publish(last);
    return new Result(last, seats, millis, audit, money, prefix);
  }

  private Audit auditRun(String prefix) {
    try {
      List<StoredEvent> mine = store.readAll().stream().filter(e -> e.event().seatId().startsWith(prefix)).toList();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      NdjsonExport.write(mine, out);
      String text = out.toString(StandardCharsets.UTF_8);
      LogAuditor.Report report =
          new LogAuditor().audit(text.isEmpty() ? List.of() : Arrays.asList(text.split("\n")));
      return new Audit(report.clean(), report.events(), report.seatsSold(), report.violations());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Progress progress(
      String runId, int total, AtomicInteger done, AtomicInteger sold, AtomicInteger refused,
      AtomicInteger declined, AtomicInteger refunded, boolean finished) {
    return new Progress(runId, total, done.get(), sold.get(), refused.get(), declined.get(), refunded.get(), finished);
  }

  private void publish(Progress progress) {
    messaging.convertAndSend("/topic/rush", progress);
  }

  @PreDestroy
  void stop() {
    runner.shutdownNow();
  }
}
