package dev.turnstile.tools;

import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainException;
import dev.turnstile.eventstore.InMemoryEventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a contended sale and writes the resulting event log to stdout as NDJSON.
 *
 * <p>Exists so the shell verifier has something real to audit. The point of the
 * pipeline
 *
 * <pre>
 *   turnstilectl simulate | scripts/verify-invariants.sh -s
 * </pre>
 *
 * is that the two halves share no code. This side is the system under test; the
 * awk on the other side of the pipe re-derives the invariants from the bytes
 * alone and has no idea a SeatAggregate exists. If they ever disagree, this side
 * is the one that is wrong.
 *
 * <p>Diagnostics go to stderr so stdout stays a clean stream of events that can
 * be piped, teed, or redirected without contamination.
 */
public final class SimulateRun {

  private static final Duration HOLD_TTL = Duration.ofMinutes(5);

  public static void main(String[] args) throws Exception {
    int seats = positiveIntArg(args, "--seats", 200);
    int buyers = positiveIntArg(args, "--buyers", 2_000);
    int threads = positiveIntArg(args, "--threads", 64);

    PrintStream err = System.err;
    err.printf("simulating %d buyers against %d seats on %d threads%n", buyers, seats, threads);

    InMemoryEventStore store = new InMemoryEventStore();
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));

    CountDownLatch startGun = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(buyers);
    AtomicInteger sold = new AtomicInteger();
    AtomicInteger rejected = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    AtomicReference<RuntimeException> firstFailure = new AtomicReference<>();

    boolean timedOut = false;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < buyers; i++) {
        // Consecutive buyers share a seat. Tasks reach the pool in order, so
        // only neighbours run concurrently; "i % seats" put same-seat buyers
        // `seats` tasks apart, which never overlap and produced no contention.
        String seatId = "seat-" + ((long) i * seats / buyers);
        String buyerId = "buyer-" + i;
        pool.submit(
            () -> {
              try {
                startGun.await();
                String holdId = UUID.randomUUID().toString();
                handler.hold(seatId, holdId, buyerId, HOLD_TTL, "hold-" + buyerId);
                handler.confirmSale(seatId, holdId, "order-" + buyerId, "sale-" + buyerId);
                sold.incrementAndGet();
              } catch (DomainException expected) {
                // Losing the race is the correct outcome for most buyers.
                rejected.incrementAndGet();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed.incrementAndGet();
              } catch (RuntimeException unexpected) {
                // Retry exhaustion and anything else genuinely wrong. Counted
                // and reported rather than disappearing into an unread Future,
                // because a buyer that neither bought nor was refused is the
                // one case that must never pass silently.
                failed.incrementAndGet();
                firstFailure.compareAndSet(null, unexpected);
              } finally {
                finished.countDown();
              }
            });
      }

      long startedAt = System.nanoTime();
      startGun.countDown();
      timedOut = !finished.await(120, TimeUnit.SECONDS);
      long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;
      if (!timedOut) {
        err.printf("settled in %d ms%n", elapsedMs);
      }

    } finally {
      pool.shutdown();
      if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    }

    // Reported before the exit checks so a failed run still explains itself.
    err.printf("sold %d, refused %d, failed %d%n", sold.get(), rejected.get(), failed.get());

    if (timedOut) {
      err.println("timed out waiting for buyers");
      System.exit(2);
    }
    if (failed.get() > 0) {
      err.printf("%d buyer(s) failed unexpectedly; first failure:%n", failed.get());
      RuntimeException failure = firstFailure.get();
      if (failure != null) {
        failure.printStackTrace(err);
      }
      System.exit(3);
    }
    if (sold.get() + rejected.get() != buyers) {
      err.printf(
          "accounting error: %d sold + %d refused != %d buyers%n",
          sold.get(), rejected.get(), buyers);
      System.exit(4);
    }

    List<StoredEvent> log = store.readAll();
    err.printf("writing %d events to stdout%n", log.size());
    NdjsonExport.write(log, System.out);
  }


  /**
   * Parses a positive integer argument, failing with a usable message.
   *
   * <p>Validation is not ceremony here. {@code --seats 0} used to reach the
   * worker loop and throw ArithmeticException on {@code i % seats} inside every
   * one of the buyer threads, where the executor swallowed it into a Future
   * nobody read, and the run simply produced an empty log.
   */
  private static int positiveIntArg(String[] args, String name, int fallback) {
    for (int i = 0; i < args.length - 1; i++) {
      if (args[i].equals(name)) {
        int value;
        try {
          value = Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              name + " expects an integer, got: " + args[i + 1]);
        }
        if (value <= 0) {
          throw new IllegalArgumentException(name + " must be greater than zero, got: " + value);
        }
        return value;
      }
    }
    return fallback;
  }

  private SimulateRun() {}
}
