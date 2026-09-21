package dev.turnstile.bench;

import dev.turnstile.TurnstileApplication;
import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.domain.DomainException;
import dev.turnstile.eventstore.EventCodec;
import dev.turnstile.eventstore.PostgresEventStore;
import dev.turnstile.testsupport.TestPostgres;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Measurements, not assertions. Run one at a time:
 *
 * <pre>
 *   mvn test -Dtest=Benchmarks#appendThroughput -Dbench=true
 * </pre>
 *
 * Each prints one clearly marked result block. They exist so that an improvement
 * is a before-and-after number and not a feeling, and they never run in the normal
 * suite.
 */
@EnabledIfSystemProperty(named = "bench", matches = "true")
class Benchmarks {

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static void report(String name, String... lines) {
    System.out.println("\n##### BENCH " + name + " #####");
    for (String line : lines) {
      System.out.println("##   " + line);
    }
    System.out.println("##### END #####\n");
  }

  private static long pct(List<Long> sortedNanos, double p) {
    return sortedNanos.get((int) Math.min(sortedNanos.size() - 1, Math.floor(p * sortedNanos.size()))) / 1_000;
  }

  private static String latencies(List<Long> nanos) {
    List<Long> sorted = new ArrayList<>(nanos);
    Collections.sort(sorted);
    return "latency us: p50=" + pct(sorted, 0.50) + " p95=" + pct(sorted, 0.95) + " p99=" + pct(sorted, 0.99);
  }

  // ---------------------------------------------------------------------------

  /** Commands per second straight through the handler onto PostgreSQL, no HTTP in the way. */
  @Test
  void appendThroughput() throws Exception {
    TestPostgres.reset();
    PostgresEventStore store = new PostgresEventStore(TestPostgres.pooled(32), new EventCodec());
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    int commands = 6_000; // hold + confirm on 3,000 distinct seats
    int threads = 16;

    List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    long start = System.nanoTime();
    List<Future<?>> work = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      int offset = t;
      work.add(
          pool.submit(
              () -> {
                for (int seat = offset; seat < commands / 2; seat += threads) {
                  String id = "b" + seat;
                  long a = System.nanoTime();
                  handler.hold("bench-seat-" + seat, "h" + id, id, Duration.ofMinutes(5), "hold-" + id);
                  latencies.add(System.nanoTime() - a);
                  long b = System.nanoTime();
                  handler.confirmSale("bench-seat-" + seat, "h" + id, "o" + id, "sale-" + id);
                  latencies.add(System.nanoTime() - b);
                }
                return null;
              }));
    }
    for (Future<?> f : work) {
      f.get();
    }
    double seconds = (System.nanoTime() - start) / 1e9;
    pool.shutdown();
    report("appendThroughput",
        String.format("commands/sec = %.0f  (%d commands in %.1fs, %d threads)", commands / seconds, commands, seconds, threads),
        latencies(latencies));
  }

  /** The same, but many buyers fight over few seats, so conflicts and retries dominate. */
  @Test
  void contendedThroughput() throws Exception {
    TestPostgres.reset();
    PostgresEventStore store = new PostgresEventStore(TestPostgres.pooled(32), new EventCodec());
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    int buyers = 3_000;
    int seats = 30;
    int threads = 32;
    AtomicInteger sold = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<?>> work = new ArrayList<>();
    for (int i = 0; i < buyers; i++) {
      int n = i;
      work.add(
          pool.submit(
              () -> {
                go.await();
                String seat = "hot-" + (n * seats / buyers);
                long a = System.nanoTime();
                try {
                  handler.hold(seat, "h" + n, "b" + n, Duration.ofMinutes(5), "hold-" + n);
                  handler.confirmSale(seat, "h" + n, "o" + n, "sale-" + n);
                  sold.incrementAndGet();
                } catch (DomainException refused) {
                  // expected for losers
                } catch (RuntimeException e) {
                  failed.incrementAndGet();
                }
                latencies.add(System.nanoTime() - a);
                return null;
              }));
    }
    long start = System.nanoTime();
    go.countDown();
    for (Future<?> f : work) {
      f.get();
    }
    double seconds = (System.nanoTime() - start) / 1e9;
    pool.shutdown();
    report("contendedThroughput",
        String.format("buyers/sec = %.0f  (%d buyers on %d seats in %.1fs)", buyers / seconds, buyers, seats, seconds),
        "sold=" + sold + " (expect " + seats + ")  unexpected failures=" + failed,
        latencies(latencies));
  }

  // ---------------------------------------------------------------------------

  private static ConfigurableApplicationContext node(Map<String, Object> extra) {
    java.util.Map<String, Object> props = new java.util.HashMap<>();
    props.put("server.port", "0");
    props.put("turnstile.demo.enabled", "false");
    props.putAll(extra);
    return new SpringApplicationBuilder(TurnstileApplication.class).properties(props).run();
  }

  private static Map<String, Object> postgresProps(Map<String, Object> more) {
    java.util.Map<String, Object> p = new java.util.HashMap<>();
    p.put("turnstile.store", "postgres");
    p.put("turnstile.postgres.url", TestPostgres.jdbcUrl());
    p.put("turnstile.postgres.username", "postgres");
    p.put("turnstile.postgres.password", "");
    if (System.getProperty("bench.pool") != null) {
      p.put("turnstile.postgres.pool-size", System.getProperty("bench.pool"));
    }
    p.putAll(more);
    return p;
  }

  private static String base(ConfigurableApplicationContext ctx) {
    return "http://localhost:" + ctx.getEnvironment().getProperty("local.server.port");
  }

  /** Purchases per second over real HTTP onto PostgreSQL with 64 concurrent clients. */
  @Test
  void httpPurchases() throws Exception {
    TestPostgres.reset();
    ConfigurableApplicationContext ctx = node(postgresProps(Map.of()));
    try {
      int requests = 4_000;
      int clients = 64;
      List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
      AtomicInteger ok = new AtomicInteger();
      ExecutorService pool = Executors.newFixedThreadPool(clients);
      CountDownLatch go = new CountDownLatch(1);
      List<Future<?>> work = new ArrayList<>();
      for (int i = 0; i < requests; i++) {
        int n = i;
        work.add(
            pool.submit(
                () -> {
                  go.await();
                  long a = System.nanoTime();
                  HttpResponse<String> r =
                      HTTP.send(
                          HttpRequest.newBuilder(URI.create(base(ctx) + "/api/purchases"))
                              .header("Content-Type", "application/json")
                              .header("Idempotency-Key", "k" + n)
                              .POST(HttpRequest.BodyPublishers.ofString("{\"seatId\":\"http-" + n + "\",\"buyerId\":\"u" + n + "\"}"))
                              .build(),
                          HttpResponse.BodyHandlers.ofString());
                  latencies.add(System.nanoTime() - a);
                  if (r.statusCode() == 201) {
                    ok.incrementAndGet();
                  }
                  return null;
                }));
      }
      long start = System.nanoTime();
      go.countDown();
      for (Future<?> f : work) {
        f.get();
      }
      double seconds = (System.nanoTime() - start) / 1e9;
      pool.shutdown();
      MeterRegistry registry = ctx.getBean(MeterRegistry.class);
      report("httpPurchases",
          String.format("purchases/sec = %.0f  (%d ok of %d in %.1fs, %d clients, pool size %s)",
              ok.get() / seconds, ok.get(), requests, seconds, clients, ctx.getEnvironment().getProperty("turnstile.postgres.pool-size", "default")),
          latencies(latencies),
          "hikari max=" + registry.find("hikaricp.connections.max").gauge().value()
              + "  total time waiting for a connection (s)=" + registry.find("hikaricp.connections.acquire").timer().totalTime(TimeUnit.SECONDS));
    } finally {
      ctx.close();
    }
  }

  // ---------------------------------------------------------------------------

  /** Time and heap to export the whole log, at a size where holding it all in memory hurts. */
  @Test
  void exportLargeLog() throws Exception {
    TestPostgres.reset();
    int events = Integer.getInteger("bench.events", 600_000);
    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    // Straight into the table: the point is reading it back, and 600k appends would take minutes.
    jdbc.update(
        "INSERT INTO events (stream_id, version, type, payload, occurred_at) "
            + "SELECT 'big-' || g, 1, 'SeatHeld', "
            + "jsonb_build_object('seatId', 'big-' || g, 'holdId', 'h' || g, 'buyerId', 'b' || g, "
            + "'occurredAt', '2026-09-21T12:00:00Z', 'expiresAt', '2026-09-21T12:05:00Z'), now() "
            + "FROM generate_series(1, ?) g",
        events);
    PostgresEventStore store = new PostgresEventStore(TestPostgres.pooled(32), new EventCodec());

    Runtime rt = Runtime.getRuntime();
    System.gc();
    long start = System.nanoTime();
    String outcome;
    try {
      exportTo(store, OutputStream.nullOutputStream());
      outcome = "completed";
    } catch (OutOfMemoryError oom) {
      outcome = "OutOfMemoryError";
    }
    double seconds = (System.nanoTime() - start) / 1e9;
    report("exportLargeLog",
        String.format("%,d events, max heap %d MB: %s in %.1fs", events, rt.maxMemory() / 1_000_000, outcome, seconds));
  }

  /** The export path under test. Replaced when the store learns to stream. */
  private static void exportTo(PostgresEventStore store, OutputStream out) throws Exception {
    NdjsonExport.write(store, out);
  }

  // ---------------------------------------------------------------------------

  /** How many events a restarted instance reprocesses from Kafka, and how many stay in the outbox. */
  @Test
  void restartAndOutbox() throws Exception {
    TestPostgres.reset();
    EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 3, "turnstile.seat-events");
    kafka.afterPropertiesSet();
    try {
      Map<String, Object> props =
          postgresProps(
              Map.of(
                  "turnstile.kafka.enabled", "true",
                  "spring.kafka.bootstrap-servers", kafka.getBrokersAsString(),
                  "spring.kafka.consumer.auto-offset-reset", "earliest",
                  "turnstile.outbox.retention", "PT0S",
                  "turnstile.outbox.prune-interval", "PT1S",
                  "turnstile.kafka.replay-margin", "PT1S"));
      ConfigurableApplicationContext first = node(props);
      int purchases = 1_500;
      ExecutorService pool = Executors.newFixedThreadPool(32);
      List<Future<?>> work = new ArrayList<>();
      for (int i = 0; i < purchases; i++) {
        int n = i;
        work.add(
            pool.submit(
                () -> HTTP.send(
                    HttpRequest.newBuilder(URI.create(base(first) + "/api/purchases"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"seatId\":\"rs-" + n + "\",\"buyerId\":\"u" + n + "\"}"))
                        .build(),
                    HttpResponse.BodyHandlers.ofString())));
      }
      for (Future<?> f : work) {
        f.get();
      }
      pool.shutdown();
      JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline
          && jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class) > 0) {
        Thread.sleep(200);
      }
      Thread.sleep(2_000);
      long events = jdbc.queryForObject("SELECT count(*) FROM events", Long.class);
      long outboxRows = jdbc.queryForObject("SELECT count(*) FROM outbox", Long.class);
      first.close();

      ConfigurableApplicationContext second = node(props);
      Thread.sleep(8_000); // let the consumer catch up
      MeterRegistry m = second.getBean(MeterRegistry.class);
      double applied = counter(m, "applied");
      double duplicates = counter(m, "duplicate");
      second.close();
      report("restartAndOutbox",
          "events in the log = " + events,
          "outbox rows still stored after everything was published = " + outboxRows,
          "after a restart, messages the new instance re-read from Kafka: " + (long) (applied + duplicates)
              + "  (applied=" + (long) applied + " duplicate=" + (long) duplicates + ")");
    } finally {
      kafka.destroy();
    }
  }

  private static double counter(MeterRegistry m, String result) {
    var c = m.find("turnstile.projection.events").tag("result", result).counter();
    return c == null ? 0 : c.count();
  }

  // ---------------------------------------------------------------------------

  /** Cost of reading the seat map when there are 20,000 seats and many viewers. */
  @Test
  void seatMapReads() throws Exception {
    ConfigurableApplicationContext ctx = node(Map.of());
    try {
      SeatCommandHandler handler = ctx.getBean(SeatCommandHandler.class);
      for (int i = 0; i < 20_000; i++) {
        handler.hold("map-" + i, "h" + i, "b" + i, Duration.ofMinutes(5), null);
      }
      int clients = 32;
      long until = System.nanoTime() + 6_000_000_000L;
      List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
      AtomicInteger bytes = new AtomicInteger();
      ExecutorService pool = Executors.newFixedThreadPool(clients);
      List<Future<?>> work = new ArrayList<>();
      for (int c = 0; c < clients; c++) {
        work.add(
            pool.submit(
                () -> {
                  while (System.nanoTime() < until) {
                    long a = System.nanoTime();
                    HttpResponse<String> r =
                        HTTP.send(HttpRequest.newBuilder(URI.create(base(ctx) + "/api/seats")).build(), HttpResponse.BodyHandlers.ofString());
                    latencies.add(System.nanoTime() - a);
                    bytes.addAndGet(r.body().length());
                  }
                  return null;
                }));
      }
      for (Future<?> f : work) {
        f.get();
      }
      pool.shutdown();
      report("seatMapReads",
          String.format("GET /api/seats over 20,000 seats, %d clients: %.0f reads/sec", clients, latencies.size() / 6.0),
          latencies(latencies));
    } finally {
      ctx.close();
    }
  }

  // ---------------------------------------------------------------------------

  /** Verifying the hash chain over a log too big to hold in memory. */
  @Test
  void verifyChainLargeLog() throws Exception {
    TestPostgres.reset();
    int events = Integer.getInteger("bench.events", 600_000);
    JdbcTemplate jdbc = new JdbcTemplate(TestPostgres.dataSource());
    // One event per seat, each with a genuinely valid hash, computed by the
    // database exactly as the store does it, so the verifier has real work to do.
    jdbc.update(
        "WITH p AS (SELECT 'big-' || g AS s, jsonb_build_object('seatId', 'big-' || g, 'holdId', 'h' || g, "
            + "'buyerId', 'b' || g, 'occurredAt', '2026-09-21T12:00:00Z', 'expiresAt', '2026-09-21T12:05:00Z') AS j "
            + "FROM generate_series(1, ?) g) "
            + "INSERT INTO events (stream_id, version, type, payload, occurred_at, prev_hash, hash) "
            + "SELECT s, 1, 'SeatHeld', j, now(), repeat('0', 64), "
            + "encode(sha256(convert_to(repeat('0', 64) || '|' || s || '|1|SeatHeld|' || j::text, 'UTF8')), 'hex') FROM p",
        events);
    var verifier = new dev.turnstile.eventstore.ChainVerifier(TestPostgres.pooled(4));

    Runtime rt = Runtime.getRuntime();
    System.gc();
    long start = System.nanoTime();
    String outcome;
    try {
      var report = verifier.verify();
      outcome = "completed, " + report.events() + " events checked, intact=" + report.intact();
    } catch (OutOfMemoryError oom) {
      outcome = "OutOfMemoryError";
    }
    report("verifyChainLargeLog",
        String.format("%,d events, max heap %d MB: %s in %.1fs", events, rt.maxMemory() / 1_000_000, outcome, (System.nanoTime() - start) / 1e9));
  }
}
