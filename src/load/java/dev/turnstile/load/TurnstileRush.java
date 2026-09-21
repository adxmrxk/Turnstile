package dev.turnstile.load;

import static io.gatling.javaapi.core.CoreDsl.atOnceUsers;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Every buyer arrives at the same instant and tries to buy a seat over HTTP.
 *
 * <p>Gatling only generates the load. It does not decide whether the system was
 * right: that is the job of the independent verifier, run over the exported log
 * afterwards (see {@code turnstilectl load}). What Gatling asserts is narrower:
 * every request got a well-formed answer, and only the answers the protocol
 * allows (created, taken, payment failed), never an error.
 *
 * <p>Neighbouring buyers share a seat, so the crowd collides on each one.
 *
 * <p>Parameters: {@code -Dbase=http://localhost:8080}, {@code -Dusers=20000},
 * {@code -Dseats=2000}, {@code -Dramp=SECONDS} (0 = all at once).
 */
public class TurnstileRush extends Simulation {

  private static final String BASE = System.getProperty("base", "http://localhost:8080");
  private static final int USERS = Integer.getInteger("users", 20_000);
  private static final int SEATS = Integer.getInteger("seats", 2_000);
  // 0 releases everyone in one instant; N spreads the arrivals over N seconds.
  private static final int RAMP_SECONDS = Integer.getInteger("ramp", 0);

  private final AtomicLong next = new AtomicLong();

  // A run id keeps repeated runs against one server from reusing seat ids.
  private final String run = Long.toString(System.currentTimeMillis() % 100_000_000L, 36);

  private final Iterator<Map<String, Object>> buyers =
      Stream.generate(
              (Supplier<Map<String, Object>>)
                  () -> {
                    long n = next.getAndIncrement();
                    long seat = n * SEATS / USERS;
                    return Map.<String, Object>of("buyer", "b" + run + "-" + n, "seat", "load-" + run + "-" + seat);
                  })
          .iterator();

  private final HttpProtocolBuilder protocol =
      http.baseUrl(BASE)
          .acceptHeader("application/json")
          .contentTypeHeader("application/json")
          // One connection per simulated buyer exhausts the load generator's own
          // ephemeral ports (16,384 on Windows) long before it stresses the server,
          // so buyers share a pool of keep-alive connections instead.
          .shareConnections()
          .maxConnectionsPerHost(500);

  private final ScenarioBuilder rush =
      scenario("everyone at once")
          .feed(buyers)
          .exec(
              http("buy a seat")
                  .post("/api/purchases")
                  .header("Idempotency-Key", "#{buyer}")
                  .body(StringBody("{\"seatId\":\"#{seat}\",\"buyerId\":\"#{buyer}\"}"))
                  // 201 bought, 409 seat taken, 402 payment failed. Anything else is a fault.
                  .check(status().in(201, 409, 402)));

  {
    setUp(rush.injectOpen(RAMP_SECONDS == 0 ? atOnceUsers(USERS) : rampUsers(USERS).during(RAMP_SECONDS)))
        .protocols(protocol)
        .assertions(global().failedRequests().count().is(0L));
  }
}
