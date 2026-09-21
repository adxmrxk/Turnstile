package dev.turnstile.web;

import dev.turnstile.audit.LogAuditor;
import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.NotifyingEventStore;
import dev.turnstile.query.SeatQueries;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import dev.turnstile.security.Access;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.stereotype.Controller;

/** Seat-map queries over GraphQL: ask for exactly the fields you need. */
@Controller
public class SeatGraphQl {

  public record Seat(String id, String status, String holdId, int eventsApplied) {}

  public record SeatEvent(long version, String type, String occurredAt, String holdId) {}

  public record Stats(long available, long held, long sold, long total) {}

  private final SeatMapProjection seatMap;
  private final SeatQueries queries;
  private final NotifyingEventStore store;
  private final Access access;

  public SeatGraphQl(
      SeatMapProjection seatMap, SeatQueries queries, NotifyingEventStore store, Access access) {
    this.seatMap = seatMap;
    this.queries = queries;
    this.store = store;
    this.access = access;
  }

  @QueryMapping
  public Seat seat(@Argument String id, @Argument String asOf) {
    Ids.require("id", id);
    SeatQueries.SeatState state;
    if (asOf == null) {
      state = queries.current(id);
    } else {
      try {
        state = queries.asOf(id, Instant.parse(asOf));
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("asOf must be an ISO-8601 instant");
      }
    }
    if (state.eventsApplied() == 0 && asOf == null) {
      return null; // a seat that has never had an event does not exist yet
    }
    return new Seat(state.seatId(), state.status().name(), state.holdId(), (int) state.eventsApplied());
  }

  @QueryMapping
  public List<Seat> seats(@Argument String status, @Argument Integer limit) {
    int cap = limit == null ? 100 : Math.max(1, Math.min(limit, 1000));
    return seatMap.all().stream()
        .filter(v -> status == null || v.status().equals(status))
        .limit(cap)
        .map(SeatGraphQl::of)
        .toList();
  }

  @QueryMapping
  public Stats stats() {
    Map<String, Long> counts = seatMap.counts();
    long available = counts.getOrDefault("AVAILABLE", 0L);
    long held = counts.getOrDefault("HELD", 0L);
    long sold = counts.getOrDefault("SOLD", 0L);
    return new Stats(available, held, sold, available + held + sold);
  }

  @QueryMapping
  public LogAuditor.Report audit() throws IOException {
    access.requireStaff();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NdjsonExport.write(store.readAll(), out);
    List<String> lines = Arrays.asList(out.toString(StandardCharsets.UTF_8).split("\n"));
    return new LogAuditor().audit(lines);
  }

  @SchemaMapping(typeName = "Seat", field = "history")
  public List<SeatEvent> history(Seat seat) {
    access.requireStaff();
    return queries.history(seat.id()).stream()
        .map(
            e -> {
              DomainEvent event = e.event();
              String hold =
                  event instanceof DomainEvent.SeatHeld h
                      ? h.holdId()
                      : event instanceof DomainEvent.HoldReleased r
                          ? r.holdId()
                          : ((DomainEvent.SeatSold) event).holdId();
              return new SeatEvent(
                  e.version(), event.getClass().getSimpleName(), event.occurredAt().toString(), hold);
            })
        .toList();
  }

  private static Seat of(SeatView v) {
    return new Seat(v.seatId(), v.status(), v.holdId(), (int) v.version());
  }
}
