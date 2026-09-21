package dev.turnstile.web;

import dev.turnstile.eventstore.StoredEvent;
import dev.turnstile.query.SeatQueries;
import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Reading the seat map, and asking the log what a seat looked like at any moment. */
@RestController
@RequestMapping("/api")
public class SeatController {

  public record SeatState(String seatId, String status, String holdId, long eventsApplied, String asOf) {}

  public record HistoryEntry(long version, long seq, String type, Object event) {}

  private final SeatMapProjection seatMap;
  private final SeatQueries queries;

  public SeatController(SeatMapProjection seatMap, SeatQueries queries) {
    this.seatMap = seatMap;
    this.queries = queries;
  }

  /** The fast read model: every seat that has had an event. */
  @GetMapping("/seats")
  public List<SeatView> seats() {
    return seatMap.all();
  }

  @GetMapping("/stats")
  public Map<String, Long> stats() {
    return seatMap.counts();
  }

  /**
   * A seat's state, from the log itself. With {@code asOf} it is the state the
   * seat had at that instant, derived by replaying only what had happened by then.
   */
  @GetMapping("/seats/{seatId}")
  public SeatState seat(@PathVariable String seatId, @RequestParam(required = false) String asOf) {
    Ids.require("seatId", seatId);
    Instant at = null;
    if (asOf != null) {
      try {
        at = Instant.parse(asOf);
      } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("asOf must be an ISO-8601 instant, e.g. 2026-09-21T12:00:00Z");
      }
    }
    SeatQueries.SeatState state = at == null ? queries.current(seatId) : queries.asOf(seatId, at);
    return new SeatState(
        state.seatId(), state.status().name(), state.holdId(), state.eventsApplied(), at == null ? null : at.toString());
  }

  /** Every event this seat ever had. Staff only when security is on. */
  @GetMapping("/seats/{seatId}/history")
  public List<HistoryEntry> history(@PathVariable String seatId) {
    Ids.require("seatId", seatId);
    return queries.history(seatId).stream().map(SeatController::entry).toList();
  }

  private static HistoryEntry entry(StoredEvent e) {
    return new HistoryEntry(e.version(), e.globalSequence(), e.event().getClass().getSimpleName(), e.event());
  }
}
