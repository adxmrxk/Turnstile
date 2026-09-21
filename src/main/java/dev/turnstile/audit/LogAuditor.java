package dev.turnstile.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Re-derives the no-oversell invariants from the exported log as plain text.
 *
 * <p>This is the in-process twin of {@code scripts/verify-invariants.sh}, so the
 * running server can answer "is my own log clean?" on demand. It deliberately
 * knows nothing about seats, aggregates or the event store: it reads NDJSON lines
 * and applies the same rules the awk script does, and a test runs the two side by
 * side on the same logs and requires them to agree. Two independent checkers that
 * agree are worth more than one that is trusted.
 *
 * <ul>
 *   <li>I1 no seat is sold more than once
 *   <li>I2 every sale descends from a hold placed on that seat
 *   <li>I3 nothing happens to a seat after it is sold
 * </ul>
 */
public final class LogAuditor {

  public record Report(
      long events, long seatsSold, long openHolds, List<String> violations, boolean clean) {}

  private static final ObjectMapper JSON = new ObjectMapper();

  public Report audit(Iterable<String> ndjsonLines) {
    Map<String, Long> soldAtLine = new HashMap<>();
    Set<String> placedHolds = new HashSet<>();
    Set<String> openHolds = new HashSet<>();
    List<String> violations = new ArrayList<>();
    long events = 0;
    long line = 0;
    long sold = 0;

    for (String raw : ndjsonLines) {
      line++;
      if (raw.isBlank()) {
        continue;
      }
      events++;
      String type;
      String seat;
      String hold;
      try {
        JsonNode node = JSON.readTree(raw);
        type = text(node, "type");
        seat = text(node, "seatId");
        hold = text(node, "holdId");
      } catch (Exception unparseable) {
        violations.add("E0 line " + line + ": unparseable event");
        continue;
      }
      if (type.isEmpty() || seat.isEmpty()) {
        violations.add("E0 line " + line + ": unparseable event");
        continue;
      }

      String pair = seat + "\u0000" + hold;
      if (soldAtLine.containsKey(seat)) {
        violations.add("I3 line " + line + ": event " + type + " on seat " + seat + " after it was sold");
      }
      switch (type) {
        case "SeatHeld" -> {
          placedHolds.add(pair);
          openHolds.add(pair);
        }
        case "HoldReleased" -> openHolds.remove(pair);
        case "SeatSold" -> {
          if (soldAtLine.containsKey(seat)) {
            violations.add(
                "I1 line " + line + ": seat " + seat + " sold a second time (first at line "
                    + soldAtLine.get(seat) + ")");
          } else {
            soldAtLine.put(seat, line);
            sold++;
          }
          if (!placedHolds.contains(pair)) {
            violations.add("I2 line " + line + ": seat " + seat + " sold via hold " + hold + " that was never placed");
          }
          openHolds.remove(pair);
        }
        default -> {
          // An event type this auditor does not know is neither good nor bad.
        }
      }
    }
    return new Report(events, sold, openHolds.size(), violations, violations.isEmpty());
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? "" : value.asText();
  }
}
