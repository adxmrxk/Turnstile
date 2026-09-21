package dev.turnstile.audit;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.eventstore.StoredEvent;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The event log as newline-delimited JSON, the format {@code
 * scripts/verify-invariants.sh} audits. One writer serves the simulator, the live
 * HTTP export and the CLI, so the auditor sees the same bytes wherever the log
 * came from.
 *
 * <p>Hand-rolled on purpose: the shape is fixed, and an exporter that depends on
 * no serializer cannot have its audit trail altered by a library upgrade. Seat
 * ids can now arrive from a URL, so values are escaped; the awk verifier reads
 * escaped quotes correctly.
 */
public final class NdjsonExport {

  private NdjsonExport() {}

  /** Streams the whole log; memory does not grow with its size. */
  public static void write(dev.turnstile.eventstore.EventStore store, OutputStream target) throws IOException {
    BufferedWriter out =
        new BufferedWriter(new OutputStreamWriter(target, StandardCharsets.UTF_8), 1 << 16);
    store.forEachEvent(
        stored -> {
          try {
            out.write(line(stored));
          } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        });
    out.flush();
  }

  public static void write(List<StoredEvent> log, OutputStream target) throws IOException {
    BufferedWriter out =
        new BufferedWriter(new OutputStreamWriter(target, StandardCharsets.UTF_8), 1 << 16);
    for (StoredEvent stored : log) {
      out.write(line(stored));
    }
    out.flush();
  }

  public static String line(StoredEvent stored) {
    DomainEvent event = stored.event();
    // A literal \n and not %n, which would emit CRLF on Windows.
    return "{\"seq\":"
        + stored.globalSequence()
        + ",\"version\":"
        + stored.version()
        + ",\"type\":\""
        + event.getClass().getSimpleName()
        + "\",\"seatId\":\""
        + escape(event.seatId())
        + "\",\"holdId\":\""
        + escape(holdIdOf(event))
        + "\",\"occurredAt\":\""
        + event.occurredAt()
        + "\"}\n";
  }

  static String holdIdOf(DomainEvent event) {
    if (event instanceof DomainEvent.SeatHeld held) {
      return held.holdId();
    } else if (event instanceof DomainEvent.HoldReleased released) {
      return released.holdId();
    } else if (event instanceof DomainEvent.SeatSold sold) {
      return sold.holdId();
    }
    throw new IllegalStateException("unhandled event type: " + event.getClass().getName());
  }

  static String escape(String value) {
    StringBuilder out = new StringBuilder(value.length() + 8);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    return out.toString();
  }
}
