package dev.turnstile.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.turnstile.readmodel.SeatMapProjection;
import java.util.zip.CRC32;
import org.springframework.stereotype.Component;

/**
 * The seat map, already serialised.
 *
 * <p>Building the response used to mean copying, sorting and serialising every
 * seat on every request. At 20,000 seats that took about 600 ms and topped out near
 * 50 requests a second, and the seat map is exactly what a crowd of buyers polls.
 * The snapshot is rebuilt only when the map has changed, and at least once a second
 * regardless, because a hold that lapses changes what the map should say without any
 * event to announce it. So a reader can be at most about a second behind an expiry,
 * and no behind at all on a real change.
 *
 * <p>Concurrent readers that find it stale wait for one rebuild instead of each
 * doing their own.
 */
@Component
public class SeatMapSnapshot {

  /** How stale a snapshot may get with no change at all, to let lapsed holds show. */
  private static final long MAX_AGE_MILLIS = 1_000;

  public record Snapshot(long changes, long builtAtMillis, byte[] json, String etag) {}

  private final SeatMapProjection projection;
  private final ObjectMapper mapper;
  private volatile Snapshot current;

  public SeatMapSnapshot(SeatMapProjection projection, ObjectMapper mapper) {
    this.projection = projection;
    this.mapper = mapper;
  }

  public Snapshot get() {
    Snapshot snapshot = current;
    if (fresh(snapshot)) {
      return snapshot;
    }
    synchronized (this) {
      if (!fresh(current)) {
        current = build();
      }
      return current;
    }
  }

  private boolean fresh(Snapshot s) {
    return s != null
        && s.changes() == projection.changeCount()
        && System.currentTimeMillis() - s.builtAtMillis() < MAX_AGE_MILLIS;
  }

  private Snapshot build() {
    long changes = projection.changeCount();
    try {
      byte[] json = mapper.writeValueAsBytes(projection.all());
      CRC32 crc = new CRC32();
      crc.update(json);
      String etag = "\"" + Long.toHexString(crc.getValue()) + "-" + Integer.toHexString(json.length) + "\"";
      return new Snapshot(changes, System.currentTimeMillis(), json, etag);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise the seat map", e);
    }
  }
}
