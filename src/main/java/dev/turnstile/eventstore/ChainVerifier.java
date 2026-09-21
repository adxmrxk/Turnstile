package dev.turnstile.eventstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Checks that the event log has not been rewritten.
 *
 * <p>Each event stores the hash of its seat's previous event and a hash of itself,
 * so a seat's events form a chain. {@link #verify} walks every chain and reports:
 * an event whose contents no longer match its hash (edited), a link that does not
 * point at the event before it (something removed or reordered), a gap in the
 * version numbers, and events that were never chained at all.
 *
 * <p>The chain alone has two blind spots, and both are the reason
 * {@link Checkpoint} exists. Someone who edits an event can recompute every hash
 * after it, since the algorithm is not secret, and the chain then looks perfect. And
 * someone who deletes the <em>last</em> events of a seat leaves a shorter but
 * perfectly valid chain. A checkpoint is a summary of every seat's head, taken at
 * one moment and kept <em>somewhere the database's owner cannot reach</em>. Checked
 * later, it catches both: the head it recorded must still be there, with the same
 * hash. Without an external copy of a checkpoint the guarantee is weaker, and that
 * limit is inherent, not a bug.
 */
public final class ChainVerifier {

  public static final String GENESIS = "0".repeat(64);

  public record Problem(String streamId, long version, String what) {}

  public record Report(long events, long streams, List<Problem> problems) {
    @JsonProperty("intact")
    public boolean intact() {
      return problems.isEmpty();
    }
  }

  public record Head(long version, String hash) {}

  /** Every seat's head at one moment, and a digest of them. Store it outside the database. */
  public record Checkpoint(long events, Map<String, Head> heads, String root) {}

  private final JdbcTemplate jdbc;

  public ChainVerifier(DataSource dataSource) {
    this.jdbc = new JdbcTemplate(dataSource);
  }

  /** The hash of one event. The database computes the same value with sha256(). */
  public static String hash(String prevHash, String streamId, long version, String type, String payloadText) {
    return sha256Hex(prevHash + "|" + streamId + "|" + version + "|" + type + "|" + payloadText);
  }

  private static String sha256Hex(String input) {
    try {
      return HexFormat.of()
          .formatHex(MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private record Row(String streamId, long version, String type, String payload, String prev, String hash) {}

  private List<Row> rows() {
    return jdbc.query(
        "SELECT stream_id, version, type, payload::text AS payload, prev_hash, hash "
            + "FROM events ORDER BY stream_id, version",
        (rs, i) ->
            new Row(
                rs.getString("stream_id"),
                rs.getLong("version"),
                rs.getString("type"),
                rs.getString("payload"),
                rs.getString("prev_hash"),
                rs.getString("hash")));
  }

  public Report verify() {
    List<Row> all = rows();
    List<Problem> problems = new ArrayList<>();
    String currentStream = null;
    String expectedPrev = GENESIS;
    long expectedVersion = 1;
    long streams = 0;

    for (Row row : all) {
      if (!row.streamId().equals(currentStream)) {
        currentStream = row.streamId();
        expectedPrev = GENESIS;
        expectedVersion = 1;
        streams++;
      }
      if (row.hash() == null || row.prev() == null) {
        problems.add(new Problem(row.streamId(), row.version(), "event was never chained"));
        expectedPrev = null;
        expectedVersion = row.version() + 1;
        continue;
      }
      if (row.version() != expectedVersion) {
        problems.add(
            new Problem(
                row.streamId(), row.version(),
                "expected version " + expectedVersion + ": an event is missing or out of order"));
      }
      if (expectedPrev != null && !row.prev().equals(expectedPrev)) {
        problems.add(new Problem(row.streamId(), row.version(), "does not follow the previous event's hash"));
      }
      String recomputed = hash(row.prev(), row.streamId(), row.version(), row.type(), row.payload());
      if (!recomputed.equals(row.hash())) {
        problems.add(new Problem(row.streamId(), row.version(), "contents do not match the stored hash (edited)"));
      }
      expectedPrev = row.hash();
      expectedVersion = row.version() + 1;
    }
    return new Report(all.size(), streams, problems);
  }

  /** Summarises every seat's head right now. */
  public Checkpoint checkpoint() {
    Map<String, Head> heads = new TreeMap<>();
    for (Row row : rows()) {
      heads.put(row.streamId(), new Head(row.version(), row.hash()));
    }
    StringBuilder digestInput = new StringBuilder();
    heads.forEach((stream, head) -> digestInput.append(stream).append(':').append(head.version()).append(':')
        .append(head.hash()).append('\n'));
    long events = heads.values().stream().mapToLong(Head::version).sum();
    return new Checkpoint(events, heads, sha256Hex(digestInput.toString()));
  }

  /**
   * The chain check, plus: every head the checkpoint recorded must still be in the
   * log with the same hash. This is what catches a wholesale rewrite and a truncated
   * tail. Seats added since the checkpoint are fine; anything it recorded must remain.
   */
  public Report verifyAgainst(Checkpoint checkpoint) {
    Report chain = verify();
    List<Problem> problems = new ArrayList<>(chain.problems());
    Map<String, Map<Long, String>> hashesByStream = new TreeMap<>();
    for (Row row : rows()) {
      hashesByStream.computeIfAbsent(row.streamId(), k -> new TreeMap<>()).put(row.version(), row.hash());
    }
    checkpoint.heads().forEach(
        (stream, head) -> {
          String now = hashesByStream.getOrDefault(stream, Map.of()).get(head.version());
          if (now == null) {
            problems.add(new Problem(stream, head.version(), "the checkpoint recorded this event but it is gone (truncated)"));
          } else if (!now.equals(head.hash())) {
            problems.add(new Problem(stream, head.version(), "the checkpoint recorded a different hash here (rewritten)"));
          }
        });
    return new Report(chain.events(), chain.streams(), problems);
  }
}
