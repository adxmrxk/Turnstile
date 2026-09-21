package dev.turnstile.eventstore;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
 *
 * <p>The log is read through a database cursor, one row at a time, so a check
 * needs memory for one seat, not for the log. (Loading it whole ran out of memory
 * at 600,000 events on a 300 MB heap, which is when an audit is most needed.) A
 * checkpoint check additionally holds the checkpoint's own heads, one per seat.
 */
public final class ChainVerifier {

  public static final String GENESIS = "0".repeat(64);

  /** A badly tampered log could produce a problem per event; report the first of them. */
  static final int MAX_PROBLEMS = 1_000;

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

  private final JdbcTemplate streaming;
  private final TransactionTemplate readOnly;

  public ChainVerifier(DataSource dataSource) {
    this.streaming = new JdbcTemplate(dataSource);
    // PostgreSQL only uses a cursor inside a transaction with a fetch size set;
    // without both the driver reads the whole result into memory.
    this.streaming.setFetchSize(2_000);
    this.readOnly = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    this.readOnly.setReadOnly(true);
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

  /** Every event, seat by seat and in version order, streamed. */
  private void forEachRow(Consumer<Row> sink) {
    readOnly.executeWithoutResult(
        status ->
            streaming.query(
                "SELECT stream_id, version, type, payload::text AS payload, prev_hash, hash "
                    + "FROM events ORDER BY stream_id, version",
                (RowCallbackHandler)
                    rs ->
                        sink.accept(
                            new Row(
                                rs.getString("stream_id"),
                                rs.getLong("version"),
                                rs.getString("type"),
                                rs.getString("payload"),
                                rs.getString("prev_hash"),
                                rs.getString("hash")))));
  }

  public Report verify() {
    return scan(null);
  }

  /**
   * One pass over the log. Checks each seat's chain and, when a checkpoint is
   * given, that every head it recorded is still present with the same hash.
   */
  private Report scan(Checkpoint checkpoint) {
    List<Problem> problems = new ArrayList<>();
    long[] dropped = {0};
    long[] counts = {0, 0}; // events, streams
    String[] current = {null};
    String[] expectedPrev = {GENESIS};
    long[] expectedVersion = {1};
    Set<String> headsSeen = new HashSet<>();

    java.util.function.BiConsumer<Row, String> report =
        (row, what) -> {
          if (problems.size() < MAX_PROBLEMS) {
            problems.add(new Problem(row.streamId(), row.version(), what));
          } else {
            dropped[0]++;
          }
        };

    forEachRow(
        row -> {
          counts[0]++;
          if (!row.streamId().equals(current[0])) {
            current[0] = row.streamId();
            expectedPrev[0] = GENESIS;
            expectedVersion[0] = 1;
            counts[1]++;
          }

          if (checkpoint != null) {
            Head head = checkpoint.heads().get(row.streamId());
            if (head != null && head.version() == row.version()) {
              headsSeen.add(row.streamId());
              if (!Objects.equals(head.hash(), row.hash())) {
                report.accept(row, "the checkpoint recorded a different hash here (rewritten)");
              }
            }
          }

          if (row.hash() == null || row.prev() == null) {
            report.accept(row, "event was never chained");
            expectedPrev[0] = null;
            expectedVersion[0] = row.version() + 1;
            return;
          }
          if (row.version() != expectedVersion[0]) {
            report.accept(row, "expected version " + expectedVersion[0] + ": an event is missing or out of order");
          }
          if (expectedPrev[0] != null && !row.prev().equals(expectedPrev[0])) {
            report.accept(row, "does not follow the previous event's hash");
          }
          String recomputed = hash(row.prev(), row.streamId(), row.version(), row.type(), row.payload());
          if (!recomputed.equals(row.hash())) {
            report.accept(row, "contents do not match the stored hash (edited)");
          }
          expectedPrev[0] = row.hash();
          expectedVersion[0] = row.version() + 1;
        });

    if (checkpoint != null) {
      checkpoint.heads().forEach(
          (stream, head) -> {
            if (!headsSeen.contains(stream)) {
              problems.add(new Problem(stream, head.version(), "the checkpoint recorded this event but it is gone (truncated)"));
            }
          });
    }
    if (dropped[0] > 0) {
      problems.add(new Problem("*", 0, dropped[0] + " further problems were found and not listed"));
    }
    return new Report(counts[0], counts[1], problems);
  }

  /** Summarises every seat's head right now. */
  public Checkpoint checkpoint() {
    Map<String, Head> heads = new TreeMap<>();
    forEachRow(row -> heads.put(row.streamId(), new Head(row.version(), row.hash())));
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
    return scan(checkpoint);
  }
}
