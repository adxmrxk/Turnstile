package dev.turnstile.messaging;

import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Drains the outbox into a broker.
 *
 * <p>The guarantee is at-least-once, and it is worth being exact about why:
 * a row is marked published in the same transaction that read it, <em>after</em>
 * the broker acknowledged it. If the process dies between the acknowledgement and
 * the commit, the row is still unpublished and goes out again. That duplicate is
 * the price of never losing an event, and it is why every message carries an
 * {@code eventKey} for consumers to deduplicate on. "Exactly once" here means
 * exactly-once <em>effect</em>: at-least-once delivery into an idempotent
 * consumer.
 *
 * <p>Order within a seat is kept by three things together: rows are read in id
 * order, a failed publish stops the batch instead of skipping ahead, and a
 * Postgres advisory lock lets only one relay drain at a time, so two instances
 * cannot interleave a seat's events.
 */
public final class OutboxRelay {

  /** Arbitrary, but fixed: every relay must contend for the same lock. */
  private static final long ADVISORY_LOCK = 0x7475726E7374696CL;

  private record Row(long id, String streamId, String eventKey, String payload) {}

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final EventPublisher publisher;
  private final int batchSize;

  public OutboxRelay(DataSource dataSource, EventPublisher publisher, int batchSize) {
    this.jdbc = new JdbcTemplate(dataSource);
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    this.publisher = publisher;
    this.batchSize = batchSize;
  }

  /** Publishes one batch. @return how many events were acknowledged and marked. */
  public int pollOnce() {
    Integer published =
        tx.execute(
            status -> {
              Boolean locked =
                  jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class, ADVISORY_LOCK);
              if (!Boolean.TRUE.equals(locked)) {
                return 0; // another relay is draining
              }
              List<Row> rows =
                  jdbc.query(
                      "SELECT id, stream_id, event_key, payload::text AS payload FROM outbox "
                          + "WHERE published_at IS NULL ORDER BY id LIMIT ?",
                      (rs, i) ->
                          new Row(
                              rs.getLong("id"),
                              rs.getString("stream_id"),
                              rs.getString("event_key"),
                              rs.getString("payload")),
                      batchSize);

              List<Long> acknowledged = new ArrayList<>();
              for (Row row : rows) {
                try {
                  publisher.publish(row.streamId(), row.eventKey(), row.payload());
                } catch (RuntimeException notDelivered) {
                  break; // stop here: skipping ahead would reorder this seat's events
                }
                acknowledged.add(row.id());
              }
              for (long id : acknowledged) {
                jdbc.update("UPDATE outbox SET published_at = now() WHERE id = ?", id);
              }
              return acknowledged.size();
            });
    return published == null ? 0 : published;
  }

  /**
   * Deletes rows that were published more than {@code retention} ago, a bounded
   * batch at a time. Rows still waiting to be published are never deleted, however
   * old: an unpublished row is an announcement the system still owes. Without
   * pruning this table grows by one row per event, forever.
   */
  public int prune(java.time.Duration retention) {
    return jdbc.update(
        "DELETE FROM outbox WHERE id IN (SELECT id FROM outbox WHERE published_at IS NOT NULL "
            + "AND published_at < now() - make_interval(secs => ?) LIMIT 10000)",
        retention.toMillis() / 1000.0);
  }

  public long pending() {
    Long n = jdbc.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL", Long.class);
    return n == null ? 0 : n;
  }
}
