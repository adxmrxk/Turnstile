package dev.turnstile.eventstore;

import dev.turnstile.domain.DomainEvent;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import javax.sql.DataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The event store on PostgreSQL.
 *
 * <p>It makes the same two guarantees as {@link InMemoryEventStore}, and the
 * database does the work that the in-memory store fakes with
 * {@code ConcurrentHashMap.compute}:
 *
 * <ul>
 *   <li><b>Compare-and-append</b> is the unique constraint on
 *       {@code (stream_id, version)}. Two writers that decided from the same
 *       version both insert the next one and the database lets exactly one
 *       through. The version is also checked up front, which is what rejects a
 *       writer whose expected version is <em>ahead</em> of the stream.
 *   <li><b>Idempotency</b> is a primary key on the key, claimed with
 *       {@code INSERT ... ON CONFLICT DO NOTHING} inside the append's own
 *       transaction. A concurrent claimant blocks on that row until the owner
 *       commits or rolls back, so there is no pending state to spin on: a key is
 *       either committed together with its events or does not exist.
 * </ul>
 *
 * <p>Every event also writes an outbox row in the same transaction, so an event
 * is announced if and only if it committed.
 */
public final class PostgresEventStore implements EventStore {

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final EventCodec codec;

  public PostgresEventStore(DataSource dataSource, EventCodec codec) {
    this.jdbc = new JdbcTemplate(dataSource);
    this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    this.codec = codec;
  }

  @Override
  public List<StoredEvent> load(String streamId) {
    return jdbc.query(
        "SELECT stream_id, version, global_seq, type, payload::text AS payload "
            + "FROM events WHERE stream_id = ? ORDER BY version",
        (rs, i) ->
            new StoredEvent(
                rs.getString("stream_id"),
                rs.getLong("version"),
                rs.getLong("global_seq"),
                codec.fromJson(rs.getString("type"), rs.getString("payload"))),
        streamId);
  }

  @Override
  public long currentVersion(String streamId) {
    Long version =
        jdbc.queryForObject(
            "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?", Long.class, streamId);
    return version == null ? 0 : version;
  }

  @Override
  public OptionalLong versionForIdempotencyKey(String idempotencyKey) {
    if (idempotencyKey == null) {
      return OptionalLong.empty();
    }
    List<Long> found =
        jdbc.queryForList(
            "SELECT version FROM idempotency_keys WHERE idempotency_key = ?",
            Long.class,
            idempotencyKey);
    return found.isEmpty() ? OptionalLong.empty() : OptionalLong.of(found.get(0));
  }

  @Override
  public AppendResult append(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {

    if (events.isEmpty()) {
      return new AppendResult(currentVersion(streamId), false);
    }

    try {
      AppendResult result =
          tx.execute(status -> appendInTransaction(streamId, expectedVersion, events, idempotencyKey));
      if (result == null) {
        throw new IllegalStateException("append produced no result");
      }
      return result;
    } catch (DuplicateKeyException lostTheRace) {
      // Another writer inserted the version this one was about to. The
      // transaction has rolled back, which also released any idempotency claim.
      throw new ConcurrencyConflictException(streamId, expectedVersion, currentVersion(streamId));
    }
  }

  private AppendResult appendInTransaction(
      String streamId, long expectedVersion, List<DomainEvent> events, String idempotencyKey) {

    // Dedupe first, ahead of the version check, for the same reason the handler
    // dedupes before deciding: a retry carries the version it originally read and
    // must be answered from the record, not punished with a conflict.
    if (idempotencyKey != null) {
      int claimed =
          jdbc.update(
              "INSERT INTO idempotency_keys (idempotency_key, stream_id, version) "
                  + "VALUES (?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING",
              idempotencyKey,
              streamId,
              // The final version is known up front, so the key is written once and
              // never updated. If this append fails, the transaction rolls back and
              // takes the key with it, exactly as before.
              expectedVersion + events.size());
      if (claimed == 0) {
        Long version =
            jdbc.queryForObject(
                "SELECT version FROM idempotency_keys WHERE idempotency_key = ?",
                Long.class,
                idempotencyKey);
        return new AppendResult(version == null ? 0 : version, true);
      }
    }

    // One query for both the stream's current version and its head hash.
    List<Object[]> head =
        jdbc.query(
            "SELECT version, hash FROM events WHERE stream_id = ? ORDER BY version DESC LIMIT 1",
            (rs, i) -> new Object[] {rs.getLong(1), rs.getString(2)},
            streamId);
    long actual = head.isEmpty() ? 0 : (Long) head.get(0)[0];
    if (actual != expectedVersion) {
      throw new ConcurrencyConflictException(streamId, expectedVersion, actual);
    }

    // Chain each event to the one before it. The hash is computed by the database
    // inside the INSERT, over the payload as jsonb normalises it, because the
    // append-only trigger forbids filling it in with an UPDATE afterwards.
    // ChainVerifier recomputes the same value in Java, so the two agree or it shows.
    String previousHash =
        head.isEmpty() || head.get(0)[1] == null ? ChainVerifier.GENESIS : (String) head.get(0)[1];
    List<Object[]> outboxRows = new java.util.ArrayList<>(events.size());
    long version = expectedVersion;
    for (DomainEvent event : events) {
      version++;
      String type = codec.typeOf(event);
      String[] written =
          jdbc.queryForObject(
              "WITH p AS (SELECT ?::jsonb AS payload) "
                  + "INSERT INTO events (stream_id, version, type, payload, occurred_at, prev_hash, hash) "
                  + "SELECT ?, ?, ?, p.payload, ?, ?, "
                  + "  encode(sha256(convert_to(CAST(? AS TEXT) || '|' || CAST(? AS TEXT) || '|' "
                  + "    || CAST(? AS TEXT) || '|' || CAST(? AS TEXT) || '|' || p.payload::text, 'UTF8')), 'hex') "
                  + "FROM p RETURNING global_seq::text, hash",
              (rs, i) -> new String[] {rs.getString(1), rs.getString(2)},
              codec.toJson(event),
              streamId,
              version,
              type,
              Timestamp.from(occurredAt(event)),
              previousHash,
              previousHash,
              streamId,
              Long.toString(version),
              type);
      if (written == null) {
        throw new IllegalStateException("insert returned nothing");
      }
      previousHash = written[1];

      outboxRows.add(
          new Object[] {
            streamId + ":" + version,
            streamId,
            codec.envelope(streamId, version, Long.parseLong(written[0]), event)
          });
    }

    // All of this append's outbox rows in one round trip.
    jdbc.batchUpdate(
        "INSERT INTO outbox (event_key, stream_id, payload) VALUES (?, ?, ?::jsonb)", outboxRows);
    return new AppendResult(version, false);
  }

  /**
   * Streams the log through a database cursor. PostgreSQL only uses a cursor
   * inside a transaction with a fetch size set, which is why this is a read-only
   * transaction and a dedicated template: without both, the driver quietly reads
   * the whole result set into memory, which is the very thing this avoids.
   */
  @Override
  public void forEachEvent(java.util.function.Consumer<StoredEvent> sink) {
    JdbcTemplate streaming = new JdbcTemplate(jdbc.getDataSource());
    streaming.setFetchSize(2_000);
    TransactionTemplate readOnly = new TransactionTemplate(tx.getTransactionManager());
    readOnly.setReadOnly(true);
    readOnly.executeWithoutResult(
        status ->
            streaming.query(
                "SELECT stream_id, version, global_seq, type, payload::text AS payload "
                    + "FROM events ORDER BY global_seq",
                (org.springframework.jdbc.core.RowCallbackHandler)
                    rs ->
                        sink.accept(
                            new StoredEvent(
                                rs.getString("stream_id"),
                                rs.getLong("version"),
                                rs.getLong("global_seq"),
                                codec.fromJson(rs.getString("type"), rs.getString("payload"))))));
  }

  @Override
  public List<StoredEvent> readAll() {
    return new ArrayList<>(
        jdbc.query(
            "SELECT stream_id, version, global_seq, type, payload::text AS payload "
                + "FROM events ORDER BY global_seq",
            (rs, i) ->
                new StoredEvent(
                    rs.getString("stream_id"),
                    rs.getLong("version"),
                    rs.getLong("global_seq"),
                    codec.fromJson(rs.getString("type"), rs.getString("payload")))));
  }

  private static Instant occurredAt(DomainEvent event) {
    return event.occurredAt();
  }
}
