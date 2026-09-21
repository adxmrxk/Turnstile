package dev.turnstile.saga;

import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public final class JdbcSagaLog implements SagaLog {

  private static final RowMapper<SagaRecord> MAPPER =
      (rs, i) ->
          new SagaRecord(
              rs.getString("saga_id"),
              rs.getString("seat_id"),
              rs.getString("buyer_id"),
              rs.getString("hold_id"),
              rs.getString("order_id"),
              rs.getLong("amount_cents"),
              SagaRecord.State.valueOf(rs.getString("state")),
              rs.getString("detail"));

  private final JdbcTemplate jdbc;

  public JdbcSagaLog(DataSource dataSource) {
    this.jdbc = new JdbcTemplate(dataSource);
  }

  @Override
  public boolean insert(SagaRecord r) {
    return jdbc.update(
            "INSERT INTO sagas (saga_id, seat_id, buyer_id, hold_id, order_id, amount_cents, state, detail) "
                + "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (saga_id) DO NOTHING",
            r.sagaId(),
            r.seatId(),
            r.buyerId(),
            r.holdId(),
            r.orderId(),
            r.amountCents(),
            r.state().name(),
            r.detail())
        == 1;
  }

  @Override
  public void update(SagaRecord r) {
    jdbc.update(
        // A finished purchase is never overwritten: see InMemorySagaLog.update.
        "UPDATE sagas SET state = ?, detail = ?, updated_at = now() WHERE saga_id = ? "
            + "AND state NOT IN ('CONFIRMED','REFUSED','DECLINED','REFUNDED')",
        r.state().name(),
        r.detail(),
        r.sagaId());
  }

  @Override
  public Optional<SagaRecord> find(String sagaId) {
    return jdbc.query("SELECT * FROM sagas WHERE saga_id = ?", MAPPER, sagaId).stream().findFirst();
  }

  @Override
  public List<SagaRecord> incompleteOlderThan(java.time.Duration age) {
    return jdbc.query(
        "SELECT * FROM sagas WHERE state NOT IN ('CONFIRMED','REFUSED','DECLINED','REFUNDED') "
            + "AND updated_at < now() - make_interval(secs => ?) ORDER BY updated_at",
        MAPPER,
        age.toMillis() / 1000.0);
  }

  @Override
  public List<SagaRecord> incomplete() {
    return jdbc.query(
        "SELECT * FROM sagas WHERE state NOT IN ('CONFIRMED','REFUSED','DECLINED','REFUNDED') "
            + "ORDER BY updated_at",
        MAPPER);
  }
}
