package dev.turnstile.eventstore;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.testsupport.TestPostgres;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Plays a malicious database administrator.
 *
 * <p>The append-only trigger stops ordinary SQL from editing history, but whoever
 * owns the database can switch it off. These tests do exactly that and then try to
 * rewrite the log, to see what the hash chain and the checkpoints can and cannot
 * catch. Two of the cases are the chain's known blind spots, and are asserted as
 * such: a check that quietly could not see them would be worse than one that says so.
 */
class ChainVerifierTest {

  private JdbcTemplate jdbc;
  private ChainVerifier verifier;
  private PostgresEventStore store;

  @BeforeEach
  void freshLogWithHistory() {
    TestPostgres.reset();
    jdbc = new JdbcTemplate(TestPostgres.dataSource());
    verifier = new ChainVerifier(TestPostgres.dataSource());
    store = new PostgresEventStore(TestPostgres.dataSource(), new EventCodec());
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));

    // A seat with a real history: held, abandoned, held again, sold.
    Duration ttl = Duration.ofMinutes(5);
    handler.hold("seat-A", "h1", "alice", ttl, "k1");
    handler.releaseHold("seat-A", "h1", "k2");
    handler.hold("seat-A", "h2", "bob", ttl, "k3");
    handler.confirmSale("seat-A", "h2", "o1", "k4");
    // And a few other seats.
    for (int i = 0; i < 5; i++) {
      handler.hold("seat-" + i, "h-" + i, "buyer-" + i, ttl, "hold-" + i);
      handler.confirmSale("seat-" + i, "h-" + i, "o-" + i, "sale-" + i);
    }
  }

  /** Runs SQL with the append-only trigger off, as a database owner could. */
  private void asDba(Runnable tampering) {
    jdbc.execute("ALTER TABLE events DISABLE TRIGGER events_are_append_only");
    try {
      tampering.run();
    } finally {
      jdbc.execute("ALTER TABLE events ENABLE TRIGGER events_are_append_only");
    }
  }

  @Test
  @DisplayName("an untouched log verifies, and the database and Java compute the same hashes")
  void clean_log_is_intact() {
    ChainVerifier.Report report = verifier.verify();

    assertThat(report.problems()).isEmpty();
    assertThat(report.events()).isEqualTo(store.readAll().size());
    assertThat(report.streams()).isEqualTo(6);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM events WHERE hash IS NULL", Long.class)).isZero();
  }

  @Test
  @DisplayName("editing an event's contents is caught, and pinpointed")
  void edited_event_is_detected() {
    asDba(() -> jdbc.update(
        "UPDATE events SET payload = jsonb_set(payload, '{buyerId}', '\"mallory\"') "
            + "WHERE stream_id = 'seat-A' AND version = 3"));

    ChainVerifier.Report report = verifier.verify();

    assertThat(report.intact()).isFalse();
    assertThat(report.problems())
        .extracting(ChainVerifier.Problem::streamId, ChainVerifier.Problem::version)
        .as("exactly the edited event")
        .containsExactly(org.assertj.core.groups.Tuple.tuple("seat-A", 3L));
    assertThat(report.problems().get(0).what()).contains("edited");
  }

  @Test
  @DisplayName("deleting an event from the middle is caught")
  void deleted_event_is_detected() {
    asDba(() -> jdbc.update("DELETE FROM events WHERE stream_id = 'seat-A' AND version = 2"));

    ChainVerifier.Report report = verifier.verify();

    assertThat(report.intact()).isFalse();
    assertThat(report.problems()).extracting(ChainVerifier.Problem::what)
        .anyMatch(w -> w.contains("missing or out of order"))
        .anyMatch(w -> w.contains("does not follow"));
  }

  @Test
  @DisplayName("an event inserted with no hash is reported as unchained, not trusted")
  void unchained_event_is_reported() {
    jdbc.update(
        "INSERT INTO events (stream_id, version, type, payload, occurred_at) "
            + "VALUES ('seat-9', 1, 'SeatHeld', "
            + "'{\"seatId\":\"seat-9\",\"holdId\":\"x\",\"buyerId\":\"b\",\"occurredAt\":\"2026-01-01T00:00:00Z\","
            + "\"expiresAt\":\"2026-01-01T00:05:00Z\"}'::jsonb, now())");

    assertThat(verifier.verify().problems())
        .extracting(ChainVerifier.Problem::what)
        .contains("event was never chained");
  }

  @Test
  @DisplayName("BLIND SPOT: an attacker who recomputes the hashes defeats the chain, but not a checkpoint")
  void wholesale_rewrite_fools_the_chain_but_not_a_checkpoint() {
    ChainVerifier.Checkpoint checkpoint = verifier.checkpoint(); // kept outside the database

    asDba(
        () -> {
          jdbc.update(
              "UPDATE events SET payload = jsonb_set(payload, '{buyerId}', '\"mallory\"') "
                  + "WHERE stream_id = 'seat-A' AND version = 1");
          // The algorithm is not secret, so the attacker rebuilds the whole chain.
          String prev = ChainVerifier.GENESIS;
          List<Map<String, Object>> rows =
              jdbc.queryForList(
                  "SELECT version, type, payload::text AS payload FROM events "
                      + "WHERE stream_id = 'seat-A' ORDER BY version");
          for (Map<String, Object> row : rows) {
            long version = ((Number) row.get("version")).longValue();
            String hash =
                ChainVerifier.hash(prev, "seat-A", version, (String) row.get("type"), (String) row.get("payload"));
            jdbc.update(
                "UPDATE events SET prev_hash = ?, hash = ? WHERE stream_id = 'seat-A' AND version = ?",
                prev, hash, version);
            prev = hash;
          }
        });

    assertThat(verifier.verify().intact())
        .as("the chain alone is fooled by a full recompute; this is the known blind spot")
        .isTrue();

    ChainVerifier.Report caught = verifier.verifyAgainst(checkpoint);
    assertThat(caught.intact()).as("but the checkpoint taken earlier is not").isFalse();
    assertThat(caught.problems()).extracting(ChainVerifier.Problem::what)
        .anyMatch(w -> w.contains("rewritten"));
  }

  @Test
  @DisplayName("BLIND SPOT: deleting the newest events leaves a valid chain, but not a valid checkpoint")
  void truncated_tail_fools_the_chain_but_not_a_checkpoint() {
    ChainVerifier.Checkpoint checkpoint = verifier.checkpoint();

    // Erase the sale: the seat quietly becomes available again.
    asDba(() -> jdbc.update("DELETE FROM events WHERE stream_id = 'seat-A' AND version = 4"));

    assertThat(verifier.verify().intact())
        .as("a shorter chain is still a valid chain; this is the known blind spot")
        .isTrue();

    ChainVerifier.Report caught = verifier.verifyAgainst(checkpoint);
    assertThat(caught.problems()).extracting(ChainVerifier.Problem::what)
        .anyMatch(w -> w.contains("gone (truncated)"));
  }

  @Test
  @DisplayName("a checkpoint stays valid as new events arrive, and its digest depends on every seat")
  void checkpoint_survives_growth_and_changes_with_content() {
    ChainVerifier.Checkpoint before = verifier.checkpoint();

    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    handler.hold("seat-new", "hn", "nina", Duration.ofMinutes(5), "kn");

    assertThat(verifier.verifyAgainst(before).intact()).as("growth is not tampering").isTrue();
    assertThat(verifier.checkpoint().root()).as("a new event changes the digest").isNotEqualTo(before.root());
    assertThat(verifier.checkpoint().root()).as("and it is deterministic").isEqualTo(verifier.checkpoint().root());
  }

  @Test
  @DisplayName("a wrecked log reports the first problems and says how many more it left out")
  void problem_list_is_capped() {
    // 1,200 events with no hash at all: each is a problem.
    jdbc.update(
        "INSERT INTO events (stream_id, version, type, payload, occurred_at) "
            + "SELECT 'wreck-' || g, 1, 'SeatHeld', '{}'::jsonb, now() FROM generate_series(1, 1200) g");

    ChainVerifier.Report report = verifier.verify();

    assertThat(report.intact()).isFalse();
    assertThat(report.problems()).as("the cap plus one summary line").hasSize(ChainVerifier.MAX_PROBLEMS + 1);
    assertThat(report.problems().get(ChainVerifier.MAX_PROBLEMS).what())
        .as("the summary says what was left out")
        .contains("200 further problems");
    assertThat(report.events()).as("every event was still examined").isEqualTo(store.readAll().size());
  }
}
