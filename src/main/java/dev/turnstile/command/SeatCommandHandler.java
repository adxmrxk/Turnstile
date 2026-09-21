package dev.turnstile.command;

import dev.turnstile.domain.DomainEvent;
import dev.turnstile.domain.DomainException;
import dev.turnstile.domain.SeatAggregate;
import dev.turnstile.eventstore.AppendResult;
import dev.turnstile.eventstore.ConcurrencyConflictException;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.StoredEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Function;

/**
 * Runs the read, decide, append cycle and is where a contended write is
 * resolved.
 *
 * <p>The loop is the entire concurrency story:
 *
 * <ol>
 *   <li>load the stream and note its version
 *   <li>replay it into an aggregate and ask the aggregate to decide
 *   <li>append the resulting events, but only if the stream is still at that
 *       version
 *   <li>on conflict, discard everything and start over
 * </ol>
 *
 * <p>The important part is what step 4 does <em>not</em> do. It does not retry
 * the append. It throws away the decision as well, because that decision was
 * made against a seat that has since changed. On the retry the aggregate replays
 * the winner's SeatHeld event and refuses with SeatAlreadyHeld. That is how a
 * genuine race turns into one sale and one clean rejection instead of two sales:
 * the loser is not queued behind the winner, it is re-judged against the new
 * reality.
 *
 * <p>Retries are bounded. Exhausting them means sustained contention on a single
 * seat rather than a lost update, so it surfaces as its own signal instead of
 * spinning forever.
 */
public final class SeatCommandHandler {

  /** High enough that genuine contention resolves, low enough to fail loudly. */
  static final int MAX_ATTEMPTS = 32;

  private final EventStore eventStore;
  private final Clock clock;

  public SeatCommandHandler(EventStore eventStore, Clock clock) {
    this.eventStore = eventStore;
    this.clock = clock;
  }

  public AppendResult hold(
      String seatId, String holdId, String buyerId, Duration ttl, String idempotencyKey) {
    return execute(seatId, idempotencyKey, seat -> seat.hold(holdId, buyerId, ttl));
  }

  public AppendResult confirmSale(
      String seatId, String holdId, String orderId, String idempotencyKey) {
    return execute(seatId, idempotencyKey, seat -> seat.confirmSale(holdId, orderId));
  }

  public AppendResult releaseHold(String seatId, String holdId, String idempotencyKey) {
    return execute(seatId, idempotencyKey, seat -> seat.releaseHold(holdId));
  }

  private AppendResult execute(
      String seatId, String idempotencyKey, Function<SeatAggregate, List<DomainEvent>> decision) {

    // Dedupe is checked before the aggregate is consulted, and the ordering is
    // load bearing. Deciding first and deduping at append time looks equivalent
    // and is not: a client retrying a confirmSale that already succeeded would
    // replay a stream in which the seat is SOLD, the aggregate would rightly
    // refuse with SeatAlreadySold, and that refusal would escape before the
    // append was ever reached. The buyer would be told their purchase failed
    // when it went through. A retry has to be answered from the record of what
    // happened, not re-adjudicated against a world it already changed.
    if (idempotencyKey != null) {
      OptionalLong alreadyApplied = eventStore.versionForIdempotencyKey(idempotencyKey);
      if (alreadyApplied.isPresent()) {
        return new AppendResult(alreadyApplied.getAsLong(), true);
      }
    }

    ConcurrencyConflictException lastConflict = null;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      List<StoredEvent> history = eventStore.load(seatId);
      long expectedVersion = history.size();

      SeatAggregate seat =
          SeatAggregate.replay(seatId, history.stream().map(StoredEvent::event).toList(), clock);

      // A domain refusal is a real answer, so it propagates immediately. Only a
      // stale read is worth another attempt.
      //
      // One exception: the refusal may be a reaction to this very request having
      // succeeded. Two copies of a command in flight together (a double click, a
      // retry racing its own original) can both pass the check above before
      // either has appended. The loser then reloads a seat its twin just took and
      // is refused, which would tell the buyer their purchase failed when it went
      // through. So a refusal is confirmed against the idempotency record first.
      List<DomainEvent> newEvents;
      try {
        newEvents = decision.apply(seat);
      } catch (DomainException refusal) {
        if (idempotencyKey != null) {
          OptionalLong applied = eventStore.versionForIdempotencyKey(idempotencyKey);
          if (applied.isPresent()) {
            return new AppendResult(applied.getAsLong(), true);
          }
        }
        throw refusal;
      }

      try {
        return eventStore.append(seatId, expectedVersion, newEvents, idempotencyKey);
      } catch (ConcurrencyConflictException conflict) {
        lastConflict = conflict;
        Thread.onSpinWait();
      }
    }

    throw new IllegalStateException(
        "gave up on seat " + seatId + " after " + MAX_ATTEMPTS + " attempts", lastConflict);
  }
}
