package dev.turnstile.eventstore;

import dev.turnstile.domain.DomainEvent;

/**
 * A domain event plus its position. {@code version} is the event's index within
 * its own stream and is what optimistic concurrency compares against;
 * {@code globalSequence} is the total order across all streams, which is what
 * the projections and the invariant verifier walk.
 */
public record StoredEvent(
    String streamId, long version, long globalSequence, DomainEvent event) {}
