package dev.turnstile.eventstore;

/**
 * @param newVersion stream version after the append
 * @param deduplicated true when the idempotency key had already been used, in
 *     which case nothing was written and the original outcome stands
 */
public record AppendResult(long newVersion, boolean deduplicated) {}
