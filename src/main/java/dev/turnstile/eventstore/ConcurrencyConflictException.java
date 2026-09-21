package dev.turnstile.eventstore;

/**
 * The stream moved between the read and the append, so the decision was made
 * against stale state and must not be committed.
 *
 * <p>This is not an error the caller reports to a user. It means "re-read and
 * decide again": the command handler retries, and on the retry the aggregate
 * sees the seat is now taken and refuses properly with a domain exception. That
 * two-step is the whole reason a race produces a clean "sold out" rather than a
 * double sale.
 */
public class ConcurrencyConflictException extends RuntimeException {

  private final String streamId;
  private final long expectedVersion;
  private final long actualVersion;

  public ConcurrencyConflictException(String streamId, long expectedVersion, long actualVersion) {
    super(
        "stream "
            + streamId
            + " expected version "
            + expectedVersion
            + " but was "
            + actualVersion);
    this.streamId = streamId;
    this.expectedVersion = expectedVersion;
    this.actualVersion = actualVersion;
  }

  public String streamId() {
    return streamId;
  }

  public long expectedVersion() {
    return expectedVersion;
  }

  public long actualVersion() {
    return actualVersion;
  }
}
