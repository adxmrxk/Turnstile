package dev.turnstile.saga;

import java.util.List;
import java.util.Optional;

/** Durable saga state. What lets a restarted process finish what a dead one began. */
public interface SagaLog {

  /** @return false if a saga with this id already exists */
  boolean insert(SagaRecord record);

  void update(SagaRecord record);

  Optional<SagaRecord> find(String sagaId);

  List<SagaRecord> incomplete();

  /**
   * Unfinished purchases nobody has touched for at least {@code age}. A live saga
   * writes its state as it goes, so a stale one belongs to a driver that died.
   */
  List<SagaRecord> incompleteOlderThan(java.time.Duration age);
}
