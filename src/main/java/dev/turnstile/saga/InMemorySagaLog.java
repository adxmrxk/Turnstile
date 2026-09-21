package dev.turnstile.saga;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemorySagaLog implements SagaLog {

  private final Map<String, SagaRecord> sagas = new ConcurrentHashMap<>();
  private final Map<String, Long> touchedAtNanos = new ConcurrentHashMap<>();

  @Override
  public boolean insert(SagaRecord record) {
    boolean inserted = sagas.putIfAbsent(record.sagaId(), record) == null;
    if (inserted) {
      touchedAtNanos.put(record.sagaId(), System.nanoTime());
    }
    return inserted;
  }

  @Override
  public void update(SagaRecord record) {
    // A finished purchase is never overwritten. Two drivers can briefly work the
    // same saga (a slow one, and the reaper); whichever finishes first wins, and a
    // stale write from the other must not drag it back to an earlier state.
    sagas.compute(record.sagaId(), (id, old) -> old != null && old.state().terminal() ? old : record);
    touchedAtNanos.put(record.sagaId(), System.nanoTime());
  }

  @Override
  public Optional<SagaRecord> find(String sagaId) {
    return Optional.ofNullable(sagas.get(sagaId));
  }

  @Override
  public List<SagaRecord> incomplete() {
    return sagas.values().stream().filter(s -> !s.state().terminal()).toList();
  }

  @Override
  public List<SagaRecord> incompleteOlderThan(Duration age) {
    long now = System.nanoTime();
    return incomplete().stream()
        .filter(s -> now - touchedAtNanos.getOrDefault(s.sagaId(), 0L) >= age.toNanos())
        .toList();
  }
}
