package dev.turnstile.saga;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemorySagaLog implements SagaLog {

  private final Map<String, SagaRecord> sagas = new ConcurrentHashMap<>();

  @Override
  public boolean insert(SagaRecord record) {
    return sagas.putIfAbsent(record.sagaId(), record) == null;
  }

  @Override
  public void update(SagaRecord record) {
    sagas.put(record.sagaId(), record);
  }

  @Override
  public Optional<SagaRecord> find(String sagaId) {
    return Optional.ofNullable(sagas.get(sagaId));
  }

  @Override
  public List<SagaRecord> incomplete() {
    return sagas.values().stream().filter(s -> !s.state().terminal()).toList();
  }
}
