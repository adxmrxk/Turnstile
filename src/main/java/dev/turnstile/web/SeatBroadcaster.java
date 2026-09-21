package dev.turnstile.web;

import dev.turnstile.readmodel.SeatMapProjection;
import dev.turnstile.readmodel.SeatView;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Pushes seat changes to connected viewers.
 *
 * <p>Changes are coalesced: only the latest state of each seat is kept between
 * flushes, and one batch goes out every 100ms. During a rush that turns tens of
 * thousands of events into a handful of messages a browser can actually render,
 * and nothing is lost, because a seat's latest state supersedes its earlier ones.
 */
@Component
public class SeatBroadcaster {

  private static final String TOPIC = "/topic/seats";

  private final SeatMapProjection projection;
  private final SimpMessagingTemplate messaging;
  private final Map<String, SeatView> dirty = new ConcurrentHashMap<>();
  private final ScheduledExecutorService flusher =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "seat-broadcaster");
            t.setDaemon(true);
            return t;
          });

  public SeatBroadcaster(SeatMapProjection projection, SimpMessagingTemplate messaging) {
    this.projection = projection;
    this.messaging = messaging;
  }

  @PostConstruct
  void start() {
    projection.onChange(view -> dirty.put(view.seatId(), view));
    flusher.scheduleWithFixedDelay(this::flush, 100, 100, TimeUnit.MILLISECONDS);
  }

  void flush() {
    if (dirty.isEmpty()) {
      return;
    }
    List<SeatView> batch = new ArrayList<>();
    for (String seatId : new ArrayList<>(dirty.keySet())) {
      SeatView view = dirty.remove(seatId);
      if (view != null) {
        batch.add(view);
      }
    }
    if (!batch.isEmpty()) {
      messaging.convertAndSend(TOPIC, batch);
    }
  }

  @PreDestroy
  void stop() {
    flusher.shutdownNow();
  }
}
