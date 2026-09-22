package dev.turnstile.messaging;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Deletes this process's consumer group when it shuts down cleanly.
 *
 * <p>Every process uses its own group (so each one sees every event), which meant
 * every restart left a dead group on the broker holding committed offsets until Kafka
 * expired it, and lag monitors reported it as a consumer that had stopped. Stopping
 * the offset commits was tried first and did not work: the container kept committing.
 * Deleting the group afterwards works whoever committed.
 *
 * <p>Only a clean shutdown gets to run this. A process that is killed leaves its group
 * behind, and Kafka's own expiry is what eventually removes it. That is a limit of the
 * approach, not something it hides.
 */
public final class SeatMapGroupCleanup implements DisposableBean {

  public static final String LISTENER_ID = "seatMapListener";

  private static final Logger LOG = LoggerFactory.getLogger(SeatMapGroupCleanup.class);

  private final KafkaListenerEndpointRegistry registry;
  private final KafkaAdmin admin;

  public SeatMapGroupCleanup(KafkaListenerEndpointRegistry registry, KafkaAdmin admin) {
    this.registry = registry;
    this.admin = admin;
  }

  @Override
  public void destroy() {
    MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
    if (container == null || container.getGroupId() == null) {
      return;
    }
    String group = container.getGroupId();
    container.stop(); // leaves the group; a group can only be deleted once it is empty
    try (Admin client = Admin.create(admin.getConfigurationProperties())) {
      for (int attempt = 1; ; attempt++) {
        try {
          client.deleteConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS);
          return;
        } catch (java.util.concurrent.ExecutionException e) {
          if (e.getCause() instanceof GroupNotEmptyException && attempt < 10) {
            Thread.sleep(300); // the member is still leaving
            continue;
          }
          throw e;
        }
      }
    } catch (Exception e) {
      LOG.warn("could not delete consumer group {}; Kafka will expire it: {}", group, e.toString());
    }
  }
}
