package dev.turnstile.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/** Seat availability pushed over a real WebSocket, not polled. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LiveFeedTest {

  @LocalServerPort int port;
  @Autowired TestRestTemplate http;

  @Test
  @DisplayName("a subscriber is told a seat was held and then sold, without asking")
  void seat_changes_are_pushed() throws Exception {
    WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
    client.setMessageConverter(new MappingJackson2MessageConverter());
    BlockingQueue<List<Map<String, Object>>> batches = new LinkedBlockingQueue<>();

    StompSession session =
        client
            .connectAsync("ws://localhost:" + port + "/ws", new StompSessionHandlerAdapter() {})
            .get(10, TimeUnit.SECONDS);
    session.subscribe(
        "/topic/seats",
        new StompFrameHandler() {
          @Override
          public Type getPayloadType(StompHeaders headers) {
            return List.class;
          }

          @Override
          @SuppressWarnings("unchecked")
          public void handleFrame(StompHeaders headers, Object payload) {
            batches.add((List<Map<String, Object>>) payload);
          }
        });
    // The subscription is registered asynchronously on the broker.
    Thread.sleep(500);

    String seat = "live-" + UUID.randomUUID().toString().substring(0, 8);
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    http.postForEntity(
        "/api/purchases",
        new HttpEntity<>("{\"seatId\":\"" + seat + "\",\"buyerId\":\"alice\"}", headers),
        String.class);

    String lastStatus = null;
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline && !"SOLD".equals(lastStatus)) {
      List<Map<String, Object>> batch = batches.poll(500, TimeUnit.MILLISECONDS);
      if (batch == null) {
        continue;
      }
      for (Map<String, Object> view : batch) {
        if (seat.equals(view.get("seatId"))) {
          lastStatus = (String) view.get("status");
        }
      }
    }
    session.disconnect();

    assertThat(lastStatus).as("the last pushed state of %s", seat).isEqualTo("SOLD");
  }
}
