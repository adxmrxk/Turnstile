package dev.turnstile.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.turnstile.domain.DomainEvent;

/**
 * Turns domain events into the JSON stored in the log and back.
 *
 * <p>Lives beside the store rather than in the domain: the domain is forbidden
 * from importing Jackson, so the wire format is a persistence concern and the
 * events themselves stay plain records.
 *
 * <p>The type name is the record's simple name, the same string the NDJSON export
 * and the shell verifier use.
 */
public final class EventCodec {

  private final ObjectMapper mapper =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  public String typeOf(DomainEvent event) {
    return event.getClass().getSimpleName();
  }

  public String toJson(DomainEvent event) {
    try {
      return mapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise " + event, e);
    }
  }

  /**
   * The message an event travels in: the event plus its position, and an
   * {@code eventKey} that is unique per event so a consumer can discard the
   * duplicates at-least-once delivery produces.
   */
  public String envelope(String streamId, long version, long globalSeq, DomainEvent event) {
    var node = mapper.createObjectNode();
    node.put("eventKey", streamId + ":" + version);
    node.put("streamId", streamId);
    node.put("version", version);
    node.put("seq", globalSeq);
    node.put("type", typeOf(event));
    node.set("event", mapper.valueToTree(event));
    return node.toString();
  }

  public StoredEvent fromEnvelope(String json) {
    try {
      var node = mapper.readTree(json);
      String type = node.get("type").asText();
      DomainEvent event = fromJson(type, node.get("event").toString());
      return new StoredEvent(
          node.get("streamId").asText(), node.get("version").asLong(), node.get("seq").asLong(), event);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot read an event envelope", e);
    }
  }

  public DomainEvent fromJson(String type, String json) {
    try {
      Class<? extends DomainEvent> target =
          switch (type) {
            case "SeatHeld" -> DomainEvent.SeatHeld.class;
            case "HoldReleased" -> DomainEvent.HoldReleased.class;
            case "SeatSold" -> DomainEvent.SeatSold.class;
            default -> throw new IllegalStateException("unknown event type in the log: " + type);
          };
      return mapper.readValue(json, target);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot read a stored " + type, e);
    }
  }
}
