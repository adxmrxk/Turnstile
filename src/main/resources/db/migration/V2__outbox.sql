-- Transactional outbox. A row is written in the same transaction as the event it
-- announces, so an event is published if and only if it was committed. Nothing
-- can commit an event and forget to tell anyone, and nothing can announce an
-- event that was rolled back.

CREATE TABLE outbox (
  id           BIGSERIAL   PRIMARY KEY,
  -- stream_id:version. Unique per event, so a consumer can discard the
  -- duplicates that at-least-once delivery will produce.
  event_key    TEXT        NOT NULL UNIQUE,
  stream_id    TEXT        NOT NULL,
  payload      JSONB       NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
