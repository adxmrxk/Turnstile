-- The event log. Append-only, and the database enforces that rather than
-- trusting the application to behave.

CREATE TABLE events (
  -- Total order across all streams. What projections and the verifier walk.
  global_seq  BIGSERIAL   PRIMARY KEY,
  stream_id   TEXT        NOT NULL,
  -- Position within the stream. The compare-and-append guarantee rests on this
  -- constraint: two writers that both decided from version N both try to insert
  -- N+1, and the database lets exactly one of them.
  version     BIGINT      NOT NULL CHECK (version >= 1),
  type        TEXT        NOT NULL,
  payload     JSONB       NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT events_stream_version UNIQUE (stream_id, version)
);

-- A consumed idempotency key, and the stream version it produced. Written in the
-- same transaction as the events, so a failed append releases its key with no
-- cleanup code, and a key can never outlive an append that did not happen.
CREATE TABLE idempotency_keys (
  idempotency_key TEXT        PRIMARY KEY,
  stream_id       TEXT        NOT NULL,
  version         BIGINT      NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION refuse_event_mutation() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'the event log is append-only: % on % is not allowed', TG_OP, TG_TABLE_NAME
    USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_are_append_only
  BEFORE UPDATE OR DELETE ON events
  FOR EACH ROW EXECUTE FUNCTION refuse_event_mutation();
