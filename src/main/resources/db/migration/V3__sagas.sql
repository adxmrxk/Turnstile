-- Durable state of each purchase saga, written after every step, so a crash
-- between "payment taken" and "seat sold" leaves a record that says exactly where
-- to resume, and a refund is never forgotten.

CREATE TABLE sagas (
  saga_id      TEXT        PRIMARY KEY,
  seat_id      TEXT        NOT NULL,
  buyer_id     TEXT        NOT NULL,
  hold_id      TEXT        NOT NULL,
  order_id     TEXT        NOT NULL,
  amount_cents BIGINT      NOT NULL CHECK (amount_cents >= 0),
  state        TEXT        NOT NULL,
  detail       TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX sagas_incomplete ON sagas (updated_at)
  WHERE state NOT IN ('CONFIRMED', 'REFUSED', 'DECLINED', 'REFUNDED');
