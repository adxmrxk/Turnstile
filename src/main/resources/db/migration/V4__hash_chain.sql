-- Tamper evidence. Each event carries the hash of the seat's previous event and its
-- own hash over (prev_hash, stream, version, type, payload), so the events of a seat
-- form a chain: change or remove anything in the middle and every later hash stops
-- matching.
--
-- The append-only trigger already blocks UPDATE and DELETE, but a superuser can
-- disable it. The chain is what makes such an edit visible afterwards.
--
-- Nullable so the migration applies to a database that already has events; the
-- verifier reports any event without a hash as unchained instead of trusting it.

ALTER TABLE events
  ADD COLUMN prev_hash TEXT,
  ADD COLUMN hash      TEXT;
