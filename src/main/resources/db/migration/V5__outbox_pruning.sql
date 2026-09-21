-- Lets the relay delete published outbox rows by age without scanning the table.
-- Unpublished rows are never touched: they are announcements still owed.
CREATE INDEX outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;
