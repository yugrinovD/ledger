-- +goose Up
-- Poison events that exhausted the relay retry budget are parked instead of
-- blocking every later event of the same aggregate forever. Operators can list
-- and requeue them through the admin API / ledgerctl.
ALTER TABLE outbox ADD COLUMN parked_at timestamptz;

CREATE INDEX outbox_parked_idx
    ON outbox (parked_at, id)
    WHERE parked_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS outbox_parked_idx;
ALTER TABLE outbox DROP COLUMN IF EXISTS parked_at;
