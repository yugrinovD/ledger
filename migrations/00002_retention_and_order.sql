-- +goose Up
-- The relay must not publish a later event for an aggregate while an earlier
-- event of the same aggregate is still pending.
CREATE INDEX outbox_aggregate_pending_idx
    ON outbox (aggregate_id, id)
    WHERE published_at IS NULL;

-- Retention scans.
CREATE INDEX outbox_published_at_idx
    ON outbox (published_at)
    WHERE published_at IS NOT NULL;
CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);
CREATE INDEX audit_events_consumed_at_idx ON audit_events (consumed_at);
CREATE INDEX reconciliation_runs_started_at_idx ON reconciliation_runs (started_at);

-- +goose Down
DROP INDEX IF EXISTS reconciliation_runs_started_at_idx;
DROP INDEX IF EXISTS audit_events_consumed_at_idx;
DROP INDEX IF EXISTS processed_events_processed_at_idx;
DROP INDEX IF EXISTS outbox_published_at_idx;
DROP INDEX IF EXISTS outbox_aggregate_pending_idx;
