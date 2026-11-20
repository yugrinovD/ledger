package postgres

import (
	"context"
	"time"
)

func (s *Store) DeletePublishedOutboxBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM outbox
		WHERE id IN (
			SELECT id FROM outbox
			WHERE published_at IS NOT NULL AND published_at < $1
			ORDER BY published_at
			LIMIT $2
		)`, before, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) DeleteProcessedEventsBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM processed_events
		WHERE ctid IN (
			SELECT ctid FROM processed_events
			WHERE processed_at < $1
			ORDER BY processed_at
			LIMIT $2
		)`, before, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) DeleteAuditEventsBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM audit_events
		WHERE id IN (
			SELECT id FROM audit_events
			WHERE consumed_at < $1
			ORDER BY consumed_at
			LIMIT $2
		)`, before, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) DeleteReconciliationRunsBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM reconciliation_runs
		WHERE id IN (
			SELECT id FROM reconciliation_runs
			WHERE started_at < $1 AND status <> 'running'
			ORDER BY started_at
			LIMIT $2
		)`, before, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) EstimatedTableRows(ctx context.Context, tables []string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT relname, GREATEST(reltuples, 0)::bigint
		FROM pg_class
		WHERE relnamespace = 'public'::regnamespace AND relname = ANY($1)`, tables)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64, len(tables))
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			return nil, err
		}
		out[name] = count
	}
	return out, rows.Err()
}
