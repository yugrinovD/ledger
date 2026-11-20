package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/db"
	"github.com/yugrinovD/ledger/internal/outbox"
)

func (s *Store) InsertOutbox(ctx context.Context, tx pgx.Tx, e outbox.Event) error {
	headers := e.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload, headers,
			created_at, available_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.EventID, e.AggregateType, e.AggregateID, e.Type, e.Payload, headers, e.CreatedAt, e.AvailableAt)
	return err
}

func (s *Store) PollOutbox(ctx context.Context, q db.Querier, limit int) ([]outbox.Event, error) {
	rows, err := q.Query(ctx, `
		SELECT o.id, o.event_id, o.aggregate_type, o.aggregate_id, o.event_type, o.payload, o.headers,
			o.created_at, o.available_at, o.attempts
		FROM outbox o
		WHERE o.published_at IS NULL
		  AND o.parked_at IS NULL
		  AND o.available_at <= now()
		  AND NOT EXISTS (
		      SELECT 1 FROM outbox p
		      WHERE p.aggregate_id = o.aggregate_id
		        AND p.published_at IS NULL
		        AND p.parked_at IS NULL
		        AND p.id < o.id
		  )
		ORDER BY o.id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []outbox.Event
	for rows.Next() {
		var e outbox.Event
		var headers []byte
		if err := rows.Scan(&e.ID, &e.EventID, &e.AggregateType, &e.AggregateID, &e.Type,
			&e.Payload, &headers, &e.CreatedAt, &e.AvailableAt, &e.Attempts); err != nil {
			return nil, err
		}
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &e.Headers); err != nil {
				return nil, err
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxFailed(ctx context.Context, q db.Querier, id int64, reason string, nextAttempt time.Duration) error {
	_, err := q.Exec(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1,
			last_error = $2,
			available_at = now() + make_interval(secs => $3)
		WHERE id = $1`, id, reason, nextAttempt.Seconds())
	return err
}

func (s *Store) MarkOutboxParked(ctx context.Context, q db.Querier, id int64, reason string) error {
	_, err := q.Exec(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1,
			last_error = $2,
			parked_at = now()
		WHERE id = $1 AND published_at IS NULL`, id, reason)
	return err
}

func (s *Store) ListParkedOutbox(ctx context.Context, q db.Querier, limit int) ([]outbox.Event, error) {
	rows, err := q.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, headers,
			created_at, available_at, attempts, COALESCE(last_error, ''), parked_at
		FROM outbox
		WHERE published_at IS NULL AND parked_at IS NOT NULL
		ORDER BY parked_at DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outbox.Event
	for rows.Next() {
		var e outbox.Event
		var headers []byte
		if err := rows.Scan(&e.ID, &e.EventID, &e.AggregateType, &e.AggregateID, &e.Type,
			&e.Payload, &headers, &e.CreatedAt, &e.AvailableAt, &e.Attempts, &e.LastError, &e.ParkedAt); err != nil {
			return nil, err
		}
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &e.Headers); err != nil {
				return nil, err
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) RequeueOutbox(ctx context.Context, q db.Querier, id int64) (bool, error) {
	tag, err := q.Exec(ctx, `
		UPDATE outbox
		SET parked_at = NULL,
			attempts = 0,
			last_error = NULL,
			available_at = now()
		WHERE id = $1 AND published_at IS NULL AND parked_at IS NOT NULL`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) OutboxStats(ctx context.Context, q db.Querier) (pending int64, oldestAgeSeconds float64, err error) {
	err = q.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
		FROM outbox
		WHERE published_at IS NULL AND parked_at IS NULL`).Scan(&pending, &oldestAgeSeconds)
	return pending, oldestAgeSeconds, err
}

func (s *Store) OutboxParkedCount(ctx context.Context, q db.Querier) (int64, error) {
	var parked int64
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM outbox
		WHERE published_at IS NULL AND parked_at IS NOT NULL`).Scan(&parked)
	return parked, err
}

func (s *Store) ConsumeEvent(ctx context.Context, consumer string, eventID uuid.UUID, eventType string, aggregateID uuid.UUID, payload []byte) (bool, error) {
	processed := false
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO processed_events (consumer, event_id)
			VALUES ($1, $2)
			ON CONFLICT (consumer, event_id) DO NOTHING`, consumer, eventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_events (event_id, event_type, aggregate_id, payload)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (event_id) DO NOTHING`, eventID, eventType, aggregateID, string(payload)); err != nil {
			return err
		}
		processed = true
		return nil
	})
	return processed, err
}

func (s *Store) MarkOutboxPublishedBatch(ctx context.Context, q db.Querier, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := q.Exec(ctx, `
		UPDATE outbox SET published_at = now()
		WHERE id = ANY($1) AND published_at IS NULL`, ids)
	return err
}
