package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

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

func (s *Store) PollOutbox(ctx context.Context, limit int) ([]outbox.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, headers,
			created_at, available_at, attempts
		FROM outbox
		WHERE published_at IS NULL AND available_at <= now()
		ORDER BY id
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

func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, reason string, nextAttempt time.Duration) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1,
			last_error = $2,
			available_at = now() + make_interval(secs => $3)
		WHERE id = $1`, id, reason, nextAttempt.Seconds())
	return err
}

func (s *Store) OutboxStats(ctx context.Context) (pending int64, oldestAgeSeconds float64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
		FROM outbox
		WHERE published_at IS NULL`).Scan(&pending, &oldestAgeSeconds)
	return pending, oldestAgeSeconds, err
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

func (s *Store) MarkOutboxPublishedBatch(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox SET published_at = now()
		WHERE id = ANY($1) AND published_at IS NULL`, ids)
	return err
}
