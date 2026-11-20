package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
)

// AcquireIdempotency atomically inserts a key or returns the committed record.
// It also replaces an expired row in place. If cleanup deletes the conflicting
// row between INSERT and SELECT, the bounded loop retries the insert instead of
// returning a spurious 404.
func (s *Store) AcquireIdempotency(ctx context.Context, tx pgx.Tx, r idempotency.Record) (idempotency.Record, bool, error) {
	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (owner_id, key, request_hash, method, path, created_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (owner_id, key) DO NOTHING`,
			r.OwnerID, r.Key, r.RequestHash, r.Method, r.Path, r.CreatedAt, r.ExpiresAt)
		if err != nil {
			return idempotency.Record{}, false, err
		}
		if tag.RowsAffected() == 1 {
			return r, true, nil
		}
		existing, err := lockIdempotency(ctx, tx, r.OwnerID, r.Key)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return idempotency.Record{}, false, err
		}
		if !existing.ExpiresAt.After(time.Now().UTC()) {
			if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE owner_id = $1 AND key = $2`, r.OwnerID, r.Key); err != nil {
				return idempotency.Record{}, false, err
			}
			continue
		}
		return existing, false, nil
	}
	return idempotency.Record{}, false, domain.Conflict("idempotency key is under contention; retry the request")
}

func lockIdempotency(ctx context.Context, tx pgx.Tx, ownerID, key string) (idempotency.Record, error) {
	var r idempotency.Record
	var statusCode *int
	var body []byte
	var resourceType *string
	err := tx.QueryRow(ctx, `
		SELECT owner_id, key, request_hash, method, path, status_code, response_body,
			resource_type, resource_id, created_at, expires_at
		FROM idempotency_keys
		WHERE owner_id = $1 AND key = $2
		FOR UPDATE`, ownerID, key).Scan(
		&r.OwnerID, &r.Key, &r.RequestHash, &r.Method, &r.Path, &statusCode, &body,
		&resourceType, &r.ResourceID, &r.CreatedAt, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return idempotency.Record{}, pgx.ErrNoRows
	}
	if err != nil {
		return idempotency.Record{}, err
	}
	r.StatusCode = statusCode
	r.ResponseBody = body
	if resourceType != nil {
		r.ResourceType = *resourceType
	}
	return r, nil
}

func (s *Store) CompleteIdempotency(ctx context.Context, tx pgx.Tx, ownerID, key string, statusCode int, body []byte, resourceType string, resourceID *uuid.UUID) error {
	tag, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET status_code = $3, response_body = $4::jsonb, resource_type = $5, resource_id = $6
		WHERE owner_id = $1 AND key = $2`,
		ownerID, key, statusCode, string(body), resourceType, resourceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.NotFound("idempotency key %s", key)
	}
	return nil
}

func (s *Store) DeleteExpiredIdempotency(ctx context.Context, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys
		WHERE (owner_id, key) IN (
			SELECT owner_id, key FROM idempotency_keys WHERE expires_at < now() LIMIT $1
		)`, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
