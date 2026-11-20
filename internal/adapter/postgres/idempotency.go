package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
)

func (s *Store) TryInsertIdempotency(ctx context.Context, tx pgx.Tx, r idempotency.Record) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (owner_id, key, request_hash, method, path, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (owner_id, key) DO NOTHING`,
		r.OwnerID, r.Key, r.RequestHash, r.Method, r.Path, r.CreatedAt, r.ExpiresAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) LockIdempotency(ctx context.Context, tx pgx.Tx, ownerID, key string) (idempotency.Record, error) {
	var r idempotency.Record
	var statusCode *int
	var body []byte
	err := tx.QueryRow(ctx, `
		SELECT owner_id, key, request_hash, method, path, status_code, response_body,
			resource_type, resource_id, created_at, expires_at
		FROM idempotency_keys
		WHERE owner_id = $1 AND key = $2
		FOR UPDATE`, ownerID, key).Scan(
		&r.OwnerID, &r.Key, &r.RequestHash, &r.Method, &r.Path, &statusCode, &body,
		&r.ResourceType, &r.ResourceID, &r.CreatedAt, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return idempotency.Record{}, domain.NotFound("idempotency key %s", key)
	}
	if err != nil {
		return idempotency.Record{}, err
	}
	r.StatusCode = statusCode
	r.ResponseBody = body
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
