package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
)

func (s *Store) InsertHold(ctx context.Context, tx pgx.Tx, h domain.Hold) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO holds (id, owner_id, idempotency_key, account_id, amount_minor, currency,
			status, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
		h.ID, h.OwnerID, h.IdempotencyKey, h.AccountID, h.AmountMinor, h.Currency,
		h.Status, h.ExpiresAt, h.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Conflict("hold with this idempotency key already exists")
		}
		return err
	}
	return nil
}

func (s *Store) GetHold(ctx context.Context, id uuid.UUID) (domain.Hold, error) {
	return scanHold(s.pool.QueryRow(ctx, `SELECT `+holdColumns+` FROM holds h WHERE id = $1`, id))
}

func (s *Store) LockHold(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Hold, error) {
	return scanHold(tx.QueryRow(ctx, `SELECT `+holdColumns+` FROM holds h WHERE id = $1 FOR UPDATE`, id))
}

func (s *Store) UpdateHold(ctx context.Context, tx pgx.Tx, h domain.Hold) error {
	tag, err := tx.Exec(ctx, `
		UPDATE holds
		SET status = $2, captured_transaction_id = $3, updated_at = now()
		WHERE id = $1`, h.ID, h.Status, h.CapturedTransactionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.NotFound("hold %s", h.ID)
	}
	return nil
}

func (s *Store) ListExpiredHoldIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM holds
		WHERE status = 'active' AND expires_at <= now()
		ORDER BY expires_at, id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

const holdColumns = `h.id, h.owner_id, h.idempotency_key, h.account_id, h.amount_minor, h.currency,
	h.status, h.expires_at, h.created_at, h.updated_at, h.captured_transaction_id`

func scanHold(row pgx.Row) (domain.Hold, error) {
	var h domain.Hold
	err := row.Scan(&h.ID, &h.OwnerID, &h.IdempotencyKey, &h.AccountID, &h.AmountMinor, &h.Currency,
		&h.Status, &h.ExpiresAt, &h.CreatedAt, &h.UpdatedAt, &h.CapturedTransactionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Hold{}, domain.NotFound("hold")
	}
	return h, err
}
