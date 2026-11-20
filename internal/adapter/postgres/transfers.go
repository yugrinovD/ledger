package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
)

func (s *Store) InsertTransaction(ctx context.Context, tx pgx.Tx, t domain.JournalTransaction) error {
	metadata := t.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO journal_transactions (id, type, status, description, metadata, occurred_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)`,
		t.ID, t.Type, t.Status, t.Description, metadata, t.OccurredAt)
	return err
}

func (s *Store) InsertEntry(ctx context.Context, tx pgx.Tx, e domain.Entry) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, transaction_id, account_id, direction, amount_minor, currency, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.ID, e.TransactionID, e.AccountID, e.Direction, e.AmountMinor, e.Currency, e.CreatedAt)
	return err
}

func (s *Store) InsertTransfer(ctx context.Context, tx pgx.Tx, t domain.Transfer) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, owner_id, idempotency_key, transaction_id, source_account_id,
			destination_account_id, amount_minor, currency, description, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		t.ID, t.OwnerID, t.IdempotencyKey, t.TransactionID, t.SourceAccountID,
		t.DestinationAccountID, t.AmountMinor, t.Currency, t.Description, t.Status, t.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Conflict("transfer with this idempotency key already exists")
		}
		return err
	}
	return nil
}

func (s *Store) GetTransfer(ctx context.Context, id uuid.UUID) (domain.Transfer, error) {
	var t domain.Transfer
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_id, idempotency_key, transaction_id, source_account_id, destination_account_id,
			amount_minor, currency, description, status, created_at
		FROM transfers WHERE id = $1`, id).Scan(
		&t.ID, &t.OwnerID, &t.IdempotencyKey, &t.TransactionID, &t.SourceAccountID, &t.DestinationAccountID,
		&t.AmountMinor, &t.Currency, &t.Description, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Transfer{}, domain.NotFound("transfer %s", id)
	}
	return t, err
}

func (s *Store) GetTransferByKey(ctx context.Context, ownerID, key string) (domain.Transfer, error) {
	var t domain.Transfer
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_id, idempotency_key, transaction_id, source_account_id, destination_account_id,
			amount_minor, currency, description, status, created_at
		FROM transfers WHERE owner_id = $1 AND idempotency_key = $2`, ownerID, key).Scan(
		&t.ID, &t.OwnerID, &t.IdempotencyKey, &t.TransactionID, &t.SourceAccountID, &t.DestinationAccountID,
		&t.AmountMinor, &t.Currency, &t.Description, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Transfer{}, domain.NotFound("transfer with key %s", key)
	}
	return t, err
}

func (s *Store) GetJournalTransaction(ctx context.Context, id uuid.UUID) (domain.JournalTransaction, error) {
	var t domain.JournalTransaction
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, status, description, metadata, occurred_at, created_at
		FROM journal_transactions WHERE id = $1`, id).Scan(
		&t.ID, &t.Type, &t.Status, &t.Description, &t.Metadata, &t.OccurredAt, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.JournalTransaction{}, domain.NotFound("journal transaction %s", id)
	}
	return t, err
}

func (s *Store) ListTransactionEntries(ctx context.Context, transactionID uuid.UUID) ([]domain.Entry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, transaction_id, account_id, direction, amount_minor, currency, created_at
		FROM journal_entries WHERE transaction_id = $1
		ORDER BY created_at, id`, transactionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Entry
	for rows.Next() {
		var e domain.Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Direction, &e.AmountMinor, &e.Currency, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
