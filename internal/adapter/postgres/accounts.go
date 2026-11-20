package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
)

const accountColumns = `
	a.id, a.owner_id, a.name, a.currency, a.type, a.status, a.allow_negative, a.created_at, a.updated_at`

func (s *Store) CreateAccount(ctx context.Context, tx pgx.Tx, a domain.Account) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO accounts (id, owner_id, name, currency, type, status, allow_negative, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		a.ID, a.OwnerID, a.Name, a.Currency, a.Type, a.Status, a.AllowNegative, a.CreatedAt)
	if err != nil {
		return mapAccountWriteError(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO account_balances (account_id, balance_minor, held_minor, version, updated_at)
		VALUES ($1, 0, 0, 0, $2)`, a.ID, a.CreatedAt)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (domain.Account, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts a WHERE a.id = $1`, id)
	a, err := scanAccount(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, domain.NotFound("account %s", id)
	}
	return a, err
}

func (s *Store) GetAccountWithBalance(ctx context.Context, id uuid.UUID) (domain.Account, domain.Balance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+accountColumns+`, b.balance_minor, b.held_minor, b.version, b.updated_at
		FROM accounts a
		JOIN account_balances b ON b.account_id = a.id
		WHERE a.id = $1`, id)
	a, b, err := scanAccountWithBalance(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, domain.Balance{}, domain.NotFound("account %s", id)
	}
	return a, b, err
}

// LockAccount takes a row lock on the account and its balance projection. It is
// deliberately the only way to read a mutable balance inside a write
// transaction. Callers must lock accounts in ascending ID order.
func (s *Store) LockAccount(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Account, domain.Balance, error) {
	row := tx.QueryRow(ctx, `
		SELECT `+accountColumns+`, b.balance_minor, b.held_minor, b.version, b.updated_at
		FROM accounts a
		JOIN account_balances b ON b.account_id = a.id
		WHERE a.id = $1
		FOR UPDATE OF a, b`, id)
	a, b, err := scanAccountWithBalance(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, domain.Balance{}, domain.NotFound("account %s", id)
	}
	return a, b, err
}

func (s *Store) GetBalance(ctx context.Context, id uuid.UUID) (domain.Balance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT account_id, balance_minor, held_minor, version, updated_at
		FROM account_balances WHERE account_id = $1`, id)
	var b domain.Balance
	if err := row.Scan(&b.AccountID, &b.Balance, &b.Held, &b.Version, &b.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Balance{}, domain.NotFound("balance for account %s", id)
		}
		return domain.Balance{}, err
	}
	return b, nil
}

func (s *Store) UpdateBalance(ctx context.Context, tx pgx.Tx, b domain.Balance) error {
	tag, err := tx.Exec(ctx, `
		UPDATE account_balances
		SET balance_minor = $2, held_minor = $3, version = version + 1, updated_at = now()
		WHERE account_id = $1 AND version = $4`,
		b.AccountID, b.Balance, b.Held, b.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.Conflict("balance for account %s was modified concurrently", b.AccountID)
	}
	return nil
}

func (s *Store) ListEntries(ctx context.Context, accountID uuid.UUID, limit int, beforeAt *time.Time, beforeID *uuid.UUID) ([]domain.Entry, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if beforeAt == nil {
		rows, err = s.pool.Query(ctx, `
			SELECT id, transaction_id, account_id, direction, amount_minor, currency, created_at
			FROM journal_entries
			WHERE account_id = $1
			ORDER BY created_at DESC, id DESC
			LIMIT $2`, accountID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, transaction_id, account_id, direction, amount_minor, currency, created_at
			FROM journal_entries
			WHERE account_id = $1 AND (created_at, id) < ($2, $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4`, accountID, *beforeAt, *beforeID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]domain.Entry, 0, limit)
	for rows.Next() {
		var e domain.Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Direction, &e.AmountMinor, &e.Currency, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanAccount(row pgx.Row) (domain.Account, error) {
	var a domain.Account
	err := row.Scan(&a.ID, &a.OwnerID, &a.Name, &a.Currency, &a.Type, &a.Status, &a.AllowNegative, &a.CreatedAt, &a.UpdatedAt)
	return a, err
}

func scanAccountWithBalance(row pgx.Row) (domain.Account, domain.Balance, error) {
	var a domain.Account
	var b domain.Balance
	err := row.Scan(
		&a.ID, &a.OwnerID, &a.Name, &a.Currency, &a.Type, &a.Status, &a.AllowNegative, &a.CreatedAt, &a.UpdatedAt,
		&b.Balance, &b.Held, &b.Version, &b.UpdatedAt,
	)
	b.AccountID = a.ID
	return a, b, err
}

func mapAccountWriteError(err error) error {
	switch {
	case isUniqueViolation(err):
		return domain.Conflict("an account with the same owner and name already exists")
	default:
		return err
	}
}

func (s *Store) GetAccountByOwnerName(ctx context.Context, ownerID, name string) (domain.Account, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts a WHERE a.owner_id = $1 AND a.name = $2 AND a.status <> 'closed' LIMIT 1`, ownerID, name)
	a, err := scanAccount(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, domain.NotFound("account %s/%s", ownerID, name)
	}
	return a, err
}
