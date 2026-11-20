package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/reconciliation"
)

func (s *Store) BeginReconciliationRun(ctx context.Context, id uuid.UUID, startedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO reconciliation_runs (id, started_at, status)
		VALUES ($1, $2, 'running')`, id, startedAt)
	return err
}

func (s *Store) FinishReconciliationRun(ctx context.Context, r reconciliation.Run) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE reconciliation_runs
		SET finished_at = now(), status = $2, checked_accounts = $3,
			discrepancy_count = $4, error = NULLIF($5, ''), stats = $6
		WHERE id = $1`, r.ID, r.Status, r.CheckedAccounts, r.DiscrepancyCount, r.Error, r.Stats)
	return err
}

func (s *Store) InsertDiscrepancies(ctx context.Context, runID uuid.UUID, items []reconciliation.Discrepancy) error {
	for _, d := range items {
		details := d.Details
		if details == nil {
			details = map[string]any{}
		}
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO reconciliation_discrepancies (run_id, kind, account_id, transaction_id, expected, actual, details)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			runID, d.Kind, d.AccountID, d.TransactionID, d.Expected, d.Actual, details); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetReconciliationRun(ctx context.Context, id uuid.UUID) (reconciliation.Run, error) {
	var r reconciliation.Run
	err := s.pool.QueryRow(ctx, `
		SELECT id, started_at, finished_at, status, checked_accounts, discrepancy_count,
			COALESCE(error, ''), stats
		FROM reconciliation_runs WHERE id = $1`, id).Scan(
		&r.ID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.CheckedAccounts, &r.DiscrepancyCount, &r.Error, &r.Stats)
	if errors.Is(err, pgx.ErrNoRows) {
		return reconciliation.Run{}, fmt.Errorf("reconciliation run %s not found", id)
	}
	return r, err
}

func (s *Store) ListReconciliationRuns(ctx context.Context, limit int) ([]reconciliation.Run, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, started_at, finished_at, status, checked_accounts, discrepancy_count,
			COALESCE(error, ''), stats
		FROM reconciliation_runs
		ORDER BY started_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Run
	for rows.Next() {
		var r reconciliation.Run
		if err := rows.Scan(&r.ID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.CheckedAccounts,
			&r.DiscrepancyCount, &r.Error, &r.Stats); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListDiscrepancies(ctx context.Context, runID uuid.UUID) ([]reconciliation.Discrepancy, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, kind, account_id, transaction_id, expected, actual, details
		FROM reconciliation_discrepancies
		WHERE run_id = $1
		ORDER BY id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Discrepancy
	for rows.Next() {
		var d reconciliation.Discrepancy
		if err := rows.Scan(&d.ID, &d.Kind, &d.AccountID, &d.TransactionID, &d.Expected, &d.Actual, &d.Details); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) CountAccounts(ctx context.Context, q Querier) (int64, error) {
	var n int64
	err := q.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&n)
	return n, err
}

func (s *Store) FindBalanceDrift(ctx context.Context, q Querier) ([]reconciliation.Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT account_id, stored, expected
		FROM (
			SELECT b.account_id,
			       b.balance_minor AS stored,
			       COALESCE(sum(CASE
			           WHEN (e.direction = 'debit') = (a.type IN ('asset', 'expense'))
			           THEN e.amount_minor ELSE -e.amount_minor END), 0) AS expected
			FROM account_balances b
			JOIN accounts a ON a.id = b.account_id
			LEFT JOIN journal_entries e ON e.account_id = b.account_id
			GROUP BY b.account_id, b.balance_minor
		) drift
		WHERE stored <> expected
		ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Discrepancy
	for rows.Next() {
		var d reconciliation.Discrepancy
		var accountID uuid.UUID
		var stored, expected int64
		if err := rows.Scan(&accountID, &stored, &expected); err != nil {
			return nil, err
		}
		d.Kind = "balance_drift"
		d.AccountID = &accountID
		d.Expected = &expected
		d.Actual = &stored
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) FindHoldDrift(ctx context.Context, q Querier) ([]reconciliation.Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT account_id, stored, expected
		FROM (
			SELECT b.account_id,
			       b.held_minor AS stored,
			       COALESCE(sum(h.amount_minor) FILTER (WHERE h.status = 'active'), 0) AS expected
			FROM account_balances b
			LEFT JOIN holds h ON h.account_id = b.account_id
			GROUP BY b.account_id, b.held_minor
		) drift
		WHERE stored <> expected
		ORDER BY account_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Discrepancy
	for rows.Next() {
		var d reconciliation.Discrepancy
		var accountID uuid.UUID
		var stored, expected int64
		if err := rows.Scan(&accountID, &stored, &expected); err != nil {
			return nil, err
		}
		d.Kind = "hold_drift"
		d.AccountID = &accountID
		d.Expected = &expected
		d.Actual = &stored
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) FindUnbalancedTransactions(ctx context.Context, q Querier) ([]reconciliation.Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT transaction_id, currency, delta
		FROM (
			SELECT transaction_id, currency,
			       sum(CASE direction WHEN 'debit' THEN amount_minor ELSE -amount_minor END) AS delta
			FROM journal_entries
			GROUP BY transaction_id, currency
		) d
		WHERE delta <> 0
		ORDER BY transaction_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Discrepancy
	for rows.Next() {
		var d reconciliation.Discrepancy
		var txID uuid.UUID
		var currency string
		var delta int64
		if err := rows.Scan(&txID, &currency, &delta); err != nil {
			return nil, err
		}
		d.Kind = "unbalanced_transaction"
		d.TransactionID = &txID
		d.Expected = new(int64)
		d.Actual = &delta
		d.Details = map[string]any{"currency": currency}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) FindTransferEntryCountAnomalies(ctx context.Context, q Querier) ([]reconciliation.Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT t.id, count(e.id)
		FROM transfers t
		LEFT JOIN journal_entries e ON e.transaction_id = t.transaction_id
		GROUP BY t.id
		HAVING count(e.id) <> 2
		ORDER BY t.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reconciliation.Discrepancy
	for rows.Next() {
		var d reconciliation.Discrepancy
		var txID uuid.UUID
		var count int64
		if err := rows.Scan(&txID, &count); err != nil {
			return nil, err
		}
		d.Kind = "transfer_entry_count"
		d.TransactionID = &txID
		expected := int64(2)
		d.Expected = &expected
		d.Actual = &count
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) RepairBalance(ctx context.Context, tx pgx.Tx, accountID uuid.UUID, expected int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE account_balances
		SET balance_minor = $2, version = version + 1, updated_at = now()
		WHERE account_id = $1`, accountID, expected)
	return err
}

func (s *Store) RepairHeld(ctx context.Context, tx pgx.Tx, accountID uuid.UUID, expected int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE account_balances
		SET held_minor = $2, version = version + 1, updated_at = now()
		WHERE account_id = $1`, accountID, expected)
	return err
}

type AccountRepair struct {
	AccountID     uuid.UUID
	BeforeBalance int64
	AfterBalance  int64
	BeforeHeld    int64
	AfterHeld     int64
	Changed       bool
}

// RepairAccount recomputes both projections from the immutable journal while
// holding the account lock, so concurrent transfers can never observe a
// half-repaired projection.
func (s *Store) RepairAccount(ctx context.Context, tx pgx.Tx, accountID uuid.UUID) (AccountRepair, error) {
	if _, _, err := s.LockAccount(ctx, tx, accountID); err != nil {
		return AccountRepair{}, err
	}
	var out AccountRepair
	out.AccountID = accountID
	var expectedBalance, expectedHeld int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(sum(CASE
			WHEN (e.direction = 'debit') = (a.type IN ('asset', 'expense'))
			THEN e.amount_minor ELSE -e.amount_minor END), 0)
		FROM accounts a
		LEFT JOIN journal_entries e ON e.account_id = a.id
		WHERE a.id = $1
		GROUP BY a.id`, accountID).Scan(&expectedBalance); err != nil {
		return AccountRepair{}, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(sum(amount_minor) FILTER (WHERE status = 'active'), 0)
		FROM holds WHERE account_id = $1`, accountID).Scan(&expectedHeld); err != nil {
		return AccountRepair{}, err
	}
	row := tx.QueryRow(ctx, `SELECT balance_minor, held_minor FROM account_balances WHERE account_id = $1 FOR UPDATE`, accountID)
	if err := row.Scan(&out.BeforeBalance, &out.BeforeHeld); err != nil {
		return AccountRepair{}, err
	}
	out.AfterBalance = expectedBalance
	out.AfterHeld = expectedHeld
	out.Changed = out.BeforeBalance != expectedBalance || out.BeforeHeld != expectedHeld
	if !out.Changed {
		return out, nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE account_balances
		SET balance_minor = $2, held_minor = $3, version = version + 1, updated_at = now()
		WHERE account_id = $1`, accountID, expectedBalance, expectedHeld)
	return out, err
}

func (s *Store) FailStaleReconciliationRuns(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE reconciliation_runs
		SET status = 'failed', finished_at = now(), error = 'stale run (process crashed or timed out)'
		WHERE status = 'running' AND started_at < $1`, olderThan)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
