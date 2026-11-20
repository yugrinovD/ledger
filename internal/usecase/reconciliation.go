package usecase

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/db"
	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/reconciliation"
)

type ReconciliationResult struct {
	Run           reconciliation.Run
	Discrepancies []reconciliation.Discrepancy
}

func (s *Service) Reconcile(ctx context.Context, repair bool) (ReconciliationResult, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.Reconcile")
	defer span.End()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return ReconciliationResult{}, err
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, db.ReconciliationLockKey).Scan(&locked); err != nil {
		return ReconciliationResult{}, err
	}
	if !locked {
		return ReconciliationResult{}, domain.Conflict("reconciliation is already running")
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, db.ReconciliationLockKey)
	}()

	// The advisory lock above guarantees that no other reconciliation can be
	// running while this process is inside Reconcile. Any row still marked
	// running and older than the threshold therefore belongs to a crashed run.
	if stale, err := s.store.FailStaleReconciliationRuns(ctx, s.now().Add(-time.Hour)); err != nil {
		s.logger.Warn("could not mark stale reconciliation runs", "err", err)
	} else if stale > 0 {
		s.logger.Warn("marked stale reconciliation runs as failed", "count", stale)
	}

	runID, err := uuid.NewV7()
	if err != nil {
		return ReconciliationResult{}, err
	}
	startedAt := s.now()
	run := reconciliation.Run{ID: runID, StartedAt: startedAt, Status: "running", Stats: map[string]any{}}
	if err := s.store.BeginReconciliationRun(ctx, runID, startedAt); err != nil {
		return ReconciliationResult{}, err
	}

	discrepancies, checked, stats, checkErr := s.runChecks(ctx)
	if checkErr != nil {
		run.Status = "failed"
		run.Error = checkErr.Error()
		return ReconciliationResult{}, s.failRun(ctx, run, checkErr)
	}

	run.CheckedAccounts = checked
	run.DiscrepancyCount = int64(len(discrepancies))
	run.Stats = stats
	if err := s.store.InsertDiscrepancies(ctx, runID, discrepancies); err != nil {
		run.Status = "failed"
		run.Error = err.Error()
		return ReconciliationResult{}, s.failRun(ctx, run, err)
	}

	if repair {
		repaired, err := s.repairAccounts(ctx, discrepancies)
		if err != nil {
			run.Status = "failed"
			run.Error = err.Error()
			return ReconciliationResult{}, s.failRun(ctx, run, err)
		}
		run.Stats["repaired_accounts"] = repaired
	}

	run.Status = "completed"
	if err := s.finishRun(ctx, &run); err != nil {
		return ReconciliationResult{}, err
	}
	s.publishReconciliationMetrics(stats)
	return ReconciliationResult{Run: run, Discrepancies: discrepancies}, nil
}

func (s *Service) finishRun(ctx context.Context, run *reconciliation.Run) error {
	finishedAt, err := s.store.FinishReconciliationRun(ctx, *run)
	if err != nil {
		return err
	}
	run.FinishedAt = &finishedAt
	return nil
}

func (s *Service) failRun(ctx context.Context, run reconciliation.Run, cause error) error {
	if err := s.finishRun(ctx, &run); err != nil {
		s.logger.Error("could not finish failed reconciliation run", "run_id", run.ID.String(), "err", err)
		return errors.Join(cause, err)
	}
	return cause
}

func (s *Service) setStatementTimeout(ctx context.Context, tx pgx.Tx) error {
	timeout := s.reconciliationTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	_, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true)`, strconv.FormatInt(timeout.Milliseconds(), 10))
	return err
}

func (s *Service) runChecks(ctx context.Context) ([]reconciliation.Discrepancy, int64, map[string]any, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.setStatementTimeout(ctx, tx); err != nil {
		return nil, 0, nil, err
	}

	checked, err := s.store.CountAccounts(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	var all []reconciliation.Discrepancy

	drift, err := s.store.FindBalanceDrift(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, drift...)

	holdDrift, err := s.store.FindHoldDrift(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, holdDrift...)

	unbalanced, err := s.store.FindUnbalancedTransactions(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, unbalanced...)

	empty, err := s.store.FindEmptyTransactions(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, empty...)

	holdCaptures, err := s.store.FindHoldCaptureAnomalies(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, holdCaptures...)

	entryCount, err := s.store.FindTransferEntryCountAnomalies(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}
	all = append(all, entryCount...)

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, nil, err
	}

	counts := map[string]int{}
	for _, d := range all {
		counts[d.Kind]++
	}
	byKind := make(map[string]any, len(counts))
	for k, v := range counts {
		byKind[k] = v
	}
	pending, oldestAge, err := s.store.OutboxStats(ctx, s.pool)
	if err != nil {
		return nil, 0, nil, err
	}
	stats := map[string]any{
		"checked_accounts":       checked,
		"discrepancies_by_kind":  byKind,
		"outbox_pending":         pending,
		"outbox_oldest_age_secs": oldestAge,
	}
	return all, checked, stats, nil
}

func (s *Service) repairAccounts(ctx context.Context, discrepancies []reconciliation.Discrepancy) (int, error) {
	ids := map[uuid.UUID]struct{}{}
	for _, d := range discrepancies {
		if d.AccountID != nil && (d.Kind == "balance_drift" || d.Kind == "hold_drift") {
			ids[*d.AccountID] = struct{}{}
		}
	}
	repaired := 0
	for id := range ids {
		changed := false
		err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
			if err := s.setStatementTimeout(ctx, tx); err != nil {
				return err
			}
			r, err := s.store.RepairAccount(ctx, tx, id)
			if err != nil {
				return err
			}
			changed = r.Changed
			if r.Changed {
				s.logger.Warn("repaired account projection",
					"account_id", id.String(),
					"before_balance", r.BeforeBalance, "after_balance", r.AfterBalance,
					"before_held", r.BeforeHeld, "after_held", r.AfterHeld)
			}
			return nil
		})
		if err != nil {
			return repaired, err
		}
		if changed {
			repaired++
		}
	}
	return repaired, nil
}

func (s *Service) publishReconciliationMetrics(stats map[string]any) {
	counts, _ := stats["discrepancies_by_kind"].(map[string]any)
	kinds := []string{"balance_drift", "hold_drift", "unbalanced_transaction", "empty_transaction", "hold_capture", "transfer_entry_count"}
	for _, kind := range kinds {
		n := 0
		if counts != nil {
			if v, ok := counts[kind].(int); ok {
				n = v
			}
		}
		s.metrics.ReconciliationDiscrepancies.WithLabelValues(kind).Set(float64(n))
	}
}

func (s *Service) ListReconciliationRuns(ctx context.Context, limit int) ([]reconciliation.Run, error) {
	return s.store.ListReconciliationRuns(ctx, normalizeLimit(limit))
}

func (s *Service) GetReconciliationRun(ctx context.Context, id uuid.UUID) (reconciliation.Run, []reconciliation.Discrepancy, error) {
	run, err := s.store.GetReconciliationRun(ctx, id)
	if err != nil {
		return reconciliation.Run{}, nil, err
	}
	items, err := s.store.ListDiscrepancies(ctx, id)
	if err != nil {
		return reconciliation.Run{}, nil, err
	}
	return run, items, nil
}
