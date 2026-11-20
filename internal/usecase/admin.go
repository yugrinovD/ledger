package usecase

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
)

func (s *Service) FreezeAccount(ctx context.Context, id uuid.UUID) (AccountView, error) {
	return s.setAccountStatus(ctx, id, domain.AccountFrozen, "account.frozen")
}

func (s *Service) UnfreezeAccount(ctx context.Context, id uuid.UUID) (AccountView, error) {
	return s.setAccountStatus(ctx, id, domain.AccountActive, "account.unfrozen")
}

func (s *Service) CloseAccount(ctx context.Context, id uuid.UUID) (AccountView, error) {
	return s.setAccountStatus(ctx, id, domain.AccountClosed, "account.closed")
}

func (s *Service) setAccountStatus(ctx context.Context, id uuid.UUID, status domain.AccountStatus, eventType string) (AccountView, error) {
	var view AccountView
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		account, balance, err := s.store.LockAccount(ctx, tx, id)
		if err != nil {
			return err
		}
		if status == domain.AccountFrozen && account.Status == domain.AccountClosed {
			return domain.Conflict("closed account cannot be frozen")
		}
		if status == domain.AccountActive && account.Status == domain.AccountClosed {
			return domain.Conflict("closed account cannot be reopened")
		}
		if status == domain.AccountClosed && (balance.Balance != 0 || balance.Held != 0) {
			return domain.Conflict("account must have zero balance and no active holds before closing")
		}
		if account.Status != status {
			account, err = s.store.SetAccountStatusTx(ctx, tx, id, status)
			if err != nil {
				return err
			}
			event, err := s.newOutboxEvent(ctx, "account", account.ID, eventType, map[string]any{
				"account_id": account.ID.String(),
				"owner_id":   account.OwnerID,
				"status":     string(account.Status),
			})
			if err != nil {
				return err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return err
			}
		}
		view = accountView(account)
		return nil
	})
	return view, err
}

func (s *Service) RevokeAPIKey(ctx context.Context, id uuid.UUID) error {
	return s.store.RevokeAPIKey(ctx, id)
}
