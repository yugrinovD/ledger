package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
)

type CreateHoldInput struct {
	OwnerID        string
	IdempotencyKey string
	AccountID      uuid.UUID
	Amount         string
	Currency       string
	ExpiresAt      *time.Time
}

type CaptureHoldInput struct {
	OwnerID              string
	IdempotencyKey       string
	HoldID               uuid.UUID
	DestinationAccountID uuid.UUID
}

type ReleaseHoldInput struct {
	OwnerID        string
	IdempotencyKey string
	HoldID         uuid.UUID
}

func (s *Service) CreateHold(ctx context.Context, in CreateHoldInput) (HoldView, bool, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.CreateHold")
	defer span.End()

	currency, err := domain.ParseCurrency(in.Currency)
	if err != nil {
		return HoldView{}, false, err
	}
	amount, err := domain.ParseAmount(in.Amount, currency)
	if err != nil {
		return HoldView{}, false, err
	}
	if amount <= 0 {
		return HoldView{}, false, domain.Validation("amount must be positive")
	}
	expiresAt := s.now().Add(DefaultHoldTTL)
	var hashExpiresAt *time.Time
	if in.ExpiresAt != nil {
		t := in.ExpiresAt.UTC()
		hashExpiresAt = &t
		expiresAt = t
	}
	if !expiresAt.After(s.now()) {
		return HoldView{}, false, domain.Validation("expires_at must be in the future")
	}

	var view HoldView
	result, err := s.idem.Execute(ctx, in.OwnerID, in.IdempotencyKey, "create_hold", "POST", "/v1/holds",
		struct {
			AccountID uuid.UUID  `json:"account_id"`
			Amount    string     `json:"amount"`
			Currency  string     `json:"currency"`
			ExpiresAt *time.Time `json:"expires_at,omitempty"`
		}{in.AccountID, domain.FormatAmount(amount, currency), currency.String(), hashExpiresAt},
		func(ctx context.Context, tx pgx.Tx) (idempotency.Result, error) {
			a, b, err := s.store.LockAccount(ctx, tx, in.AccountID)
			if err != nil {
				return idempotency.Result{}, err
			}
			if a.OwnerID != in.OwnerID {
				return idempotency.Result{}, domain.NotFound("account %s", in.AccountID)
			}
			if !a.Spendable() {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeAccountNotActive, Message: "account is not active", Err: domain.ErrAccountNotActive}
			}
			if a.Currency != currency {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeCurrencyMismatch, Message: fmt.Sprintf("account uses %s", a.Currency), Err: domain.ErrCurrencyMismatch}
			}
			if !a.AllowNegative && b.Available() < amount {
				return idempotency.Result{}, &domain.Error{
					Code: domain.CodeInsufficientFunds,
					Message: fmt.Sprintf("insufficient funds: available %s, requested %s",
						domain.FormatAmount(b.Available(), currency), domain.FormatAmount(amount, currency)),
					Err: domain.ErrInsufficientFunds,
				}
			}
			b.Held += amount
			if err := s.store.UpdateBalance(ctx, tx, b); err != nil {
				return idempotency.Result{}, err
			}
			now := s.now()
			id, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			hold := domain.Hold{
				ID: id, OwnerID: in.OwnerID, IdempotencyKey: in.IdempotencyKey,
				AccountID: a.ID, AmountMinor: amount, Currency: currency, Status: domain.HoldActive,
				ExpiresAt: expiresAt, CreatedAt: now, UpdatedAt: now,
			}
			if err := s.store.InsertHold(ctx, tx, hold); err != nil {
				return idempotency.Result{}, err
			}
			event, err := s.newOutboxEvent(ctx, "hold", hold.ID, "hold.created", map[string]any{
				"hold_id":    hold.ID.String(),
				"account_id": hold.AccountID.String(),
				"amount":     domain.FormatAmount(hold.AmountMinor, hold.Currency),
				"currency":   hold.Currency.String(),
				"expires_at": hold.ExpiresAt,
			})
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return idempotency.Result{}, err
			}
			view = holdView(hold)
			body, err := marshal(view)
			if err != nil {
				return idempotency.Result{}, err
			}
			return idempotency.Result{StatusCode: 201, Body: body, ResourceType: "hold", ResourceID: &hold.ID}, nil
		})
	if err != nil {
		return HoldView{}, false, err
	}
	if result.Replayed {
		if err := json.Unmarshal(result.Body, &view); err != nil {
			return HoldView{}, false, fmt.Errorf("decode replayed hold: %w", err)
		}
	}
	return view, result.Replayed, nil
}

func (s *Service) CaptureHold(ctx context.Context, in CaptureHoldInput) (HoldView, bool, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.CaptureHold")
	defer span.End()

	var view HoldView
	result, err := s.idem.Execute(ctx, in.OwnerID, in.IdempotencyKey, "capture_hold", "POST", "/v1/holds/"+in.HoldID.String()+"/capture",
		struct {
			Destination string `json:"destination_account_id"`
		}{in.DestinationAccountID.String()},
		func(ctx context.Context, tx pgx.Tx) (idempotency.Result, error) {
			hold, err := s.store.LockHold(ctx, tx, in.HoldID)
			if err != nil {
				return idempotency.Result{}, err
			}
			if hold.OwnerID != in.OwnerID {
				return idempotency.Result{}, domain.NotFound("hold %s", in.HoldID)
			}
			if hold.Status != domain.HoldActive {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeHoldNotActive, Message: fmt.Sprintf("hold is %s", hold.Status), Err: domain.ErrHoldNotActive}
			}
			if !hold.ExpiresAt.After(s.now()) {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeHoldNotActive, Message: "hold has expired", Err: domain.ErrHoldNotActive}
			}
			if hold.AccountID == in.DestinationAccountID {
				return idempotency.Result{}, domain.Validation("destination account must differ from the hold account")
			}
			locked, err := lockAccounts(ctx, s, tx, hold.AccountID, in.DestinationAccountID)
			if err != nil {
				return idempotency.Result{}, err
			}
			source := locked[hold.AccountID]
			destination := locked[in.DestinationAccountID]
			if !source.account.Spendable() || destination.account.Status != domain.AccountActive {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeAccountNotActive, Message: "account is not active", Err: domain.ErrAccountNotActive}
			}
			if source.account.Currency != hold.Currency || destination.account.Currency != hold.Currency {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeCurrencyMismatch, Message: fmt.Sprintf("accounts must use %s", hold.Currency), Err: domain.ErrCurrencyMismatch}
			}
			if source.account.NormalSide() != destination.account.NormalSide() {
				return idempotency.Result{}, domain.Validation("capture requires accounts with the same normal side")
			}
			if source.balance.Held < hold.AmountMinor {
				return idempotency.Result{}, domain.Conflict("hold projection is inconsistent; run reconciliation")
			}
			if !source.account.AllowNegative && source.balance.Balance < hold.AmountMinor {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeInsufficientFunds, Message: "account cannot cover the held amount", Err: domain.ErrInsufficientFunds}
			}

			now := s.now()
			txID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertTransaction(ctx, tx, domain.JournalTransaction{
				ID: txID, Type: domain.TransactionHoldCapture, Status: "posted",
				Description: "hold capture", OccurredAt: now, CreatedAt: now,
				Metadata: map[string]any{"hold_id": hold.ID.String()},
			}); err != nil {
				return idempotency.Result{}, err
			}
			sourceEntryID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			destEntryID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertEntry(ctx, tx, domain.Entry{
				ID: sourceEntryID, TransactionID: txID, AccountID: source.account.ID,
				Direction: source.account.NormalSide().Opposite(), AmountMinor: hold.AmountMinor,
				Currency: hold.Currency, CreatedAt: now,
			}); err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertEntry(ctx, tx, domain.Entry{
				ID: destEntryID, TransactionID: txID, AccountID: destination.account.ID,
				Direction: destination.account.NormalSide(), AmountMinor: hold.AmountMinor,
				Currency: hold.Currency, CreatedAt: now,
			}); err != nil {
				return idempotency.Result{}, err
			}
			source.balance.Balance -= hold.AmountMinor
			source.balance.Held -= hold.AmountMinor
			destination.balance.Balance += hold.AmountMinor
			if err := s.store.UpdateBalance(ctx, tx, source.balance); err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.UpdateBalance(ctx, tx, destination.balance); err != nil {
				return idempotency.Result{}, err
			}

			hold.Status = domain.HoldCaptured
			hold.CapturedTransactionID = &txID
			hold.UpdatedAt = now
			if err := s.store.UpdateHold(ctx, tx, hold); err != nil {
				return idempotency.Result{}, err
			}
			event, err := s.newOutboxEvent(ctx, "hold", hold.ID, "hold.captured", map[string]any{
				"hold_id":                hold.ID.String(),
				"transaction_id":         txID.String(),
				"account_id":             hold.AccountID.String(),
				"destination_account_id": in.DestinationAccountID.String(),
				"amount":                 domain.FormatAmount(hold.AmountMinor, hold.Currency),
				"currency":               hold.Currency.String(),
			})
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return idempotency.Result{}, err
			}
			view = holdView(hold)
			body, err := marshal(view)
			if err != nil {
				return idempotency.Result{}, err
			}
			return idempotency.Result{StatusCode: 200, Body: body, ResourceType: "hold", ResourceID: &hold.ID}, nil
		})
	if err != nil {
		return HoldView{}, false, err
	}
	if result.Replayed {
		if err := json.Unmarshal(result.Body, &view); err != nil {
			return HoldView{}, false, fmt.Errorf("decode replayed hold: %w", err)
		}
	}
	return view, result.Replayed, nil
}

func (s *Service) ReleaseHold(ctx context.Context, in ReleaseHoldInput) (HoldView, bool, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.ReleaseHold")
	defer span.End()

	var view HoldView
	result, err := s.idem.Execute(ctx, in.OwnerID, in.IdempotencyKey, "release_hold", "POST", "/v1/holds/"+in.HoldID.String()+"/release",
		struct{}{},
		func(ctx context.Context, tx pgx.Tx) (idempotency.Result, error) {
			hold, err := s.store.LockHold(ctx, tx, in.HoldID)
			if err != nil {
				return idempotency.Result{}, err
			}
			if hold.OwnerID != in.OwnerID {
				return idempotency.Result{}, domain.NotFound("hold %s", in.HoldID)
			}
			if hold.Status != domain.HoldActive {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeHoldNotActive, Message: fmt.Sprintf("hold is %s", hold.Status), Err: domain.ErrHoldNotActive}
			}
			_, balance, err := s.store.LockAccount(ctx, tx, hold.AccountID)
			if err != nil {
				return idempotency.Result{}, err
			}
			if balance.Held < hold.AmountMinor {
				return idempotency.Result{}, domain.Conflict("hold projection is inconsistent; run reconciliation")
			}
			balance.Held -= hold.AmountMinor
			if err := s.store.UpdateBalance(ctx, tx, balance); err != nil {
				return idempotency.Result{}, err
			}
			now := s.now()
			hold.Status = domain.HoldReleased
			hold.UpdatedAt = now
			if err := s.store.UpdateHold(ctx, tx, hold); err != nil {
				return idempotency.Result{}, err
			}
			event, err := s.newOutboxEvent(ctx, "hold", hold.ID, "hold.released", map[string]any{
				"hold_id":    hold.ID.String(),
				"account_id": hold.AccountID.String(),
				"amount":     domain.FormatAmount(hold.AmountMinor, hold.Currency),
				"currency":   hold.Currency.String(),
			})
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return idempotency.Result{}, err
			}
			view = holdView(hold)
			body, err := marshal(view)
			if err != nil {
				return idempotency.Result{}, err
			}
			return idempotency.Result{StatusCode: 200, Body: body, ResourceType: "hold", ResourceID: &hold.ID}, nil
		})
	if err != nil {
		return HoldView{}, false, err
	}
	if result.Replayed {
		if err := json.Unmarshal(result.Body, &view); err != nil {
			return HoldView{}, false, fmt.Errorf("decode replayed hold: %w", err)
		}
	}
	return view, result.Replayed, nil
}

func (s *Service) GetHold(ctx context.Context, ownerID string, id uuid.UUID) (HoldView, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.GetHold")
	defer span.End()
	h, err := s.store.GetHold(ctx, id)
	if err != nil {
		return HoldView{}, err
	}
	if h.OwnerID != ownerID {
		return HoldView{}, domain.NotFound("hold %s", id)
	}
	return holdView(h), nil
}

// ExpireHold is called by the worker. It re-checks status under a row lock, so
// concurrent captures and expirations are safe.
func (s *Service) ExpireHold(ctx context.Context, id uuid.UUID) (bool, error) {
	var expired bool
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
		hold, err := s.store.LockHold(ctx, tx, id)
		if err != nil {
			return err
		}
		if hold.Status != domain.HoldActive || hold.ExpiresAt.After(s.now()) {
			return nil
		}
		_, balance, err := s.store.LockAccount(ctx, tx, hold.AccountID)
		if err != nil {
			return err
		}
		if balance.Held < hold.AmountMinor {
			return domain.Conflict("hold projection is inconsistent; run reconciliation")
		}
		balance.Held -= hold.AmountMinor
		if err := s.store.UpdateBalance(ctx, tx, balance); err != nil {
			return err
		}
		hold.Status = domain.HoldExpired
		hold.UpdatedAt = s.now()
		if err := s.store.UpdateHold(ctx, tx, hold); err != nil {
			return err
		}
		event, err := s.newOutboxEvent(ctx, "hold", hold.ID, "hold.expired", map[string]any{
			"hold_id":    hold.ID.String(),
			"account_id": hold.AccountID.String(),
			"amount":     domain.FormatAmount(hold.AmountMinor, hold.Currency),
			"currency":   hold.Currency.String(),
		})
		if err != nil {
			return err
		}
		if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
			return err
		}
		expired = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if expired {
		s.metrics.HoldsExpired.Inc()
	}
	return expired, nil
}

func (s *Service) ListExpiredHoldIDs(ctx context.Context, limit int) ([]uuid.UUID, error) {
	return s.store.ListExpiredHoldIDs(ctx, normalizeLimit(limit))
}
