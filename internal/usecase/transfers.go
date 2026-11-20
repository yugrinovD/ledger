package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
)

type CreateTransferInput struct {
	OwnerID              string
	IdempotencyKey       string
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	Amount               string
	Currency             string
	Description          string
}

type lockedAccount struct {
	account domain.Account
	balance domain.Balance
}

func (s *Service) CreateTransfer(ctx context.Context, in CreateTransferInput) (TransferView, bool, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.CreateTransfer")
	defer span.End()
	start := time.Now()
	defer func() {
		s.metrics.TransferDuration.Observe(time.Since(start).Seconds())
	}()

	currency, err := domain.ParseCurrency(in.Currency)
	if err != nil {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, err
	}
	amount, err := domain.ParseAmount(in.Amount, currency)
	if err != nil {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, err
	}
	if amount <= 0 {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, domain.Validation("amount must be positive")
	}
	if in.SourceAccountID == uuid.Nil || in.DestinationAccountID == uuid.Nil {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, domain.Validation("source and destination accounts are required")
	}
	if in.SourceAccountID == in.DestinationAccountID {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, &domain.Error{Code: domain.CodeValidation, Message: "source and destination accounts must differ", Err: domain.ErrSameAccount}
	}
	if len(in.Description) > 500 {
		s.metrics.Transfers.WithLabelValues("validation_error").Inc()
		return TransferView{}, false, domain.Validation("description must be at most 500 characters")
	}

	var view TransferView
	result, err := s.idem.Execute(ctx, in.OwnerID, in.IdempotencyKey, "create_transfer", "POST", "/v1/transfers",
		struct {
			Source      string `json:"source_account_id"`
			Destination string `json:"destination_account_id"`
			Amount      string `json:"amount"`
			Currency    string `json:"currency"`
			Description string `json:"description"`
		}{in.SourceAccountID.String(), in.DestinationAccountID.String(), domain.FormatAmount(amount, currency), currency.String(), in.Description},
		func(ctx context.Context, tx pgx.Tx) (idempotency.Result, error) {
			locked, err := lockAccounts(ctx, s, tx, in.SourceAccountID, in.DestinationAccountID)
			if err != nil {
				return idempotency.Result{}, err
			}
			source := locked[in.SourceAccountID]
			destination := locked[in.DestinationAccountID]

			if source.account.OwnerID != in.OwnerID {
				return idempotency.Result{}, domain.NotFound("account %s", in.SourceAccountID)
			}
			if source.account.Status != domain.AccountActive {
				return idempotency.Result{}, accountStatusError(source.account, "source")
			}
			if destination.account.Status != domain.AccountActive {
				return idempotency.Result{}, accountStatusError(destination.account, "destination")
			}
			if source.account.Currency != currency || destination.account.Currency != currency {
				return idempotency.Result{}, &domain.Error{Code: domain.CodeCurrencyMismatch, Message: fmt.Sprintf("accounts must use %s", currency), Err: domain.ErrCurrencyMismatch}
			}
			if source.account.NormalSide() != destination.account.NormalSide() {
				return idempotency.Result{}, domain.Validation("transfers require accounts with the same normal side; use an adjustment for cross-type movements")
			}
			if !source.account.AllowNegative && source.balance.Available() < amount {
				return idempotency.Result{}, &domain.Error{
					Code: domain.CodeInsufficientFunds,
					Message: fmt.Sprintf("insufficient funds: available %s, requested %s",
						domain.FormatAmount(source.balance.Available(), currency), domain.FormatAmount(amount, currency)),
					Err: domain.ErrInsufficientFunds,
				}
			}

			now := s.now()
			txID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			transferID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}

			journal := domain.JournalTransaction{
				ID: txID, Type: domain.TransactionTransfer, Status: "posted",
				Description: in.Description, OccurredAt: now, CreatedAt: now,
				Metadata: map[string]any{"transfer_id": transferID.String()},
			}
			if err := s.store.InsertTransaction(ctx, tx, journal); err != nil {
				return idempotency.Result{}, err
			}
			sourceEntryID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			destinationEntryID, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			sourceEntry := domain.Entry{
				ID: sourceEntryID, TransactionID: txID, AccountID: source.account.ID,
				Direction: source.account.NormalSide().Opposite(), AmountMinor: amount,
				Currency: currency, CreatedAt: now,
			}
			destinationEntry := domain.Entry{
				ID: destinationEntryID, TransactionID: txID, AccountID: destination.account.ID,
				Direction: destination.account.NormalSide(), AmountMinor: amount,
				Currency: currency, CreatedAt: now,
			}
			if err := s.store.InsertEntry(ctx, tx, sourceEntry); err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertEntry(ctx, tx, destinationEntry); err != nil {
				return idempotency.Result{}, err
			}

			source.balance.Balance -= amount
			destination.balance.Balance += amount
			if err := s.store.UpdateBalance(ctx, tx, source.balance); err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.UpdateBalance(ctx, tx, destination.balance); err != nil {
				return idempotency.Result{}, err
			}

			transfer := domain.Transfer{
				ID: transferID, OwnerID: in.OwnerID, IdempotencyKey: in.IdempotencyKey,
				TransactionID: txID, SourceAccountID: source.account.ID, DestinationAccountID: destination.account.ID,
				AmountMinor: amount, Currency: currency, Description: in.Description,
				Status: domain.TransferPosted, CreatedAt: now,
			}
			if err := s.store.InsertTransfer(ctx, tx, transfer); err != nil {
				return idempotency.Result{}, err
			}
			event, err := s.newOutboxEvent(ctx, "transfer", transfer.ID, "transfer.posted", map[string]any{
				"transfer_id":            transfer.ID.String(),
				"transaction_id":         transfer.TransactionID.String(),
				"source_account_id":      transfer.SourceAccountID.String(),
				"destination_account_id": transfer.DestinationAccountID.String(),
				"amount":                 domain.FormatAmount(transfer.AmountMinor, transfer.Currency),
				"currency":               transfer.Currency.String(),
			})
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return idempotency.Result{}, err
			}

			view = transferView(transfer)
			body, err := marshal(view)
			if err != nil {
				return idempotency.Result{}, err
			}
			return idempotency.Result{StatusCode: 201, Body: body, ResourceType: "transfer", ResourceID: &transfer.ID}, nil
		})
	if err != nil {
		s.metrics.Transfers.WithLabelValues("failed").Inc()
		return TransferView{}, false, err
	}
	if result.Replayed {
		if err := json.Unmarshal(result.Body, &view); err != nil {
			return TransferView{}, false, fmt.Errorf("decode replayed transfer: %w", err)
		}
	}
	status := "posted"
	if result.Replayed {
		status = "replayed"
	}
	s.metrics.Transfers.WithLabelValues(status).Inc()
	return view, result.Replayed, nil
}

func (s *Service) GetTransfer(ctx context.Context, ownerID string, id uuid.UUID) (TransferView, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.GetTransfer")
	defer span.End()
	t, err := s.store.GetTransfer(ctx, id)
	if err != nil {
		return TransferView{}, err
	}
	if t.OwnerID != ownerID {
		return TransferView{}, domain.NotFound("transfer %s", id)
	}
	return transferView(t), nil
}

// lockAccounts locks multiple accounts in a deterministic order. This is the
// single most important deadlock-avoidance rule in the service.
func lockAccounts(ctx context.Context, s *Service, tx pgx.Tx, ids ...uuid.UUID) (map[uuid.UUID]lockedAccount, error) {
	ordered := append([]uuid.UUID(nil), ids...)
	slices.SortFunc(ordered, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	out := make(map[uuid.UUID]lockedAccount, len(ordered))
	for _, id := range ordered {
		a, b, err := s.store.LockAccount(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		out[id] = lockedAccount{account: a, balance: b}
	}
	return out, nil
}
