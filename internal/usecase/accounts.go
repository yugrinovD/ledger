package usecase

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
)

type CreateAccountInput struct {
	OwnerID        string
	IdempotencyKey string
	Name           string
	Currency       string
	Type           string
	AllowNegative  bool
}

func (s *Service) CreateAccount(ctx context.Context, in CreateAccountInput) (AccountView, bool, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.CreateAccount")
	defer span.End()

	currency, err := domain.ParseCurrency(in.Currency)
	if err != nil {
		return AccountView{}, false, err
	}
	if in.Type == "" {
		in.Type = string(domain.AccountLiability)
	}
	accountType, err := domain.ParseAccountType(in.Type)
	if err != nil {
		return AccountView{}, false, err
	}
	if len(in.Name) == 0 || len(in.Name) > 120 {
		return AccountView{}, false, domain.Validation("name must be between 1 and 120 characters")
	}

	var view AccountView
	result, err := s.idem.Execute(ctx, in.OwnerID, in.IdempotencyKey, "create_account", "POST", "/v1/accounts",
		struct {
			Name          string `json:"name"`
			Currency      string `json:"currency"`
			Type          string `json:"type"`
			AllowNegative bool   `json:"allow_negative"`
		}{in.Name, currency.String(), string(accountType), in.AllowNegative},
		func(ctx context.Context, tx pgx.Tx) (idempotency.Result, error) {
			now := s.now()
			id, err := uuid.NewV7()
			if err != nil {
				return idempotency.Result{}, err
			}
			account := domain.Account{
				ID: id, OwnerID: in.OwnerID, Name: in.Name, Currency: currency,
				Type: accountType, Status: domain.AccountActive, AllowNegative: in.AllowNegative,
				CreatedAt: now, UpdatedAt: now,
			}
			if err := s.store.CreateAccount(ctx, tx, account); err != nil {
				return idempotency.Result{}, err
			}
			event, err := s.newOutboxEvent(ctx, "account", account.ID, "account.created", map[string]any{
				"account_id": account.ID.String(),
				"owner_id":   account.OwnerID,
				"currency":   account.Currency.String(),
				"type":       string(account.Type),
			})
			if err != nil {
				return idempotency.Result{}, err
			}
			if err := s.store.InsertOutbox(ctx, tx, event); err != nil {
				return idempotency.Result{}, err
			}
			view = accountView(account)
			body, err := marshal(view)
			if err != nil {
				return idempotency.Result{}, err
			}
			return idempotency.Result{StatusCode: 201, Body: body, ResourceType: "account", ResourceID: &account.ID}, nil
		})
	if err != nil {
		return AccountView{}, false, err
	}
	if result.Replayed {
		if err := json.Unmarshal(result.Body, &view); err != nil {
			return AccountView{}, false, fmt.Errorf("decode replayed account: %w", err)
		}
	}
	return view, result.Replayed, nil
}

func (s *Service) GetAccount(ctx context.Context, ownerID string, id uuid.UUID) (AccountView, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.GetAccount")
	defer span.End()
	a, err := s.store.GetAccount(ctx, id)
	if err != nil {
		return AccountView{}, err
	}
	if a.OwnerID != ownerID {
		return AccountView{}, domain.NotFound("account %s", id)
	}
	return accountView(a), nil
}

func (s *Service) GetBalance(ctx context.Context, ownerID string, id uuid.UUID) (BalanceView, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.GetBalance")
	defer span.End()
	a, b, err := s.store.GetAccountWithBalance(ctx, id)
	if err != nil {
		return BalanceView{}, err
	}
	if a.OwnerID != ownerID {
		return BalanceView{}, domain.NotFound("account %s", id)
	}
	return balanceView(a, b), nil
}

func (s *Service) ListEntries(ctx context.Context, ownerID string, accountID uuid.UUID, limit int, cursor string) (EntriesPage, error) {
	ctx, span := s.tracer.Start(ctx, "usecase.ListEntries")
	defer span.End()
	a, err := s.store.GetAccount(ctx, accountID)
	if err != nil {
		return EntriesPage{}, err
	}
	if a.OwnerID != ownerID {
		return EntriesPage{}, domain.NotFound("account %s", accountID)
	}
	limit = normalizeLimit(limit)
	var beforeAt *time.Time
	var beforeID *uuid.UUID
	if cursor != "" {
		at, id, err := decodeEntryCursor(cursor)
		if err != nil {
			return EntriesPage{}, err
		}
		beforeAt, beforeID = &at, &id
	}
	entries, err := s.store.ListEntries(ctx, accountID, limit+1, beforeAt, beforeID)
	if err != nil {
		return EntriesPage{}, err
	}
	page := EntriesPage{Entries: make([]EntryView, 0, limit)}
	if len(entries) > limit {
		page.HasMore = true
		entries = entries[:limit]
	}
	for _, e := range entries {
		page.Entries = append(page.Entries, entryView(e))
	}
	if page.HasMore && len(entries) > 0 {
		page.NextCursor = encodeEntryCursor(entries[len(entries)-1])
	}
	return page, nil
}

type entryCursor struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

func encodeEntryCursor(e domain.Entry) string {
	raw, _ := json.Marshal(entryCursor{At: e.CreatedAt.UTC(), ID: e.ID.String()})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeEntryCursor(v string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return time.Time{}, uuid.Nil, domain.Validation("invalid cursor")
	}
	var c entryCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return time.Time{}, uuid.Nil, domain.Validation("invalid cursor")
	}
	id, err := uuid.Parse(c.ID)
	if err != nil {
		return time.Time{}, uuid.Nil, domain.Validation("invalid cursor")
	}
	return c.At, id, nil
}
