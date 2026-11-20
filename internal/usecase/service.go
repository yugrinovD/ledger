package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/outbox"
)

const DefaultHoldTTL = 15 * time.Minute

type Service struct {
	store   *postgres.Store
	pool    *pgxpool.Pool
	idem    *idempotency.Executor
	metrics *observability.Metrics
	logger  *slog.Logger
	now     func() time.Time
	tracer  trace.Tracer
}

func New(store *postgres.Store, pool *pgxpool.Pool, idem *idempotency.Executor, metrics *observability.Metrics, logger *slog.Logger) *Service {
	return &Service{
		store:   store,
		pool:    pool,
		idem:    idem,
		metrics: metrics,
		logger:  logger,
		now:     func() time.Time { return time.Now().UTC() },
		tracer:  otelTracer(),
	}
}

type AccountView struct {
	ID        string    `json:"id"`
	OwnerID   string    `json:"owner_id"`
	Name      string    `json:"name"`
	Currency  string    `json:"currency"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type BalanceView struct {
	AccountID string    `json:"account_id"`
	Currency  string    `json:"currency"`
	Balance   string    `json:"balance"`
	Held      string    `json:"held"`
	Available string    `json:"available"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

type EntryView struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transaction_id"`
	Direction     string    `json:"direction"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

type EntriesPage struct {
	Entries    []EntryView `json:"entries"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
}

type TransferView struct {
	ID                   string    `json:"id"`
	Status               string    `json:"status"`
	SourceAccountID      string    `json:"source_account_id"`
	DestinationAccountID string    `json:"destination_account_id"`
	Amount               string    `json:"amount"`
	Currency             string    `json:"currency"`
	Description          string    `json:"description"`
	CreatedAt            time.Time `json:"created_at"`
}

type HoldView struct {
	ID           string    `json:"id"`
	AccountID    string    `json:"account_id"`
	Amount       string    `json:"amount"`
	Currency     string    `json:"currency"`
	Status       string    `json:"status"`
	ExpiresAt    time.Time `json:"expires_at"`
	CreatedAt    time.Time `json:"created_at"`
	CapturedTxID *string   `json:"captured_transaction_id,omitempty"`
}

func accountView(a domain.Account) AccountView {
	return AccountView{
		ID: a.ID.String(), OwnerID: a.OwnerID, Name: a.Name, Currency: a.Currency.String(),
		Type: string(a.Type), Status: string(a.Status), CreatedAt: a.CreatedAt.UTC(),
	}
}

func balanceView(a domain.Account, b domain.Balance) BalanceView {
	return BalanceView{
		AccountID: b.AccountID.String(),
		Currency:  a.Currency.String(),
		Balance:   domain.FormatAmount(b.Balance, a.Currency),
		Held:      domain.FormatAmount(b.Held, a.Currency),
		Available: domain.FormatAmount(b.Available(), a.Currency),
		Version:   b.Version,
		UpdatedAt: b.UpdatedAt.UTC(),
	}
}

func entryView(e domain.Entry) EntryView {
	return EntryView{
		ID: e.ID.String(), TransactionID: e.TransactionID.String(), Direction: string(e.Direction),
		Amount: domain.FormatAmount(e.AmountMinor, e.Currency), Currency: e.Currency.String(),
		CreatedAt: e.CreatedAt.UTC(),
	}
}

func transferView(t domain.Transfer) TransferView {
	return TransferView{
		ID: t.ID.String(), Status: string(t.Status),
		SourceAccountID: t.SourceAccountID.String(), DestinationAccountID: t.DestinationAccountID.String(),
		Amount: domain.FormatAmount(t.AmountMinor, t.Currency), Currency: t.Currency.String(),
		Description: t.Description, CreatedAt: t.CreatedAt.UTC(),
	}
}

func holdView(h domain.Hold) HoldView {
	v := HoldView{
		ID: h.ID.String(), AccountID: h.AccountID.String(),
		Amount: domain.FormatAmount(h.AmountMinor, h.Currency), Currency: h.Currency.String(),
		Status: string(h.Status), ExpiresAt: h.ExpiresAt.UTC(), CreatedAt: h.CreatedAt.UTC(),
	}
	if h.CapturedTransactionID != nil {
		id := h.CapturedTransactionID.String()
		v.CapturedTxID = &id
	}
	return v
}

func marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal response: %w", err)
	}
	return b, nil
}

func (s *Service) Logger() *slog.Logger { return s.logger }

func (s *Service) newOutboxEvent(ctx context.Context, aggregateType string, aggregateID uuid.UUID, eventType string, data any) (outbox.Event, error) {
	eventID, err := uuid.NewV7()
	if err != nil {
		return outbox.Event{}, err
	}
	now := s.now()
	payload, err := outbox.NewEnvelope(eventID, eventType, aggregateType, aggregateID, now, data)
	if err != nil {
		return outbox.Event{}, err
	}
	return outbox.Event{
		EventID:       eventID,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Type:          eventType,
		Payload:       payload,
		Headers:       observability.TraceHeaders(ctx),
		CreatedAt:     now,
		AvailableAt:   now,
	}, nil
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func otelTracer() trace.Tracer {
	return otel.Tracer("ledger")
}
