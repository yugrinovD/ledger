package httpapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/reconciliation"
	"github.com/yugrinovD/ledger/internal/usecase"
)

const idempotencyHeader = "Idempotency-Key"
const replayedHeader = "Idempotent-Replayed"

type Handlers struct {
	svc *usecase.Service
}

func NewHandlers(svc *usecase.Service) *Handlers { return &Handlers{svc: svc} }

func (h *Handlers) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
		Type     string `json:"type"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	p := mustPrincipal(r)
	view, replayed, err := h.svc.CreateAccount(r.Context(), usecase.CreateAccountInput{
		OwnerID: p.OwnerID, IdempotencyKey: r.Header.Get(idempotencyHeader),
		Name: req.Name, Currency: req.Currency, Type: req.Type,
	})
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	if replayed {
		w.Header().Set(replayedHeader, "true")
	}
	w.Header().Set("Location", "/v1/accounts/"+view.ID)
	writeJSON(w, http.StatusCreated, view)
}

func (h *Handlers) GetAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "accountID")
	if !ok {
		return
	}
	p := mustPrincipal(r)
	view, err := h.svc.GetAccount(r.Context(), p.OwnerID, id)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handlers) GetBalance(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "accountID")
	if !ok {
		return
	}
	p := mustPrincipal(r)
	view, err := h.svc.GetBalance(r.Context(), p.OwnerID, id)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handlers) ListEntries(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "accountID")
	if !ok {
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 200 {
			writeProblem(w, r, h.logger(), domain.Validation("limit must be between 1 and 200"))
			return
		}
		limit = n
	}
	p := mustPrincipal(r)
	page, err := h.svc.ListEntries(r.Context(), p.OwnerID, id, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handlers) CreateTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Amount               string `json:"amount"`
		Currency             string `json:"currency"`
		Description          string `json:"description"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	source, err := uuid.Parse(req.SourceAccountID)
	if err != nil {
		writeProblem(w, r, h.logger(), domain.Validation("invalid source_account_id"))
		return
	}
	destination, err := uuid.Parse(req.DestinationAccountID)
	if err != nil {
		writeProblem(w, r, h.logger(), domain.Validation("invalid destination_account_id"))
		return
	}
	p := mustPrincipal(r)
	view, replayed, err := h.svc.CreateTransfer(r.Context(), usecase.CreateTransferInput{
		OwnerID: p.OwnerID, IdempotencyKey: r.Header.Get(idempotencyHeader),
		SourceAccountID: source, DestinationAccountID: destination,
		Amount: req.Amount, Currency: req.Currency, Description: req.Description,
	})
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	if replayed {
		w.Header().Set(replayedHeader, "true")
	}
	w.Header().Set("Location", "/v1/transfers/"+view.ID)
	writeJSON(w, http.StatusCreated, view)
}

func (h *Handlers) GetTransfer(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "transferID")
	if !ok {
		return
	}
	p := mustPrincipal(r)
	view, err := h.svc.GetTransfer(r.Context(), p.OwnerID, id)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handlers) CreateHold(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AccountID string     `json:"account_id"`
		Amount    string     `json:"amount"`
		Currency  string     `json:"currency"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	accountID, err := uuid.Parse(req.AccountID)
	if err != nil {
		writeProblem(w, r, h.logger(), domain.Validation("invalid account_id"))
		return
	}
	p := mustPrincipal(r)
	view, replayed, err := h.svc.CreateHold(r.Context(), usecase.CreateHoldInput{
		OwnerID: p.OwnerID, IdempotencyKey: r.Header.Get(idempotencyHeader),
		AccountID: accountID, Amount: req.Amount, Currency: req.Currency, ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	if replayed {
		w.Header().Set(replayedHeader, "true")
	}
	w.Header().Set("Location", "/v1/holds/"+view.ID)
	writeJSON(w, http.StatusCreated, view)
}

func (h *Handlers) GetHold(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "holdID")
	if !ok {
		return
	}
	p := mustPrincipal(r)
	view, err := h.svc.GetHold(r.Context(), p.OwnerID, id)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handlers) CaptureHold(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "holdID")
	if !ok {
		return
	}
	var req struct {
		DestinationAccountID string `json:"destination_account_id"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	destination, err := uuid.Parse(req.DestinationAccountID)
	if err != nil {
		writeProblem(w, r, h.logger(), domain.Validation("invalid destination_account_id"))
		return
	}
	p := mustPrincipal(r)
	view, replayed, err := h.svc.CaptureHold(r.Context(), usecase.CaptureHoldInput{
		OwnerID: p.OwnerID, IdempotencyKey: r.Header.Get(idempotencyHeader),
		HoldID: id, DestinationAccountID: destination,
	})
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	if replayed {
		w.Header().Set(replayedHeader, "true")
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handlers) ReleaseHold(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "holdID")
	if !ok {
		return
	}
	p := mustPrincipal(r)
	view, replayed, err := h.svc.ReleaseHold(r.Context(), usecase.ReleaseHoldInput{
		OwnerID: p.OwnerID, IdempotencyKey: r.Header.Get(idempotencyHeader), HoldID: id,
	})
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	if replayed {
		w.Header().Set(replayedHeader, "true")
	}
	writeJSON(w, http.StatusOK, view)
}

type reconciliationRunView struct {
	ID               string         `json:"id"`
	Status           string         `json:"status"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	CheckedAccounts  int64          `json:"checked_accounts"`
	DiscrepancyCount int64          `json:"discrepancy_count"`
	Error            string         `json:"error,omitempty"`
	Stats            map[string]any `json:"stats"`
}

type discrepancyView struct {
	Kind          string         `json:"kind"`
	AccountID     *string        `json:"account_id,omitempty"`
	TransactionID *string        `json:"transaction_id,omitempty"`
	Expected      *int64         `json:"expected,omitempty"`
	Actual        *int64         `json:"actual,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

func (h *Handlers) ListReconciliationRuns(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 100 {
			writeProblem(w, r, h.logger(), domain.Validation("limit must be between 1 and 100"))
			return
		}
		limit = n
	}
	runs, err := h.svc.ListReconciliationRuns(r.Context(), limit)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	out := make([]reconciliationRunView, 0, len(runs))
	for _, run := range runs {
		out = append(out, reconciliationRunView{
			ID: run.ID.String(), Status: run.Status, StartedAt: run.StartedAt, FinishedAt: run.FinishedAt,
			CheckedAccounts: run.CheckedAccounts, DiscrepancyCount: run.DiscrepancyCount, Error: run.Error, Stats: run.Stats,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (h *Handlers) GetReconciliationRun(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathUUID(w, r, "runID")
	if !ok {
		return
	}
	run, items, err := h.svc.GetReconciliationRun(r.Context(), id)
	if err != nil {
		writeProblem(w, r, h.logger(), err)
		return
	}
	out := struct {
		Run           reconciliationRunView `json:"run"`
		Discrepancies []discrepancyView     `json:"discrepancies"`
	}{
		Run: reconciliationRunView{
			ID: run.ID.String(), Status: run.Status, StartedAt: run.StartedAt, FinishedAt: run.FinishedAt,
			CheckedAccounts: run.CheckedAccounts, DiscrepancyCount: run.DiscrepancyCount, Error: run.Error, Stats: run.Stats,
		},
	}
	for _, d := range items {
		out.Discrepancies = append(out.Discrepancies, discrepancyToView(d))
	}
	writeJSON(w, http.StatusOK, out)
}

func discrepancyToView(d reconciliation.Discrepancy) discrepancyView {
	v := discrepancyView{Kind: d.Kind, Expected: d.Expected, Actual: d.Actual, Details: d.Details}
	if d.AccountID != nil {
		id := d.AccountID.String()
		v.AccountID = &id
	}
	if d.TransactionID != nil {
		id := d.TransactionID.String()
		v.TransactionID = &id
	}
	return v
}

func (h *Handlers) pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	raw := chi.URLParam(r, name)
	id, err := uuid.Parse(raw)
	if err != nil {
		writeProblem(w, r, h.logger(), domain.Validation("invalid %s", name))
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handlers) logger() *slog.Logger { return h.svc.Logger() }

func mustPrincipal(r *http.Request) Principal {
	p, _ := PrincipalFrom(r.Context())
	return p
}
