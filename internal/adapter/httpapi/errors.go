package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/yugrinovD/ledger/internal/domain"
)

type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	p := problem{Type: "about:blank", Status: http.StatusInternalServerError, Code: string(domain.CodeInternal), Detail: "internal error"}
	var de *domain.Error
	switch {
	case errors.As(err, &de):
		p.Code = string(de.Code)
		p.Detail = de.Message
		p.Status = statusForCode(de.Code)
	case errors.Is(err, context.DeadlineExceeded):
		p.Status = http.StatusGatewayTimeout
		p.Code = "timeout"
		p.Detail = "request timed out"
	case errors.Is(err, context.Canceled):
		p.Status = 499
		p.Code = "canceled"
		p.Detail = "request canceled"
	}
	p.Title = http.StatusText(p.Status)
	p.RequestID = RequestIDFrom(r.Context())
	if p.Status >= 500 {
		logger.Error("request failed", "request_id", p.RequestID, "err", err)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func statusForCode(code domain.Code) int {
	switch code {
	case domain.CodeValidation:
		return http.StatusBadRequest
	case domain.CodeNotFound:
		return http.StatusNotFound
	case domain.CodeConflict, domain.CodeIdempotencyConflict, domain.CodeHoldNotActive:
		return http.StatusConflict
	case domain.CodeInsufficientFunds, domain.CodeCurrencyMismatch, domain.CodeAccountNotActive:
		return http.StatusUnprocessableEntity
	case domain.CodeUnauthorized:
		return http.StatusUnauthorized
	case domain.CodeForbidden:
		return http.StatusForbidden
	case domain.CodeRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &domain.Error{Code: domain.CodeValidation, Message: fmt.Sprintf("invalid JSON body: %v", err), Err: domain.ErrValidation}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return domain.Validation("request body must contain a single JSON object")
	}
	return nil
}
