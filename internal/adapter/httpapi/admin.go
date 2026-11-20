package httpapi

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/usecase"
)

// NewAdminHandler wraps the operational admin handler (health, metrics, pprof)
// with authenticated reconciliation endpoints. Reconciliation spans the whole
// ledger, so it must never be reachable with a tenant API key.
func NewAdminHandler(base http.Handler, svc *usecase.Service, token string, logger *slog.Logger) http.Handler {
	if base == nil {
		base = http.NotFoundHandler()
	}
	if svc == nil {
		return base
	}
	h := NewHandlers(svc)
	mux := http.NewServeMux()
	mux.Handle("GET /admin/reconciliation/runs", adminAuth(token, logger, http.HandlerFunc(h.ListReconciliationRuns)))
	mux.Handle("GET /admin/reconciliation/runs/{runID}", adminAuth(token, logger, http.HandlerFunc(h.GetReconciliationRun)))
	mux.Handle("POST /admin/accounts/{accountID}/freeze", adminAuth(token, logger, http.HandlerFunc(h.FreezeAccount)))
	mux.Handle("POST /admin/accounts/{accountID}/unfreeze", adminAuth(token, logger, http.HandlerFunc(h.UnfreezeAccount)))
	mux.Handle("POST /admin/accounts/{accountID}/close", adminAuth(token, logger, http.HandlerFunc(h.CloseAccount)))
	mux.Handle("POST /admin/api-keys/{keyID}/revoke", adminAuth(token, logger, http.HandlerFunc(h.RevokeAPIKey)))
	mux.Handle("GET /admin/outbox/parked", adminAuth(token, logger, http.HandlerFunc(h.ListParkedOutbox)))
	mux.Handle("POST /admin/outbox/{eventID}/requeue", adminAuth(token, logger, http.HandlerFunc(h.RequeueOutbox)))
	mux.Handle("/", base)
	return mux
}

func adminAuth(token string, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || subtle.ConstantTimeCompare([]byte(adminTokenFrom(r)), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ledger-admin"`)
			writeProblem(w, r, logger, &domain.Error{Code: domain.CodeUnauthorized, Message: "invalid admin token", Err: domain.ErrValidation})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func adminTokenFrom(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Admin-Token"))
}
