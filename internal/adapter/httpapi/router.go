package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/adapter/ratelimit"
	"github.com/yugrinovD/ledger/internal/config"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/usecase"
)

type Server struct {
	cfg     config.Config
	store   *postgres.Store
	svc     *usecase.Service
	logger  *slog.Logger
	metrics *observability.Metrics
	limiter *ratelimit.Limiter
}

func NewServer(cfg config.Config, store *postgres.Store, svc *usecase.Service, logger *slog.Logger, metrics *observability.Metrics, limiter *ratelimit.Limiter) *Server {
	return &Server{cfg: cfg, store: store, svc: svc, logger: logger, metrics: metrics, limiter: limiter}
}

func (s *Server) PublicHandler() http.Handler {
	h := NewHandlers(s.svc)
	r := chi.NewRouter()
	r.Use(requestID())
	r.Use(recoverer(s.logger))
	r.Use(accessLog(s.logger))
	r.Use(metricsMiddleware(s.metrics))
	r.Use(timeoutMiddleware(s.cfg.RequestTimeout))
	r.Use(middleware.Compress(5, "application/json"))

	r.Route("/v1", func(r chi.Router) {
		r.Use(authMiddleware(s.store, s.cfg.AuthPepper, s.logger))
		r.Use(rateLimitMiddleware(s.limiter, s.logger, s.cfg.RateLimitRPM))

		r.With(requireScope("accounts:write")).Post("/accounts", h.CreateAccount)
		r.With(requireScope("accounts:read")).Get("/accounts/{accountID}", h.GetAccount)
		r.With(requireScope("accounts:read")).Get("/accounts/{accountID}/balance", h.GetBalance)
		r.With(requireScope("accounts:read")).Get("/accounts/{accountID}/entries", h.ListEntries)

		r.With(requireScope("transfers:write")).Post("/transfers", h.CreateTransfer)
		r.With(requireScope("transfers:read")).Get("/transfers/{transferID}", h.GetTransfer)

		r.With(requireScope("holds:write")).Post("/holds", h.CreateHold)
		r.With(requireScope("holds:read")).Get("/holds/{holdID}", h.GetHold)
		r.With(requireScope("holds:write")).Post("/holds/{holdID}/capture", h.CaptureHold)
		r.With(requireScope("holds:write")).Post("/holds/{holdID}/release", h.ReleaseHold)

		r.With(requireScope("reconciliation:read")).Get("/reconciliation/runs", h.ListReconciliationRuns)
		r.With(requireScope("reconciliation:read")).Get("/reconciliation/runs/{runID}", h.GetReconciliationRun)
	})

	return otelhttp.NewHandler(r, "ledger-api", otelhttp.WithSpanNameFormatter(func(string, *http.Request) string {
		return "HTTP"
	}))
}

func timeoutMiddleware(d time.Duration) func(http.Handler) http.Handler {
	if d <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
