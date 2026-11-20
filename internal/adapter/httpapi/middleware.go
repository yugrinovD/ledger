package httpapi

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"

	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/adapter/ratelimit"
	"github.com/yugrinovD/ledger/internal/auth"
	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/observability"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacking not supported")
	}
	return h.Hijack()
}

func requestID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
			if id == "" || len(id) > 128 {
				id = newRequestID()
			}
			ctx := WithRequestID(r.Context(), id)
			w.Header().Set("X-Request-Id", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func accessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r)
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			route := routePattern(r)
			attrs := []any{
				"request_id", RequestIDFrom(r.Context()),
				"method", r.Method,
				"route", route,
				"status", status,
				"bytes", sw.bytes,
				"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
				"remote_ip", clientIP(r),
			}
			if p, ok := PrincipalFrom(r.Context()); ok {
				attrs = append(attrs, "owner_id", p.OwnerID)
			}
			if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
				attrs = append(attrs, "trace_id", sc.TraceID().String())
			}
			if status >= 500 {
				logger.Error("http request", attrs...)
			} else {
				logger.Info("http request", attrs...)
			}
		})
	}
}

func metricsMiddleware(m *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w}
			m.HTTPInFlight.Inc()
			defer m.HTTPInFlight.Dec()
			next.ServeHTTP(sw, r)
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			route := routePattern(r)
			if route == "" {
				route = "unmatched"
			}
			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

func recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered",
						"request_id", RequestIDFrom(r.Context()),
						"panic", fmt.Sprint(rec),
						"stack", string(debug.Stack()))
					writeProblem(w, r, logger, domain.NewError(domain.CodeInternal, "internal error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func authMiddleware(store *postgres.Store, pepper string, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := bearerToken(r)
			if key == "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="ledger"`)
				writeProblem(w, r, logger, &domain.Error{Code: domain.CodeUnauthorized, Message: "missing API key", Err: domain.ErrValidation})
				return
			}
			hash, err := auth.Hash(pepper, key)
			if err != nil {
				writeProblem(w, r, logger, domain.NewError(domain.CodeInternal, "auth is misconfigured"))
				return
			}
			apiKey, err := store.FindAPIKey(r.Context(), hash)
			if errors.Is(err, pgx.ErrNoRows) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="ledger"`)
				writeProblem(w, r, logger, &domain.Error{Code: domain.CodeUnauthorized, Message: "invalid API key", Err: domain.ErrValidation})
				return
			}
			if err != nil {
				writeProblem(w, r, logger, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), Principal{OwnerID: apiKey.OwnerID, Scopes: apiKey.Scopes})))
		})
	}
}

func requireScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFrom(r.Context())
			if !ok || !auth.HasScope(p.Scopes, scope) {
				writeProblem(w, r, slog.Default(), &domain.Error{Code: domain.CodeForbidden, Message: "missing scope " + scope, Err: domain.ErrValidation})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func rateLimitMiddleware(limiter *ratelimit.Limiter, logger *slog.Logger, limit int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := clientIP(r)
			if p, ok := PrincipalFrom(r.Context()); ok {
				key = "owner:" + p.OwnerID
			}
			res, err := limiter.Allow(r.Context(), key)
			if err != nil {
				logger.Warn("rate limiter unavailable; failing open", "err", err)
			}
			if limit > 0 {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limit))
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
				if res.Reset > 0 {
					w.Header().Set("X-RateLimit-Reset", strconv.Itoa(int((time.Now().Add(res.Reset)).Unix())))
				}
			}
			if !res.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(int(res.Reset.Seconds())+1))
				writeProblem(w, r, logger, domain.NewError(domain.CodeRateLimited, "too many requests"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-API-Key"))
}

func clientIP(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		if i := strings.IndexByte(ip, ','); i >= 0 {
			return strings.TrimSpace(ip[:i])
		}
		return strings.TrimSpace(ip)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		return rc.RoutePattern()
	}
	return ""
}
