package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"

	"github.com/yugrinovD/ledger/internal/adapter/httpapi"
	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/adapter/ratelimit"
	"github.com/yugrinovD/ledger/internal/app"
	"github.com/yugrinovD/ledger/internal/config"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/usecase"
	"github.com/yugrinovD/ledger/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ledger-api exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := observability.NewLogger(cfg.LogLevel)
	for _, warning := range cfg.Warnings() {
		logger.Warn(warning)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.InitTracing(ctx, cfg.ServiceName+"-api", version.Version, cfg.OTLPEndpoint, cfg.TraceRatio)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()

	pool, err := postgres.Open(ctx, cfg.DatabaseURL, postgres.PoolConfig{
		MaxConns: cfg.DBMaxConns, MinConns: cfg.DBMinConns,
		MaxConnLifetime: cfg.DBMaxConnLife, MaxConnIdleTime: cfg.DBMaxConnIdle,
		ConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.New(pool)

	redisClient := goredis.NewClient(&goredis.Options{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
	})
	defer func() { _ = redisClient.Close() }()
	pingCtx, cancelPing := context.WithTimeout(ctx, 2*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		logger.Warn("redis is unavailable; rate limiting will fail open", "err", err)
	}
	cancelPing()

	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	idem := idempotency.NewExecutor(pool, store, cfg.IdempotencyTTL,
		func(endpoint string) { metrics.IdempotencyReplays.WithLabelValues(endpoint).Inc() },
		func(int, error) { metrics.TxRetries.Inc() },
	)
	svc := usecase.New(store, pool, idem, metrics, logger)
	svc.SetReconciliationTimeout(cfg.ReconciliationStatementTimeout)
	limiter := ratelimit.New(redisClient, cfg.RateLimitRPM, time.Minute)
	preAuthLimiter := ratelimit.New(redisClient, cfg.PreAuthRateLimitRPM, time.Minute)
	server := httpapi.NewServer(cfg, store, svc, logger, metrics, limiter, preAuthLimiter)

	public := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.PublicHandler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	adminBase := observability.AdminHandler(registry, store.Ping, cfg.Profiling)
	admin := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           httpapi.NewAdminHandler(adminBase, svc, cfg.AdminToken, logger),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	logger.Info("ledger-api starting",
		"version", version.Version, "commit", version.Commit, "env", cfg.Env,
		"http_addr", cfg.HTTPAddr, "admin_addr", cfg.AdminAddr)
	if err := app.Serve(ctx, logger, cfg.ShutdownTimeout, public, admin); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("ledger-api stopped")
	return nil
}
