package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/yugrinovD/ledger/internal/adapter/kafka"
	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/app"
	"github.com/yugrinovD/ledger/internal/config"
	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/outbox"
	"github.com/yugrinovD/ledger/internal/usecase"
	"github.com/yugrinovD/ledger/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ledger-worker exited", "err", err)
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

	shutdownTracing, err := observability.InitTracing(ctx, cfg.ServiceName+"-worker", version.Version, cfg.OTLPEndpoint, cfg.TraceRatio)
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

	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	idem := idempotency.NewExecutor(pool, store, cfg.IdempotencyTTL,
		func(endpoint string) { metrics.IdempotencyReplays.WithLabelValues(endpoint).Inc() },
		func(int, error) { metrics.TxRetries.Inc() },
	)
	svc := usecase.New(store, pool, idem, metrics, logger)
	svc.SetReconciliationTimeout(cfg.ReconciliationStatementTimeout)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaClientID)
	if err != nil {
		return err
	}
	defer func() { _ = producer.Close() }()

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaClientID, cfg.KafkaConsumerGroup,
		cfg.KafkaTopic, cfg.KafkaDLQTopic, producer,
		func(ctx context.Context, env outbox.Envelope) (bool, error) {
			eventID, err := uuid.Parse(env.EventID)
			if err != nil {
				return false, err
			}
			aggregateID, err := uuid.Parse(env.AggregateID)
			if err != nil {
				return false, err
			}
			payload, err := json.Marshal(env)
			if err != nil {
				return false, err
			}
			return store.ConsumeEvent(ctx, cfg.KafkaConsumerGroup, eventID, env.Type, aggregateID, payload)
		}, logger, metrics)
	if err != nil {
		return err
	}
	defer consumer.Close()

	relay := outbox.NewRelay(pool, store, producer, cfg.KafkaTopic, cfg.OutboxBatchSize,
		cfg.OutboxPollInterval, cfg.OutboxMaxAttempts, logger, metrics)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relay.Run(gctx) })
	g.Go(func() error { return consumer.Run(gctx) })
	g.Go(func() error { return runHoldSweeper(gctx, svc, logger, cfg.HoldSweepInterval) })
	g.Go(func() error { return runReconciler(gctx, svc, logger, cfg.ReconciliationInterval) })
	g.Go(func() error { return runIdempotencyCleanup(gctx, store, logger) })
	g.Go(func() error { return runRetention(gctx, store, cfg, logger, metrics) })

	admin := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           observability.AdminHandler(registry, probe(store, producer), cfg.Profiling),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	g.Go(func() error {
		return app.Serve(gctx, logger, cfg.ShutdownTimeout, admin)
	})

	logger.Info("ledger-worker starting",
		"version", version.Version, "commit", version.Commit, "env", cfg.Env,
		"kafka_brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "admin_addr", cfg.AdminAddr)
	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("ledger-worker stopped")
	return nil
}

func probe(store *postgres.Store, producer *kafka.Producer) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := store.Ping(ctx); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		if err := producer.Ping(ctx); err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
		return nil
	}
}

func runHoldSweeper(ctx context.Context, svc *usecase.Service, logger *slog.Logger, interval time.Duration) error {
	if interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ids, err := svc.ListExpiredHoldIDs(ctx, 200)
			if err != nil {
				logger.Error("list expired holds", "err", err)
				continue
			}
			for _, id := range ids {
				expired, err := svc.ExpireHold(ctx, id)
				if err != nil {
					logger.Error("expire hold", "hold_id", id.String(), "err", err)
					continue
				}
				if expired {
					logger.Info("hold expired", "hold_id", id.String())
				}
			}
		}
	}
}

func runIdempotencyCleanup(ctx context.Context, store *postgres.Store, logger *slog.Logger) error {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			var total int64
			for {
				n, err := store.DeleteExpiredIdempotency(ctx, 1000)
				if err != nil {
					logger.Error("delete expired idempotency keys", "err", err)
					break
				}
				total += n
				if n == 0 {
					break
				}
			}
			if total > 0 {
				logger.Info("deleted expired idempotency keys", "count", total)
			}
		}
	}
}

func runRetention(ctx context.Context, store *postgres.Store, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) error {
	type job struct {
		table  string
		retain time.Duration
		del    func(context.Context, time.Time, int) (int64, error)
	}
	jobs := []job{
		{"outbox", cfg.OutboxRetention, store.DeletePublishedOutboxBefore},
		{"processed_events", cfg.ProcessedEventsRetention, store.DeleteProcessedEventsBefore},
		{"audit_events", cfg.AuditRetention, store.DeleteAuditEventsBefore},
		{"reconciliation_runs", cfg.ReconciliationRetention, store.DeleteReconciliationRunsBefore},
	}
	run := func() {
		for _, j := range jobs {
			cutoff := time.Now().UTC().Add(-j.retain)
			var total int64
			for {
				n, err := j.del(ctx, cutoff, 1000)
				if err != nil {
					logger.Error("retention delete failed", "table", j.table, "err", err)
					break
				}
				total += n
				if n == 0 {
					break
				}
			}
			if total > 0 {
				metrics.RetentionDeleted.WithLabelValues(j.table).Add(float64(total))
				logger.Info("retention deleted rows", "table", j.table, "count", total)
			}
		}
		names := make([]string, 0, len(jobs))
		for _, j := range jobs {
			names = append(names, j.table)
		}
		if rows, err := store.EstimatedTableRows(ctx, names); err == nil {
			for table, count := range rows {
				metrics.TableEstimatedRows.WithLabelValues(table).Set(float64(count))
			}
		}
	}
	run()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			run()
		}
	}
}

func runReconciler(ctx context.Context, svc *usecase.Service, logger *slog.Logger, interval time.Duration) error {
	run := func() {
		res, err := svc.Reconcile(ctx, false)
		if err != nil {
			if domain.IsCode(err, domain.CodeConflict) {
				logger.Debug("reconciliation skipped; another run is active")
				return
			}
			logger.Error("reconciliation failed", "err", err)
			return
		}
		logger.Info("reconciliation completed",
			"run_id", res.Run.ID.String(),
			"checked_accounts", res.Run.CheckedAccounts,
			"discrepancies", res.Run.DiscrepancyCount)
	}
	// Run once on startup so a fresh demo dashboard is not empty for 15 minutes.
	run()
	if interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			run()
		}
	}
}
