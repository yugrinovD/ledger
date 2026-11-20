package integration_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yugrinovD/ledger/internal/adapter/postgres"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/outbox"
	"github.com/yugrinovD/ledger/internal/usecase"
)

var (
	migrateOnce sync.Once
	migrateErr  error
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping PostgreSQL integration test")
	}
	migrateOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			migrateErr = err
			return
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('ledger_test_migrations'))`); err != nil {
			migrateErr = err
			return
		}
		defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext('ledger_test_migrations'))`)
		migrateErr = postgres.Migrate(ctx, dsn, postgres.MigrateUp)
	})
	if migrateErr != nil {
		t.Fatalf("migrate test database: %v", migrateErr)
	}
	return dsn
}

func setup(t *testing.T) (*pgxpool.Pool, *postgres.Store, *usecase.Service) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()
	pool, err := postgres.Open(ctx, dsn, postgres.PoolConfig{MaxConns: 20, MinConns: 2})
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	resetDB(t, pool)

	store := postgres.New(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	idem := idempotency.NewExecutor(pool, store, time.Hour, nil, nil)
	svc := usecase.New(store, pool, idem, metrics, logger)
	return pool, store, svc
}

func resetDB(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		TRUNCATE TABLE
			reconciliation_discrepancies, reconciliation_runs,
			audit_events, processed_events, outbox, idempotency_keys,
			holds, transfers, journal_entries, journal_transactions,
			account_balances, accounts, api_keys
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate test database: %v", err)
	}
}

func createAccount(t *testing.T, svc *usecase.Service, owner, name, currency, typ string, allowNegative bool) usecase.AccountView {
	t.Helper()
	key := "test-account-" + owner + "-" + name + "-" + uuid.NewString()
	view, replayed, err := svc.CreateAccount(context.Background(), usecase.CreateAccountInput{
		OwnerID: owner, IdempotencyKey: key, Name: name, Currency: currency, Type: typ, AllowNegative: allowNegative,
	})
	if err != nil {
		t.Fatalf("create account %s: %v", name, err)
	}
	if replayed {
		t.Fatalf("fresh account unexpectedly replayed")
	}
	return view
}

func fund(t *testing.T, svc *usecase.Service, systemID, customerID, amount, currency string) usecase.TransferView {
	t.Helper()
	view, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "system", IdempotencyKey: "test-fund-" + uuid.NewString(),
		SourceAccountID: uuid.MustParse(systemID), DestinationAccountID: uuid.MustParse(customerID),
		Amount: amount, Currency: currency, Description: "test funding",
	})
	if err != nil {
		t.Fatalf("fund account: %v", err)
	}
	return view
}

func balance(t *testing.T, svc *usecase.Service, owner, accountID string) usecase.BalanceView {
	t.Helper()
	view, err := svc.GetBalance(context.Background(), owner, uuid.MustParse(accountID))
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	return view
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}

func mustParse(t *testing.T, value string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}
	return id
}

func newRelay(pool *pgxpool.Pool, store *postgres.Store, publisher outbox.Publisher) *outbox.Relay {
	return outbox.NewRelay(pool, store, publisher, "ledger.events.test", 100, 5*time.Millisecond, 20, testLogger(), testMetrics())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *observability.Metrics {
	return observability.NewMetrics(prometheus.NewRegistry())
}
