package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yugrinovD/ledger/internal/reconciliation"
)

func TestRetentionDeletesOnlyOldEligibleRows(t *testing.T) {
	pool, store, svc := setup(t)
	ctx := context.Background()
	published := insertOutboxEvent(t, pool, store, uuid.New(), "published")
	unpublished := insertOutboxEvent(t, pool, store, uuid.New(), "unpublished")
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now() - interval '10 days' WHERE event_id = $1`, published.EventID); err != nil {
		t.Fatal(err)
	}
	n, err := store.DeletePublishedOutboxBefore(ctx, time.Now().UTC().Add(-7*24*time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted outbox rows = %d, want 1", n)
	}
	if countRows(t, pool, `SELECT count(*) FROM outbox WHERE event_id = $1`, unpublished.EventID) != 1 {
		t.Fatal("unpublished outbox event must be retained")
	}

	eventID := uuid.New()
	if _, err := store.ConsumeEvent(ctx, "retention-test", eventID, "test", uuid.Nil, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE processed_events SET processed_at = now() - interval '40 days'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE audit_events SET consumed_at = now() - interval '40 days'`); err != nil {
		t.Fatal(err)
	}
	if n, err := store.DeleteProcessedEventsBefore(ctx, time.Now().UTC().Add(-30*24*time.Hour), 100); err != nil || n != 1 {
		t.Fatalf("delete processed = %d, %v", n, err)
	}
	if n, err := store.DeleteAuditEventsBefore(ctx, time.Now().UTC().Add(-30*24*time.Hour), 100); err != nil || n != 1 {
		t.Fatalf("delete audit = %d, %v", n, err)
	}

	runID := uuid.New()
	if err := store.BeginReconciliationRun(ctx, runID, time.Now().UTC().Add(-40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FinishReconciliationRun(ctx, reconciliation.Run{
		ID: runID, Status: "completed", Stats: map[string]any{},
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := store.DeleteReconciliationRunsBefore(ctx, time.Now().UTC().Add(-30*24*time.Hour), 100); err != nil || n != 1 {
		t.Fatalf("delete reconciliation = %d, %v", n, err)
	}
	_ = svc
}
