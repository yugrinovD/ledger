package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yugrinovD/ledger/internal/db"
	"github.com/yugrinovD/ledger/internal/outbox"
)

func TestOutboxParksPoisonEventAndLetsLaterEventsThrough(t *testing.T) {
	pool, store, _ := setup(t)
	ctx := context.Background()
	aggregate := uuid.New()
	first := insertOutboxEvent(t, pool, store, aggregate, "first")
	second := insertOutboxEvent(t, pool, store, aggregate, "second")

	// Use a relay with a one-attempt budget so the first failed publish parks
	// the event immediately.
	pub := &fakePublisher{failures: 1}
	relay := outbox.NewRelay(pool, store, pub, "ledger.events.test", 100, time.Millisecond, 1, testLogger(), testMetrics())
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	if n, err := relay.RelayOnce(ctx, conn); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("expected no published events, got %d", n)
	}

	parked, err := store.OutboxParkedCount(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if parked != 1 {
		t.Fatalf("parked = %d, want 1", parked)
	}
	list, err := store.ListParkedOutbox(ctx, pool, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].EventID != first.EventID {
		t.Fatalf("unexpected parked events: %+v", list)
	}

	// The parked first event must no longer block the second event.
	if n, err := relay.RelayOnce(ctx, conn); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("published = %d, want 1", n)
	}
	if pub.count() != 1 {
		t.Fatalf("publisher calls = %d, want 1", pub.count())
	}
	if pub.pubs[0].Key != second.AggregateID.String() {
		t.Fatalf("published wrong event: %+v", pub.pubs[0])
	}

	requeued, err := store.RequeueOutbox(ctx, pool, list[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !requeued {
		t.Fatal("expected event to be requeued")
	}
	if parked, err := store.OutboxParkedCount(ctx, pool); err != nil {
		t.Fatal(err)
	} else if parked != 0 {
		t.Fatalf("parked after requeue = %d, want 0", parked)
	}
	if n, err := relay.RelayOnce(ctx, conn); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("requeued event published = %d, want 1", n)
	}
}

func TestOutboxPartialBatchOnlyParksPoisonRecord(t *testing.T) {
	pool, store, _ := setup(t)
	ctx := context.Background()
	poison := insertOutboxEvent(t, pool, store, uuid.New(), "poison.event")
	healthy := insertOutboxEvent(t, pool, store, uuid.New(), "healthy.event")
	if _, err := pool.Exec(ctx, `UPDATE outbox SET payload = '{"poison":true}'::jsonb WHERE event_id = $1`, poison.EventID); err != nil {
		t.Fatal(err)
	}

	pub := &fakePublisher{failMarker: `"poison"`}
	relay := outbox.NewRelay(pool, store, pub, "ledger.events.test", 100, time.Millisecond, 1, testLogger(), testMetrics())
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	n, err := relay.RelayOnce(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("published = %d, want 1 healthy event", n)
	}
	if pub.count() != 1 || pub.pubs[0].Key != healthy.AggregateID.String() {
		t.Fatalf("healthy event was not published correctly: %+v", pub.pubs)
	}
	if parked, err := store.OutboxParkedCount(ctx, pool); err != nil {
		t.Fatal(err)
	} else if parked != 1 {
		t.Fatalf("parked = %d, want only the poison event", parked)
	}
	list, err := store.ListParkedOutbox(ctx, pool, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].EventID != poison.EventID {
		t.Fatalf("unexpected parked events: %+v", list)
	}
	if countRows(t, pool, `SELECT count(*) FROM outbox WHERE event_id = $1 AND published_at IS NOT NULL`, healthy.EventID) != 1 {
		t.Fatal("healthy event was not marked published")
	}
	if countRows(t, pool, `SELECT count(*) FROM outbox WHERE event_id = $1 AND parked_at IS NOT NULL`, poison.EventID) != 1 {
		t.Fatal("poison event was not parked")
	}

	if requeued, err := store.RequeueOutbox(ctx, pool, list[0].ID); err != nil {
		t.Fatal(err)
	} else if !requeued {
		t.Fatal("poison event was not requeued")
	}
	if countRows(t, pool, `SELECT count(*) FROM outbox WHERE event_id = $1 AND parked_at IS NULL AND published_at IS NOT NULL`, healthy.EventID) != 1 {
		t.Fatal("requeue changed the already-published healthy event")
	}
}

func TestOutboxRelayRunAcquiresLeadershipAndPublishes(t *testing.T) {
	pool, store, _ := setup(t)
	aggregate := uuid.New()
	insertOutboxEvent(t, pool, store, aggregate, "run-event")

	pub := &fakePublisher{}
	relay := outbox.NewRelay(pool, store, pub, "ledger.events.test", 100, 5*time.Millisecond, 20, testLogger(), testMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for pub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("relay.Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relay.Run did not stop after cancellation")
	}
	if pub.count() == 0 {
		t.Fatal("relay.Run did not publish the pending event")
	}

	// The lock must be released after Run returns.
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(context.Background(), `SELECT pg_try_advisory_lock($1)`, db.OutboxRelayLockKey).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("relay did not release its advisory lock")
	}
	_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, db.OutboxRelayLockKey)
}
