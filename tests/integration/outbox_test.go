package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/outbox"
)

func insertOutboxEvent(t *testing.T, pool interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}, store interface {
	InsertOutbox(context.Context, pgx.Tx, outbox.Event) error
}, aggregateID uuid.UUID, eventType string) outbox.Event {
	t.Helper()
	eventID := uuid.New()
	now := time.Now().UTC()
	event := outbox.Event{
		EventID: eventID, AggregateType: "test", AggregateID: aggregateID, Type: eventType,
		Payload: []byte(`{"event_id":"` + eventID.String() + `"}`), Headers: map[string]string{},
		CreatedAt: now, AvailableAt: now,
	}
	if err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return store.InsertOutbox(context.Background(), tx, event)
	}); err != nil {
		t.Fatal(err)
	}
	return event
}

func TestOutboxPreservesPerAggregateOrder(t *testing.T) {
	pool, store, _ := setup(t)
	ctx := context.Background()
	aggregate := uuid.New()
	first := insertOutboxEvent(t, pool, store, aggregate, "first")
	second := insertOutboxEvent(t, pool, store, aggregate, "second")

	// Make the first event temporarily unavailable. The second event must not
	// overtake it even though it is available now.
	if _, err := pool.Exec(ctx, `
		UPDATE outbox SET available_at = now() + interval '1 hour' WHERE event_id = $1`, first.EventID); err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	relay := newRelay(pool, store, pub)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	n, err := relay.RelayOnce(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || pub.count() != 0 {
		t.Fatalf("later event overtook blocked event: published=%d count=%d", n, pub.count())
	}

	if _, err := pool.Exec(ctx, `UPDATE outbox SET available_at = now() WHERE event_id = $1`, first.EventID); err != nil {
		t.Fatal(err)
	}
	n, err = relay.RelayOnce(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || pub.count() != 1 {
		t.Fatalf("expected first event only, published=%d count=%d", n, pub.count())
	}
	n, err = relay.RelayOnce(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || pub.count() != 2 {
		t.Fatalf("expected second event, published=%d count=%d", n, pub.count())
	}
	if pub.pubs[0].Key != aggregate.String() || pub.pubs[1].Key != aggregate.String() {
		t.Fatalf("unexpected keys: %+v", pub.pubs)
	}
	_ = second
}

func TestOutboxRelayStopsWhenLeadershipConnectionDies(t *testing.T) {
	pool, store, _ := setup(t)
	ctx := context.Background()
	insertOutboxEvent(t, pool, store, uuid.New(), "event")

	pub := &fakePublisher{}
	relay := newRelay(pool, store, pub)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('ledger_outbox_relay'))`).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("could not acquire relay lock")
	}
	raw := conn.Conn()
	_ = raw.Close(ctx)

	if _, err := relay.RelayOnce(ctx, conn); err == nil {
		t.Fatal("relay must fail when its leadership connection dies")
	}
	if pub.count() != 0 {
		t.Fatalf("relay published %d events after losing its connection", pub.count())
	}
}
