package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yugrinovD/ledger/internal/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/idempotency"
	"github.com/yugrinovD/ledger/internal/usecase"
)

func TestTransferDoubleEntryAndIdempotency(t *testing.T) {
	pool, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	input := usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "idem-transfer-1",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
		Amount: "10.50", Currency: "USD", Description: "coffee",
	}
	first, replayed, err := svc.CreateTransfer(context.Background(), input)
	if err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	if replayed {
		t.Fatal("first request must not be a replay")
	}
	second, replayed, err := svc.CreateTransfer(context.Background(), input)
	if err != nil {
		t.Fatalf("replayed transfer: %v", err)
	}
	if !replayed {
		t.Fatal("second request must be a replay")
	}
	if second.ID != first.ID {
		t.Fatalf("replay returned different transfer: %s vs %s", second.ID, first.ID)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "89.50" {
		t.Fatalf("alice balance = %s, want 89.50", got)
	}
	if got := balance(t, svc, "bob", bob.ID).Balance; got != "10.50" {
		t.Fatalf("bob balance = %s, want 10.50", got)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM transfers WHERE owner_id = 'alice' AND idempotency_key = 'idem-transfer-1'`); n != 1 {
		t.Fatalf("expected exactly one transfer row, got %d", n)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'transfer.posted' AND aggregate_id = $1`, mustParse(t, first.ID)); n != 1 {
		t.Fatalf("expected exactly one outbox event, got %d", n)
	}
	var delta int64
	if err := pool.QueryRow(context.Background(), `
		SELECT sum(CASE direction WHEN 'debit' THEN amount_minor ELSE -amount_minor END)
		FROM journal_entries WHERE transaction_id = (SELECT transaction_id FROM transfers WHERE id = $1)`,
		mustParse(t, first.ID)).Scan(&delta); err != nil {
		t.Fatal(err)
	}
	if delta != 0 {
		t.Fatalf("journal transaction is unbalanced: %d", delta)
	}
}

func TestConcurrentDuplicateTransfers(t *testing.T) {
	pool, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	input := usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "concurrent-duplicate",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
		Amount: "10.00", Currency: "USD",
	}
	const goroutines = 25
	var wg sync.WaitGroup
	ids := make(chan string, goroutines)
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, _, err := svc.CreateTransfer(context.Background(), input)
			if err != nil {
				errs <- err
				return
			}
			ids <- view.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent duplicate failed: %v", err)
	}
	unique := map[string]struct{}{}
	for id := range ids {
		unique[id] = struct{}{}
	}
	if len(unique) != 1 {
		t.Fatalf("expected one transfer id, got %d", len(unique))
	}
	if n := countRows(t, pool, `SELECT count(*) FROM transfers WHERE idempotency_key = 'concurrent-duplicate'`); n != 1 {
		t.Fatalf("expected one transfer row, got %d", n)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "90.00" {
		t.Fatalf("alice balance = %s, want 90.00", got)
	}
}

func TestConcurrentSpendDoesNotOverdraw(t *testing.T) {
	pool, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	const attempts = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for i := 0; i < attempts; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
				OwnerID: "alice", IdempotencyKey: fmt.Sprintf("spend-%d", i),
				SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
				Amount: "60.00", Currency: "USD",
			})
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
				return
			}
			if !domain.IsCode(err, domain.CodeInsufficientFunds) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("expected exactly one successful spend, got %d", successes)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "40.00" {
		t.Fatalf("alice balance = %s, want 40.00", got)
	}
	if got := balance(t, svc, "bob", bob.ID).Balance; got != "60.00" {
		t.Fatalf("bob balance = %s, want 60.00", got)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM transfers WHERE owner_id = 'alice' AND source_account_id = $1`, mustParse(t, alice.ID)); n != 1 {
		t.Fatalf("expected one transfer, got %d", n)
	}
}

func TestOppositeTransfersDoNotDeadlock(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")
	fund(t, svc, system.ID, bob.ID, "100.00", "USD")

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 10; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
				OwnerID: "alice", IdempotencyKey: fmt.Sprintf("ab-%d", i),
				SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
				Amount: "1.00", Currency: "USD",
			})
			if err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			_, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
				OwnerID: "bob", IdempotencyKey: fmt.Sprintf("ba-%d", i),
				SourceAccountID: mustParse(t, bob.ID), DestinationAccountID: mustParse(t, alice.ID),
				Amount: "1.00", Currency: "USD",
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("opposite transfer failed: %v", err)
	}
	if a, b := balance(t, svc, "alice", alice.ID).Balance, balance(t, svc, "bob", bob.ID).Balance; a != "100.00" || b != "100.00" {
		t.Fatalf("expected equal balances, got alice=%s bob=%s", a, b)
	}
}

func TestReconciliationDetectsAndRepairsDrift(t *testing.T) {
	pool, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	if _, err := pool.Exec(context.Background(), `
		UPDATE account_balances SET balance_minor = balance_minor + 777, version = version + 1
		WHERE account_id = $1`, mustParse(t, alice.ID)); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Run.DiscrepancyCount == 0 {
		t.Fatal("expected a discrepancy")
	}
	found := false
	for _, d := range res.Discrepancies {
		if d.Kind == "balance_drift" && d.AccountID != nil && *d.AccountID == mustParse(t, alice.ID) {
			found = true
		}
	}
	if !found {
		t.Fatalf("balance drift not reported: %+v", res.Discrepancies)
	}

	if _, err := svc.Reconcile(context.Background(), true); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "100.00" {
		t.Fatalf("balance after repair = %s, want 100.00", got)
	}
	res, err = svc.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Run.DiscrepancyCount != 0 {
		t.Fatalf("expected clean reconciliation, got %d discrepancies", res.Run.DiscrepancyCount)
	}
}

func TestHoldLifecycle(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	merchant := createAccount(t, svc, "merchant", "merchant-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	hold, replayed, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "hold-1", AccountID: mustParse(t, alice.ID),
		Amount: "30.00", Currency: "USD",
	})
	if err != nil || replayed {
		t.Fatalf("create hold: %v replayed=%v", err, replayed)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Held != "30.00" || got.Available != "70.00" {
		t.Fatalf("unexpected balance after hold: %+v", got)
	}
	captured, _, err := svc.CaptureHold(context.Background(), usecase.CaptureHoldInput{
		OwnerID: "alice", IdempotencyKey: "capture-1", HoldID: mustParse(t, hold.ID),
		DestinationAccountID: mustParse(t, merchant.ID),
	})
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if captured.Status != "captured" {
		t.Fatalf("hold status = %s", captured.Status)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Balance != "70.00" || got.Held != "0.00" {
		t.Fatalf("unexpected alice balance after capture: %+v", got)
	}
	if got := balance(t, svc, "merchant", merchant.ID).Balance; got != "30.00" {
		t.Fatalf("merchant balance = %s", got)
	}

	hold2, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "hold-2", AccountID: mustParse(t, alice.ID),
		Amount: "20.00", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	released, _, err := svc.ReleaseHold(context.Background(), usecase.ReleaseHoldInput{
		OwnerID: "alice", IdempotencyKey: "release-1", HoldID: mustParse(t, hold2.ID),
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released.Status != "released" {
		t.Fatalf("hold status = %s", released.Status)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Balance != "70.00" || got.Held != "0.00" {
		t.Fatalf("unexpected balance after release: %+v", got)
	}
}

func TestDatabaseRejectsUnbalancedJournal(t *testing.T) {
	pool, _, svc := setup(t)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)

	txID, _ := uuid.NewV7()
	entryID, _ := uuid.NewV7()
	err := pgx.BeginTxFunc(context.Background(), pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(context.Background(), `
			INSERT INTO journal_transactions (id, type, status) VALUES ($1, 'adjustment', 'posted')`, txID); err != nil {
			return err
		}
		_, err := tx.Exec(context.Background(), `
			INSERT INTO journal_entries (id, transaction_id, account_id, direction, amount_minor, currency)
			VALUES ($1, $2, $3, 'credit', 100, 'USD')`, entryID, txID, mustParse(t, alice.ID))
		return err
	})
	if err == nil {
		t.Fatal("unbalanced journal transaction must be rejected")
	}
	if n := countRows(t, pool, `SELECT count(*) FROM journal_transactions WHERE id = $1`, txID); n != 0 {
		t.Fatalf("failed transaction leaked rows: %d", n)
	}
}

func TestDatabaseRejectsJournalMutation(t *testing.T) {
	pool, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	transfer := fund(t, svc, system.ID, alice.ID, "10.00", "USD")

	var entryID uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT e.id FROM journal_entries e
		JOIN transfers t ON t.transaction_id = e.transaction_id
		WHERE t.id = $1 LIMIT 1`, mustParse(t, transfer.ID)).Scan(&entryID); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(context.Background(), `UPDATE journal_entries SET amount_minor = 1 WHERE id = $1`, entryID)
	if err == nil {
		t.Fatal("journal entries must be immutable")
	}
}

func TestOutboxRelayRetriesAndPublishes(t *testing.T) {
	pool, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "10.00", "USD")

	pub := &fakePublisher{failures: 1000}
	relay := newRelay(pool, store, pub)
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if n, err := relay.RelayOnce(context.Background(), conn); err != nil {
		t.Fatalf("first relay: %v", err)
	} else if n != 0 {
		t.Fatalf("expected no published events on failure, got %d", n)
	}
	pub.setFailures(0)
	time.Sleep(200 * time.Millisecond)
	n, err := relay.RelayOnce(context.Background(), conn)
	if err != nil {
		t.Fatalf("second relay: %v", err)
	}
	if n == 0 {
		t.Fatal("expected events to be published")
	}
	if pub.count() == 0 {
		t.Fatal("publisher was not called")
	}
	pending, _, err := store.OutboxStats(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("expected empty outbox, got %d pending", pending)
	}
}

func TestConsumerDeduplicatesEvents(t *testing.T) {
	_, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "10.00", "USD")

	eventID, _ := uuid.NewV7()
	payload := []byte(`{"event_id":"` + eventID.String() + `"}`)
	first, err := store.ConsumeEvent(context.Background(), "test-consumer", eventID, "transfer.posted", uuid.Nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ConsumeEvent(context.Background(), "test-consumer", eventID, "transfer.posted", uuid.Nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Fatalf("expected first processed and second duplicate, got %v / %v", first, second)
	}
}

type fakePublisher struct {
	mu         sync.Mutex
	pubs       []published
	failures   int
	failMarker string
}

type published struct {
	Topic string
	Key   string
	Value []byte
}

func (f *fakePublisher) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	for _, err := range f.PublishBatch(ctx, topic, []outbox.Message{{Key: key, Value: value, Headers: headers}}) {
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fakePublisher) PublishBatch(_ context.Context, topic string, messages []outbox.Message) []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	errs := make([]error, len(messages))
	if f.failures > 0 {
		f.failures--
		err := errors.New("simulated broker outage")
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	for i, m := range messages {
		if f.failMarker != "" && strings.Contains(string(m.Value), f.failMarker) {
			errs[i] = errors.New("simulated poison record")
			continue
		}
		f.pubs = append(f.pubs, published{Topic: topic, Key: m.Key, Value: append([]byte(nil), m.Value...)})
	}
	return errs
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pubs)
}

func (f *fakePublisher) setFailures(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = n
}

func TestIdempotencyCanonicalAmountAndDefaultHoldExpiry(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	first, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "canonical-amount",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
		Amount: "1.0", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, replayed, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "canonical-amount",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
		Amount: "1.00", Currency: "USD",
	})
	if err != nil || !replayed || second.ID != first.ID {
		t.Fatalf("equivalent amount must replay: replayed=%v err=%v", replayed, err)
	}

	hold1, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "default-expiry", AccountID: mustParse(t, alice.ID),
		Amount: "5.00", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	hold2, replayed, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "default-expiry", AccountID: mustParse(t, alice.ID),
		Amount: "5.00", Currency: "USD",
	})
	if err != nil || !replayed || hold2.ID != hold1.ID {
		t.Fatalf("default hold expiry must replay: replayed=%v err=%v", replayed, err)
	}
}

func TestIdempotencyExpiredRowIsReplaced(t *testing.T) {
	pool, store, _ := setup(t)
	ctx := context.Background()
	now := time.Now().UTC()
	record := idempotency.Record{
		OwnerID: "owner", Key: "ttl-key", RequestHash: []byte("old"), Method: "POST",
		Path: "/v1/transfers", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	var inserted bool
	if err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var err error
		_, inserted, err = store.AcquireIdempotency(ctx, tx, record)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first acquire must insert")
	}
	if _, err := pool.Exec(ctx, `UPDATE idempotency_keys SET expires_at = now() - interval '1 hour' WHERE owner_id = 'owner' AND key = 'ttl-key'`); err != nil {
		t.Fatal(err)
	}
	record.RequestHash = []byte("new")
	if err := pgx.BeginTxFunc(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var err error
		_, inserted, err = store.AcquireIdempotency(ctx, tx, record)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("expired row must be replaced, not replayed")
	}
	var hash []byte
	if err := pool.QueryRow(ctx, `SELECT request_hash FROM idempotency_keys WHERE owner_id = 'owner' AND key = 'ttl-key'`).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if string(hash) != "new" {
		t.Fatalf("hash = %q, want new", hash)
	}
}

func TestListEntriesCursorPagination(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	for i := 0; i < 5; i++ {
		fund(t, svc, system.ID, alice.ID, "1.00", "USD")
	}

	seen := map[string]struct{}{}
	cursor := ""
	for pageNumber := 0; pageNumber < 10; pageNumber++ {
		page, err := svc.ListEntries(context.Background(), "alice", mustParse(t, alice.ID), 2, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			if _, ok := seen[e.ID]; ok {
				t.Fatalf("entry %s returned twice", e.ID)
			}
			seen[e.ID] = struct{}{}
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == "" {
			t.Fatal("has_more page without next_cursor")
		}
		cursor = page.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("entries seen = %d, want 5", len(seen))
	}
}

func TestReconciliationFindsEmptyTransactionsAndBrokenHoldCaptures(t *testing.T) {
	pool, store, svc := setup(t)
	ctx := context.Background()
	emptyTx := uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO journal_transactions (id, type, status) VALUES ($1, 'transfer', 'posted')`, emptyTx); err != nil {
		t.Fatal(err)
	}
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	merchant := createAccount(t, svc, "merchant", "merchant-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "10.00", "USD")
	hold, _, err := svc.CreateHold(ctx, usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "rec-hold", AccountID: mustParse(t, alice.ID),
		Amount: "1.00", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CaptureHold(ctx, usecase.CaptureHoldInput{
		OwnerID: "alice", IdempotencyKey: "rec-capture", HoldID: mustParse(t, hold.ID),
		DestinationAccountID: mustParse(t, merchant.ID),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE holds SET captured_transaction_id = NULL WHERE id = $1`, mustParse(t, hold.ID)); err != nil {
		t.Fatal(err)
	}

	empty, err := store.FindEmptyTransactions(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 1 || empty[0].TransactionID == nil || *empty[0].TransactionID != emptyTx {
		t.Fatalf("unexpected empty transactions: %+v", empty)
	}
	captures, err := store.FindHoldCaptureAnomalies(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(captures) != 1 || captures[0].Details["hold_id"] != hold.ID {
		t.Fatalf("unexpected hold capture anomalies: %+v", captures)
	}
}

func TestTransferEntryCountUsesTransactionAndTransferIDsCorrectly(t *testing.T) {
	pool, store, svc := setup(t)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	ctx := context.Background()
	txID, transferID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `
		INSERT INTO journal_transactions (id, type, status) VALUES ($1, 'transfer', 'posted')`, txID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO transfers (id, owner_id, idempotency_key, transaction_id, source_account_id,
			destination_account_id, amount_minor, currency)
		VALUES ($1, 'alice', 'broken-transfer', $2, $3, $4, 100, 'USD')`,
		transferID, txID, mustParse(t, alice.ID), mustParse(t, bob.ID)); err != nil {
		t.Fatal(err)
	}
	items, err := store.FindTransferEntryCountAnomalies(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one anomaly, got %d", len(items))
	}
	d := items[0]
	if d.TransactionID == nil || *d.TransactionID != txID {
		t.Fatalf("transaction_id = %v, want %s", d.TransactionID, txID)
	}
	if d.Details["transfer_id"] != transferID.String() {
		t.Fatalf("transfer_id detail = %v, want %s", d.Details["transfer_id"], transferID)
	}
}
