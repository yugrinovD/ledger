package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/usecase"
)

func TestHoldExpiryReleasesReservedFunds(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	expires := time.Now().UTC().Add(20 * time.Millisecond)
	hold, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "expiring-hold", AccountID: mustParse(t, alice.ID),
		Amount: "30.00", Currency: "USD", ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	expired, err := svc.ExpireHold(context.Background(), mustParse(t, hold.ID))
	if err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("hold should have expired")
	}
	if got := balance(t, svc, "alice", alice.ID); got.Held != "0.00" || got.Balance != "100.00" {
		t.Fatalf("unexpected balance after expiry: %+v", got)
	}
}

func TestCaptureFromFrozenAccountIsRejected(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	merchant := createAccount(t, svc, "merchant", "merchant-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	hold, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "frozen-capture-hold", AccountID: mustParse(t, alice.ID),
		Amount: "30.00", Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.FreezeAccount(context.Background(), mustParse(t, alice.ID)); err != nil {
		t.Fatal(err)
	}
	_, _, err = svc.CaptureHold(context.Background(), usecase.CaptureHoldInput{
		OwnerID: "alice", IdempotencyKey: "frozen-capture", HoldID: mustParse(t, hold.ID),
		DestinationAccountID: mustParse(t, merchant.ID),
	})
	if !domain.IsCode(err, domain.CodeAccountNotActive) {
		t.Fatalf("capture from frozen account = %v, want account_not_active", err)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Held != "30.00" || got.Balance != "100.00" {
		t.Fatalf("unexpected balance after rejected capture: %+v", got)
	}
	if _, _, err := svc.ReleaseHold(context.Background(), usecase.ReleaseHoldInput{
		OwnerID: "alice", IdempotencyKey: "frozen-release", HoldID: mustParse(t, hold.ID),
	}); err != nil {
		t.Fatalf("release on frozen account must be allowed: %v", err)
	}
}

func TestCloseAccountWithActiveHoldIsRejected(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")
	if _, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "close-with-hold", AccountID: mustParse(t, alice.ID),
		Amount: "30.00", Currency: "USD",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseAccount(context.Background(), mustParse(t, alice.ID)); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("close with active hold = %v, want conflict", err)
	}
}

func TestCaptureAndExpireAreMutuallyExclusive(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	merchant := createAccount(t, svc, "merchant", "merchant-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	expires := time.Now().UTC().Add(100 * time.Millisecond)
	hold, _, err := svc.CreateHold(context.Background(), usecase.CreateHoldInput{
		OwnerID: "alice", IdempotencyKey: "race-hold", AccountID: mustParse(t, alice.ID),
		Amount: "40.00", Currency: "USD", ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	type captureResult struct {
		status string
		err    error
	}
	captured := make(chan captureResult, 1)
	go func() {
		view, _, err := svc.CaptureHold(context.Background(), usecase.CaptureHoldInput{
			OwnerID: "alice", IdempotencyKey: "race-capture", HoldID: mustParse(t, hold.ID),
			DestinationAccountID: mustParse(t, merchant.ID),
		})
		captured <- captureResult{status: view.Status, err: err}
	}()

	time.Sleep(150 * time.Millisecond)
	expired, err := svc.ExpireHold(context.Background(), mustParse(t, hold.ID))
	if err != nil {
		t.Fatal(err)
	}
	result := <-captured
	if result.err != nil && !domain.IsCode(result.err, domain.CodeHoldNotActive) {
		t.Fatalf("capture failed unexpectedly: %v", result.err)
	}
	if result.err == nil && expired {
		t.Fatal("capture and expiry both won")
	}
	final, err := svc.GetHold(context.Background(), "alice", mustParse(t, hold.ID))
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "captured" && final.Status != "expired" {
		t.Fatalf("unexpected final hold status: %s", final.Status)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Held != "0.00" {
		t.Fatalf("held must be zero, got %+v", got)
	}
}
