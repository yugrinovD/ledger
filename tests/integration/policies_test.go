package integration_test

import (
	"context"
	"testing"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/usecase"
)

func TestAllowNegativeCurrencyMismatchAndClosedAccounts(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	eur := createAccount(t, svc, "alice", "alice-eur", "EUR", "liability", false)
	zero := createAccount(t, svc, "alice", "zero-wallet", "USD", "liability", false)

	// A system account with allow_negative can fund from a zero balance.
	if _, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "system", IdempotencyKey: "allow-negative",
		SourceAccountID: mustParse(t, system.ID), DestinationAccountID: mustParse(t, alice.ID),
		Amount: "5.00", Currency: "USD",
	}); err != nil {
		t.Fatalf("allow-negative transfer failed: %v", err)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "5.00" {
		t.Fatalf("alice balance = %s, want 5.00", got)
	}

	_, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "currency-mismatch",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, eur.ID),
		Amount: "1.00", Currency: "USD",
	})
	if !domain.IsCode(err, domain.CodeCurrencyMismatch) {
		t.Fatalf("currency mismatch error = %v", err)
	}

	if _, err := svc.CloseAccount(context.Background(), mustParse(t, alice.ID)); !domain.IsCode(err, domain.CodeConflict) {
		t.Fatalf("closing a funded account = %v, want conflict", err)
	}
	closed, err := svc.CloseAccount(context.Background(), mustParse(t, zero.ID))
	if err != nil {
		t.Fatalf("close zero account: %v", err)
	}
	if closed.Status != "closed" {
		t.Fatalf("status = %s, want closed", closed.Status)
	}
	_, _, err = svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "to-closed",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, zero.ID),
		Amount: "1.00", Currency: "USD",
	})
	if !domain.IsCode(err, domain.CodeAccountNotActive) {
		t.Fatalf("transfer to closed account = %v", err)
	}
}

func TestCrossTenantDestinationIsAllowed(t *testing.T) {
	_, _, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "10.00", "USD")

	if _, _, err := svc.CreateTransfer(context.Background(), usecase.CreateTransferInput{
		OwnerID: "alice", IdempotencyKey: "cross-tenant",
		SourceAccountID: mustParse(t, alice.ID), DestinationAccountID: mustParse(t, bob.ID),
		Amount: "2.00", Currency: "USD",
	}); err != nil {
		t.Fatalf("cross-tenant destination rejected: %v", err)
	}
	if got := balance(t, svc, "bob", bob.ID).Balance; got != "2.00" {
		t.Fatalf("bob balance = %s, want 2.00", got)
	}
	if got := balance(t, svc, "alice", alice.ID).Balance; got != "8.00" {
		t.Fatalf("alice balance = %s, want 8.00", got)
	}
}
