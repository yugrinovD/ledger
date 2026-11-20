package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/yugrinovD/ledger/internal/adapter/httpapi"
	"github.com/yugrinovD/ledger/internal/auth"
	"github.com/yugrinovD/ledger/internal/config"
	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/usecase"
)

func TestAdminEndpointsFreezeRevokeAndTenantIsolation(t *testing.T) {
	_, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	const pepper = "integration-pepper"
	const adminToken = "integration-admin-token"
	keyID, plainKey := createAPIKey(t, store, pepper, "alice")
	_, bobKey := createAPIKeyForOwner(t, store, pepper, "bob")

	cfg := config.Config{AuthPepper: pepper, AdminToken: adminToken, RequestTimeout: 5 * time.Second}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	public := httptest.NewServer(httpapi.NewServer(cfg, store, svc, logger, metrics, nil, nil).PublicHandler())
	defer public.Close()
	adminBase := observability.AdminHandler(prometheus.NewRegistry(), store.Ping, false)
	admin := httptest.NewServer(httpapi.NewAdminHandler(adminBase, svc, adminToken, logger))
	defer admin.Close()

	if res := apiGet(t, public.URL+"/v1/reconciliation/runs", plainKey); res.StatusCode != http.StatusNotFound {
		t.Fatalf("public reconciliation should not exist, got %d", res.StatusCode)
	}
	if res := adminGet(t, admin.URL+"/admin/reconciliation/runs", ""); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("admin without token = %d, want 401", res.StatusCode)
	}
	if res := adminGet(t, admin.URL+"/admin/reconciliation/runs", adminToken); res.StatusCode != http.StatusOK {
		t.Fatalf("admin with token = %d body=%s", res.StatusCode, res.Body)
	}

	if res := adminPost(t, admin.URL+"/admin/accounts/"+alice.ID+"/freeze", adminToken); res.StatusCode != http.StatusOK {
		t.Fatalf("freeze = %d body=%s", res.StatusCode, res.Body)
	}
	transfer := apiPost(t, public.URL+"/v1/transfers", plainKey, "frozen-source", map[string]any{
		"source_account_id": alice.ID, "destination_account_id": bob.ID,
		"amount": "1.00", "currency": "USD",
	})
	if transfer.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("frozen transfer = %d body=%s", transfer.StatusCode, transfer.Body)
	}
	if res := adminPost(t, admin.URL+"/admin/accounts/"+alice.ID+"/unfreeze", adminToken); res.StatusCode != http.StatusOK {
		t.Fatalf("unfreeze = %d body=%s", res.StatusCode, res.Body)
	}
	transfer = apiPost(t, public.URL+"/v1/transfers", plainKey, "after-unfreeze", map[string]any{
		"source_account_id": alice.ID, "destination_account_id": bob.ID,
		"amount": "1.00", "currency": "USD",
	})
	if transfer.StatusCode != http.StatusCreated {
		t.Fatalf("transfer after unfreeze = %d body=%s", transfer.StatusCode, transfer.Body)
	}

	if res := apiGet(t, public.URL+"/v1/accounts/"+bob.ID, plainKey); res.StatusCode != http.StatusNotFound {
		t.Fatalf("alice key read bob account = %d, want 404", res.StatusCode)
	}
	if res := apiGet(t, public.URL+"/v1/accounts/"+bob.ID, bobKey); res.StatusCode != http.StatusOK {
		t.Fatalf("bob key read bob account = %d", res.StatusCode)
	}

	if res := adminPost(t, admin.URL+"/admin/api-keys/"+keyID.String()+"/revoke", adminToken); res.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke = %d body=%s", res.StatusCode, res.Body)
	}
	if res := apiGet(t, public.URL+"/v1/accounts/"+alice.ID, plainKey); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked key = %d, want 401", res.StatusCode)
	}
}

func TestReconciliationNotFoundAndEmptyDiscrepancies(t *testing.T) {
	_, store, svc := setup(t)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	_ = alice
	run, err := svc.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if run.Run.FinishedAt == nil {
		t.Fatal("finished_at must be set in the returned run")
	}
	if _, _, err := svc.GetReconciliationRun(context.Background(), uuid.New()); err == nil {
		t.Fatal("expected not found for missing run")
	}

	const adminToken = "integration-admin-token"
	// Build a small admin handler over the same service.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	adminBase := observability.AdminHandler(prometheus.NewRegistry(), store.Ping, false)
	admin := httptest.NewServer(httpapi.NewAdminHandler(adminBase, svc, adminToken, logger))
	defer admin.Close()

	res := adminGet(t, admin.URL+"/admin/reconciliation/runs/"+run.Run.ID.String(), adminToken)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("get run = %d body=%s", res.StatusCode, res.Body)
	}
	var payload struct {
		Run           map[string]any   `json:"run"`
		Discrepancies []map[string]any `json:"discrepancies"`
	}
	if err := json.Unmarshal(res.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Discrepancies == nil {
		t.Fatal("discrepancies must be [] not null")
	}
	if res := adminGet(t, admin.URL+"/admin/reconciliation/runs/"+uuid.NewString(), adminToken); res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing run = %d, want 404", res.StatusCode)
	}
}

func createAPIKey(t *testing.T, store interface {
	InsertAPIKey(context.Context, uuid.UUID, string, string, string, []byte, []string) error
}, pepper, owner string) (uuid.UUID, string) {
	return createAPIKeyWithScopes(t, store, pepper, owner, []string{"*"})
}

func createAPIKeyForOwner(t *testing.T, store interface {
	InsertAPIKey(context.Context, uuid.UUID, string, string, string, []byte, []string) error
}, pepper, owner string) (uuid.UUID, string) {
	return createAPIKeyWithScopes(t, store, pepper, owner, []string{"accounts:read", "transfers:write", "holds:write", "holds:read"})
}

func createAPIKeyWithScopes(t *testing.T, store interface {
	InsertAPIKey(context.Context, uuid.UUID, string, string, string, []byte, []string) error
}, pepper, owner string, scopes []string) (uuid.UUID, string) {
	t.Helper()
	plain, prefix, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := auth.Hash(pepper, plain)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if err := store.InsertAPIKey(context.Background(), id, owner, "test-"+owner, prefix, hash, scopes); err != nil {
		t.Fatal(err)
	}
	return id, plain
}

func adminGet(t *testing.T, url, token string) httpResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return doRequest(t, req)
}

func adminPost(t *testing.T, url, token string) httpResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return doRequest(t, req)
}

var _ = usecase.AccountView{}
