package integration_test

import (
	"bytes"
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

func TestHTTPAPIFlowAndIdempotency(t *testing.T) {
	_, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	pepper := "integration-pepper"
	plainKey, prefix, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := auth.Hash(pepper, plainKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAPIKey(context.Background(), uuid.New(), "alice", "test", prefix, hash, []string{"*"}); err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{AuthPepper: pepper, RequestTimeout: 5 * time.Second, RateLimitRPM: 0}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	server := httptest.NewServer(httpapi.NewServer(cfg, store, svc, logger, metrics, nil, nil).PublicHandler())
	defer server.Close()

	bob := apiCreateAccount(t, server.URL, plainKey, "bob-wallet")
	if bob.ID == alice.ID {
		t.Fatal("unexpected account id")
	}

	body := map[string]any{
		"source_account_id":      alice.ID,
		"destination_account_id": bob.ID,
		"amount":                 "12.34",
		"currency":               "USD",
		"description":            "api transfer",
	}
	first := apiPost(t, server.URL+"/v1/transfers", plainKey, "api-idem-1", body)
	if first.StatusCode != http.StatusCreated {
		t.Fatalf("create transfer status = %d body=%s", first.StatusCode, first.Body)
	}
	var transfer usecase.TransferView
	if err := json.Unmarshal(first.Body, &transfer); err != nil {
		t.Fatal(err)
	}
	second := apiPost(t, server.URL+"/v1/transfers", plainKey, "api-idem-1", body)
	if second.StatusCode != http.StatusCreated {
		t.Fatalf("replay status = %d body=%s", second.StatusCode, second.Body)
	}
	if second.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatal("replay header is missing")
	}
	var replayed usecase.TransferView
	if err := json.Unmarshal(second.Body, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ID != transfer.ID {
		t.Fatalf("replay returned a different transfer: %s vs %s", replayed.ID, transfer.ID)
	}

	conflict := apiPost(t, server.URL+"/v1/transfers", plainKey, "api-idem-1", map[string]any{
		"source_account_id": alice.ID, "destination_account_id": bob.ID,
		"amount": "99.00", "currency": "USD",
	})
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("key reuse status = %d body=%s", conflict.StatusCode, conflict.Body)
	}

	missing := apiPost(t, server.URL+"/v1/transfers", plainKey, "", body)
	if missing.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing idempotency key status = %d body=%s", missing.StatusCode, missing.Body)
	}

	insufficient := apiPost(t, server.URL+"/v1/transfers", plainKey, "api-idem-insufficient", map[string]any{
		"source_account_id": alice.ID, "destination_account_id": bob.ID,
		"amount": "1000.00", "currency": "USD",
	})
	if insufficient.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("insufficient funds status = %d body=%s", insufficient.StatusCode, insufficient.Body)
	}
	var problem map[string]any
	if err := json.Unmarshal(insufficient.Body, &problem); err != nil {
		t.Fatal(err)
	}
	if problem["code"] != "insufficient_funds" {
		t.Fatalf("unexpected problem code: %v", problem["code"])
	}

	unauthorized := apiPost(t, server.URL+"/v1/transfers", "", "unauth", body)
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.StatusCode)
	}

	bal := apiGet(t, server.URL+"/v1/accounts/"+alice.ID+"/balance", plainKey)
	if bal.StatusCode != http.StatusOK {
		t.Fatalf("balance status = %d body=%s", bal.StatusCode, bal.Body)
	}
	var balance usecase.BalanceView
	if err := json.Unmarshal(bal.Body, &balance); err != nil {
		t.Fatal(err)
	}
	if balance.Balance != "87.66" {
		t.Fatalf("balance = %s, want 87.66", balance.Balance)
	}
}

type httpResult struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func apiCreateAccount(t *testing.T, baseURL, key, name string) usecase.AccountView {
	t.Helper()
	res := apiPost(t, baseURL+"/v1/accounts", key, "api-account-"+name, map[string]any{
		"name": name, "currency": "USD", "type": "liability",
	})
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create account status = %d body=%s", res.StatusCode, res.Body)
	}
	var view usecase.AccountView
	if err := json.Unmarshal(res.Body, &view); err != nil {
		t.Fatal(err)
	}
	return view
}

func apiPost(t *testing.T, url, key, idempotencyKey string, body any) httpResult {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return doRequest(t, req)
}

func apiGet(t *testing.T, url, key string) httpResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return doRequest(t, req)
}

func doRequest(t *testing.T, req *http.Request) httpResult {
	t.Helper()
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return httpResult{StatusCode: res.StatusCode, Header: res.Header.Clone(), Body: body}
}

func TestHTTPHoldLifecycle(t *testing.T) {
	_, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	merchant := createAccount(t, svc, "merchant", "merchant-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "100.00", "USD")

	pepper := "hold-pepper"
	_, plainKey := createAPIKeyForOwner(t, store, pepper, "alice")
	cfg := config.Config{AuthPepper: pepper, RequestTimeout: 5 * time.Second}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	server := httptest.NewServer(httpapi.NewServer(cfg, store, svc, logger, metrics, nil, nil).PublicHandler())
	defer server.Close()

	create := apiPost(t, server.URL+"/v1/holds", plainKey, "http-hold-1", map[string]any{
		"account_id": alice.ID, "amount": "25.00", "currency": "USD",
	})
	if create.StatusCode != http.StatusCreated {
		t.Fatalf("create hold = %d body=%s", create.StatusCode, create.Body)
	}
	if create.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("security headers are missing")
	}
	var hold usecase.HoldView
	if err := json.Unmarshal(create.Body, &hold); err != nil {
		t.Fatal(err)
	}
	if got := apiGet(t, server.URL+"/v1/holds/"+hold.ID, plainKey); got.StatusCode != http.StatusOK {
		t.Fatalf("get hold = %d body=%s", got.StatusCode, got.Body)
	}

	release := apiPost(t, server.URL+"/v1/holds/"+hold.ID+"/release", plainKey, "http-release-1", map[string]any{})
	if release.StatusCode != http.StatusOK {
		t.Fatalf("release hold = %d body=%s", release.StatusCode, release.Body)
	}

	create = apiPost(t, server.URL+"/v1/holds", plainKey, "http-hold-2", map[string]any{
		"account_id": alice.ID, "amount": "10.00", "currency": "USD",
	})
	if create.StatusCode != http.StatusCreated {
		t.Fatalf("create second hold = %d body=%s", create.StatusCode, create.Body)
	}
	if err := json.Unmarshal(create.Body, &hold); err != nil {
		t.Fatal(err)
	}
	capture := apiPost(t, server.URL+"/v1/holds/"+hold.ID+"/capture", plainKey, "http-capture-1", map[string]any{
		"destination_account_id": merchant.ID,
	})
	if capture.StatusCode != http.StatusOK {
		t.Fatalf("capture hold = %d body=%s", capture.StatusCode, capture.Body)
	}
	var captured usecase.HoldView
	if err := json.Unmarshal(capture.Body, &captured); err != nil {
		t.Fatal(err)
	}
	if captured.Status != "captured" {
		t.Fatalf("hold status = %s, want captured", captured.Status)
	}
	if got := balance(t, svc, "alice", alice.ID); got.Balance != "90.00" || got.Held != "0.00" {
		t.Fatalf("unexpected alice balance: %+v", got)
	}
}

func TestHTTPCreateAccountWithAllowNegative(t *testing.T) {
	_, store, svc := setup(t)
	pepper := "allow-negative-pepper"
	_, plainKey := createAPIKey(t, store, pepper, "system")
	cfg := config.Config{AuthPepper: pepper, RequestTimeout: 5 * time.Second}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	server := httptest.NewServer(httpapi.NewServer(cfg, store, svc, logger, metrics, nil, nil).PublicHandler())
	defer server.Close()

	res := apiPost(t, server.URL+"/v1/accounts", plainKey, "http-allow-negative", map[string]any{
		"name": "system-http", "currency": "USD", "type": "equity", "allow_negative": true,
	})
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create account = %d body=%s", res.StatusCode, res.Body)
	}
	var account usecase.AccountView
	if err := json.Unmarshal(res.Body, &account); err != nil {
		t.Fatal(err)
	}
	if !account.AllowNegative {
		t.Fatal("allow_negative was not persisted")
	}
}

func TestMissingScopeReturnsForbidden(t *testing.T) {
	_, store, svc := setup(t)
	system := createAccount(t, svc, "system", "system-equity", "USD", "equity", true)
	alice := createAccount(t, svc, "alice", "alice-wallet", "USD", "liability", false)
	bob := createAccount(t, svc, "bob", "bob-wallet", "USD", "liability", false)
	fund(t, svc, system.ID, alice.ID, "10.00", "USD")

	_, readOnlyKey := createAPIKeyWithScopes(t, store, "scope-pepper", "alice", []string{"accounts:read"})
	cfg := config.Config{AuthPepper: "scope-pepper", RequestTimeout: 5 * time.Second}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics(prometheus.NewRegistry())
	server := httptest.NewServer(httpapi.NewServer(cfg, store, svc, logger, metrics, nil, nil).PublicHandler())
	defer server.Close()

	res := apiPost(t, server.URL+"/v1/transfers", readOnlyKey, "forbidden-transfer", map[string]any{
		"source_account_id": alice.ID, "destination_account_id": bob.ID,
		"amount": "1.00", "currency": "USD",
	})
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("missing scope = %d body=%s, want 403", res.StatusCode, res.Body)
	}
}
