package config

import (
	"os"
	"strings"
	"testing"
)

func cleanEnv(t *testing.T) {
	t.Helper()
	saved := map[string]string{}
	for _, item := range os.Environ() {
		key, value, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "LEDGER_") {
			saved[key] = value
			_ = os.Unsetenv(key)
		}
	}
	t.Cleanup(func() {
		for key, value := range saved {
			_ = os.Setenv(key, value)
		}
	})
}

func TestLoadDefaults(t *testing.T) {
	cleanEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Env != "dev" || cfg.DBMaxConns != 20 || cfg.OutboxBatchSize != 1000 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.IsProduction() {
		t.Fatal("dev must not be production")
	}
	if len(cfg.Warnings()) == 0 {
		t.Fatal("expected development secret warnings")
	}
}

func TestLoadRejectsInvalidPrimitive(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LEDGER_OUTBOX_BATCH_SIZE", "many")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid integer to fail")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LEDGER_REQUEST_TIMEOUT", "soon")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid duration to fail")
	}
}

func TestLoadRejectsInvalidTrustedProxy(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LEDGER_TRUSTED_PROXIES", "10.0.0.0/8,not-a-cidr")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid CIDR to fail")
	}
}

func TestLoadRejectsTooFewDBConnections(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LEDGER_DB_MAX_CONNS", "1")
	if _, err := Load(); err == nil {
		t.Fatal("expected DB pool smaller than 2 to fail")
	}
}

func TestProductionRequiresStrongSecrets(t *testing.T) {
	cleanEnv(t)
	t.Setenv("LEDGER_ENV", "production")
	if _, err := Load(); err == nil {
		t.Fatal("expected default secrets to be rejected in production")
	}
	t.Setenv("LEDGER_AUTH_PEPPER", strings.Repeat("a", 32))
	t.Setenv("LEDGER_ADMIN_TOKEN", strings.Repeat("b", 32))
	if _, err := Load(); err != nil {
		t.Fatalf("strong production secrets rejected: %v", err)
	}
}
