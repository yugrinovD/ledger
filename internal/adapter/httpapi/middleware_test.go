package httpapi

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/yugrinovD/ledger/internal/adapter/ratelimit"
)

func mustCIDR(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatal(err)
	}
	return network
}

func requestFrom(remote string) *http.Request {
	r := &http.Request{RemoteAddr: remote, Header: http.Header{}}
	r.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	return r
}

func TestClientIPIgnoresUntrustedForwardedFor(t *testing.T) {
	trusted := []*net.IPNet{mustCIDR(t, "10.0.0.0/8")}
	if got := clientIP(requestFrom("198.51.100.5:1234"), trusted); got != "198.51.100.5" {
		t.Fatalf("untrusted remote must not trust XFF, got %q", got)
	}
}

func TestClientIPUsesForwardedForFromTrustedProxy(t *testing.T) {
	trusted := []*net.IPNet{mustCIDR(t, "10.0.0.0/8")}
	if got := clientIP(requestFrom("10.1.2.3:1234"), trusted); got != "203.0.113.7" {
		t.Fatalf("trusted proxy XFF not used, got %q", got)
	}
}

func TestClientIPIgnoresForwardedForByDefault(t *testing.T) {
	if got := clientIP(requestFrom("10.1.2.3:1234"), nil); got != "10.1.2.3" {
		t.Fatalf("default must ignore XFF, got %q", got)
	}
}

func TestRateLimiterFailsOpen(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{
		Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond, WriteTimeout: 50 * time.Millisecond, MaxRetries: 0,
	})
	defer client.Close()
	limiter := ratelimit.New(client, 1, time.Minute)
	called := false
	handler := rateLimitMiddleware(limiter, slog.New(slog.NewTextHandler(io.Discard, nil)), 1, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !called || rec.Code != http.StatusOK {
		t.Fatalf("limiter must fail open: called=%v status=%d", called, rec.Code)
	}
}
