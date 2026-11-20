package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/yugrinovD/ledger/internal/adapter/ratelimit"
)

func TestRedisRateLimiter(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set; skipping Redis integration test")
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	limiter := ratelimit.New(client, 2, time.Minute)
	key := fmt.Sprintf("owner:test:%d", time.Now().UnixNano())
	for i, wantAllowed := range []bool{true, true, false} {
		res, err := limiter.Allow(ctx, key)
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if res.Allowed != wantAllowed {
			t.Fatalf("request %d allowed = %v, want %v", i, res.Allowed, wantAllowed)
		}
	}
}
