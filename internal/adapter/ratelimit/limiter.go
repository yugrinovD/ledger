package ratelimit

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

var script = goredis.NewScript(`
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('PTTL', KEYS[1])
return {current, ttl}
`)

type Result struct {
	Allowed   bool
	Remaining int
	Reset     time.Duration
}

type Limiter struct {
	client *goredis.Client
	limit  int
	window time.Duration
}

func New(client *goredis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{client: client, limit: limit, window: window}
}

func (l *Limiter) Allow(ctx context.Context, key string) (Result, error) {
	if l == nil {
		return Result{Allowed: true}, nil
	}
	if l.client == nil || l.limit <= 0 {
		return Result{Allowed: true, Remaining: l.limit}, nil
	}
	res, err := script.Run(ctx, l.client, []string{"ratelimit:" + key}, l.window.Milliseconds()).Result()
	if err != nil {
		return Result{Allowed: true}, fmt.Errorf("rate limiter: %w", err)
	}
	values, ok := res.([]any)
	if !ok || len(values) < 2 {
		return Result{Allowed: true}, fmt.Errorf("rate limiter: unexpected redis response")
	}
	current := toInt64(values[0])
	ttlMS := toInt64(values[1])
	remaining := l.limit - int(current)
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:   current <= int64(l.limit),
		Remaining: remaining,
		Reset:     time.Duration(ttlMS) * time.Millisecond,
	}, nil
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case []byte:
		var out int64
		_, _ = fmt.Sscan(string(n), &out)
		return out
	default:
		return 0
	}
}
