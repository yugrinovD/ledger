package retry

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
}

func Default() Policy {
	return Policy{
		MaxAttempts: 5,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    500 * time.Millisecond,
		Jitter:      0.2,
	}
}

// Do retries fn when it returns a transient PostgreSQL error. The callback is
// invoked before sleeping so callers can record metrics.
func Do(ctx context.Context, p Policy, fn func(context.Context) error, onRetry func(attempt int, err error)) error {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	var last error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		last = err
		if !IsRetryable(err) || attempt == p.MaxAttempts {
			return err
		}
		if onRetry != nil {
			onRetry(attempt, err)
		}
		delay := backoff(p, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return last
}

func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		// serialization_failure, deadlock_detected, lock_not_available
		case "40001", "40P01", "55P03":
			return true
		}
	}
	return false
}

func backoff(p Policy, attempt int) time.Duration {
	factor := math.Pow(2, float64(attempt-1))
	d := time.Duration(float64(p.BaseDelay) * factor)
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if p.Jitter > 0 {
		delta := float64(d) * p.Jitter
		d = time.Duration(float64(d) - delta + rand.Float64()*2*delta)
	}
	if d < 0 {
		return 0
	}
	return d
}
