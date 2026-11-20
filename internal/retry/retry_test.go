package retry

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryable(t *testing.T) {
	t.Parallel()
	for _, code := range []string{"40001", "40P01", "55P03"} {
		if !IsRetryable(&pgconn.PgError{Code: code}) {
			t.Fatalf("code %s must be retryable", code)
		}
	}
	if IsRetryable(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("unique violation must not be retried")
	}
	if IsRetryable(context.Canceled) {
		t.Fatal("canceled context must not be retried")
	}
	if IsRetryable(errors.New("boom")) {
		t.Fatal("generic error must not be retried")
	}
}

func TestDoRetriesTransientErrors(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Do(context.Background(), Policy{MaxAttempts: 5, BaseDelay: 1, MaxDelay: 2}, func(context.Context) error {
		calls++
		if calls < 3 {
			return &pgconn.PgError{Code: "40001"}
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDoStopsOnPermanentError(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Do(context.Background(), Policy{MaxAttempts: 5, BaseDelay: 1}, func(context.Context) error {
		calls++
		return errors.New("permanent")
	}, nil)
	if err == nil || calls != 1 {
		t.Fatalf("expected one attempt, got %d (err=%v)", calls, err)
	}
}
