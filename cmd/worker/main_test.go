package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeHoldExpirer struct {
	expired chan uuid.UUID
}

func (f *fakeHoldExpirer) ListExpiredHoldIDs(context.Context, int) ([]uuid.UUID, error) {
	return []uuid.UUID{uuid.New()}, nil
}

func (f *fakeHoldExpirer) ExpireHold(_ context.Context, id uuid.UUID) (bool, error) {
	select {
	case f.expired <- id:
	default:
	}
	return true, nil
}

func TestRunHoldSweeperExpiresHolds(t *testing.T) {
	fake := &fakeHoldExpirer{expired: make(chan uuid.UUID, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runHoldSweeper(ctx, fake, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond)
	}()

	select {
	case <-fake.expired:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper did not expire a hold")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper did not stop")
	}
}
