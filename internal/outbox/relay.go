package outbox

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yugrinovD/ledger/internal/db"
	"github.com/yugrinovD/ledger/internal/observability"
)

type Message struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	PublishBatch(ctx context.Context, topic string, messages []Message) error
	Close() error
}

type RelayStore interface {
	PollOutbox(ctx context.Context, q db.Querier, limit int) ([]Event, error)
	MarkOutboxPublishedBatch(ctx context.Context, q db.Querier, ids []int64) error
	MarkOutboxFailed(ctx context.Context, q db.Querier, id int64, reason string, nextAttempt time.Duration) error
	MarkOutboxParked(ctx context.Context, q db.Querier, id int64, reason string) error
	OutboxStats(ctx context.Context, q db.Querier) (int64, float64, error)
	OutboxParkedCount(ctx context.Context, q db.Querier) (int64, error)
}

type Relay struct {
	pool        *pgxpool.Pool
	store       RelayStore
	publisher   Publisher
	topic       string
	batchSize   int
	interval    time.Duration
	maxAttempts int
	logger      *slog.Logger
	metrics     *observability.Metrics
}

func NewRelay(pool *pgxpool.Pool, store RelayStore, publisher Publisher, topic string, batchSize int, interval time.Duration, maxAttempts int, logger *slog.Logger, metrics *observability.Metrics) *Relay {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if maxAttempts <= 0 {
		maxAttempts = 20
	}
	return &Relay{
		pool: pool, store: store, publisher: publisher, topic: topic,
		batchSize: batchSize, interval: interval, maxAttempts: maxAttempts,
		logger: logger, metrics: metrics,
	}
}

// Run holds a PostgreSQL advisory lock so exactly one relay is active. The
// leadership connection is also used for every outbox query: if the session
// dies, the lock is released and the old leader can no longer publish.
func (r *Relay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		conn, err := r.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		var locked bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, db.OutboxRelayLockKey).Scan(&locked); err != nil {
			conn.Release()
			return err
		}
		if !locked {
			conn.Release()
			r.logger.Info("outbox relay is on standby; another instance holds the lock")
			if !sleep(ctx, 5*time.Second) {
				return nil
			}
			continue
		}

		r.logger.Info("outbox relay acquired leadership")
		err = r.runLeadership(ctx, conn)
		unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, db.OutboxRelayLockKey)
		cancel()
		conn.Release()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.logger.Error("outbox relay lost leadership; retrying", "err", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
		}
	}
}

func (r *Relay) runLeadership(ctx context.Context, conn *pgxpool.Conn) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if _, err := r.RelayOnce(ctx, conn); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Relay) RelayOnce(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	pending, oldest, err := r.store.OutboxStats(ctx, conn)
	if err == nil {
		r.metrics.OutboxPending.Set(float64(pending))
		r.metrics.OutboxOldestAge.Set(oldest)
	}
	if parked, err := r.store.OutboxParkedCount(ctx, conn); err == nil {
		r.metrics.OutboxParked.Set(float64(parked))
	}
	events, err := r.store.PollOutbox(ctx, conn, r.batchSize)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}
	messages := make([]Message, 0, len(events))
	ids := make([]int64, 0, len(events))
	for _, e := range events {
		messages = append(messages, Message{Key: e.AggregateID.String(), Value: e.Payload, Headers: e.Headers})
		ids = append(ids, e.ID)
	}
	if err := r.publisher.PublishBatch(ctx, r.topic, messages); err != nil {
		r.metrics.OutboxFailures.Add(float64(len(events)))
		reason := truncate(err.Error(), 1000)
		for _, e := range events {
			if e.Attempts+1 >= r.maxAttempts {
				if markErr := r.store.MarkOutboxParked(ctx, conn, e.ID, reason); markErr != nil {
					r.logger.Error("park outbox event", "event_id", e.EventID.String(), "err", markErr)
				}
				r.metrics.OutboxParked.Inc()
				r.logger.Error("outbox event parked after exceeding retry budget",
					"event_id", e.EventID.String(), "attempts", e.Attempts+1, "err", err)
				continue
			}
			backoff := r.backoff(e.Attempts)
			if markErr := r.store.MarkOutboxFailed(ctx, conn, e.ID, reason, backoff); markErr != nil {
				r.logger.Error("mark outbox failure", "event_id", e.EventID.String(), "err", markErr)
			}
		}
		r.logger.Warn("outbox batch publish failed", "count", len(events), "err", err)
		return 0, nil
	}
	if err := r.store.MarkOutboxPublishedBatch(ctx, conn, ids); err != nil {
		return 0, err
	}
	r.metrics.OutboxPublished.Add(float64(len(events)))
	r.logger.Debug("outbox batch published", "count", len(events))
	return len(events), nil
}

func (r *Relay) backoff(attempts int) time.Duration {
	d := 100 * time.Millisecond
	for i := 0; i < attempts && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return strings.ToValidUTF8(s, "")
	}
	if n <= 0 {
		return ""
	}
	return strings.ToValidUTF8(s[:n], "") + "..."
}
