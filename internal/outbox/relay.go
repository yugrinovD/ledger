package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
	PollOutbox(ctx context.Context, limit int) ([]Event, error)
	MarkOutboxPublishedBatch(ctx context.Context, ids []int64) error
	MarkOutboxFailed(ctx context.Context, id int64, reason string, nextAttempt time.Duration) error
	OutboxStats(ctx context.Context) (int64, float64, error)
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

// Run holds a PostgreSQL advisory lock so exactly one relay is active. A second
// worker stays in standby and takes over automatically after the leader exits.
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
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('ledger_outbox_relay'))`).Scan(&locked); err != nil {
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
		ticker := time.NewTicker(r.interval)
		for {
			if _, err := r.RelayOnce(ctx); err != nil {
				r.logger.Error("outbox relay iteration failed", "err", err)
			}
			select {
			case <-ctx.Done():
				ticker.Stop()
				unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtext('ledger_outbox_relay'))`)
				cancel()
				conn.Release()
				return nil
			case <-ticker.C:
			}
		}
	}
}

func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	pending, oldest, err := r.store.OutboxStats(ctx)
	if err == nil {
		r.metrics.OutboxPending.Set(float64(pending))
		r.metrics.OutboxOldestAge.Set(oldest)
	}
	events, err := r.store.PollOutbox(ctx, r.batchSize)
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
		for _, e := range events {
			backoff := r.backoff(e.Attempts)
			if markErr := r.store.MarkOutboxFailed(ctx, e.ID, truncate(err.Error(), 1000), backoff); markErr != nil {
				r.logger.Error("mark outbox failure", "event_id", e.EventID.String(), "err", markErr)
			}
			if e.Attempts+1 >= r.maxAttempts {
				r.logger.Error("outbox event exceeded retry budget; keeping it pending with capped backoff",
					"event_id", e.EventID.String(), "attempts", e.Attempts+1, "err", err)
			}
		}
		r.logger.Warn("outbox batch publish failed", "count", len(events), "err", err)
		return 0, nil
	}
	if err := r.store.MarkOutboxPublishedBatch(ctx, ids); err != nil {
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
		return s
	}
	return s[:n] + "..."
}
