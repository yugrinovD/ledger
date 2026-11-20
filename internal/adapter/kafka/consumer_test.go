package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/outbox"
)

type fakeDLQ struct {
	mu      sync.Mutex
	topics  []string
	keys    []string
	headers []map[string]string
}

func (f *fakeDLQ) Publish(_ context.Context, topic, key string, _ []byte, headers map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = append(f.topics, topic)
	f.keys = append(f.keys, key)
	f.headers = append(f.headers, headers)
	return nil
}

func (f *fakeDLQ) PublishBatch(ctx context.Context, topic string, messages []outbox.Message) []error {
	errs := make([]error, len(messages))
	for i, m := range messages {
		errs[i] = f.Publish(ctx, topic, m.Key, m.Value, m.Headers)
	}
	return errs
}

func (f *fakeDLQ) Close() error { return nil }

func (f *fakeDLQ) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.topics)
}

func newTestConsumer(dlq outbox.Publisher, handler Handler) *Consumer {
	return &Consumer{
		group:       "test-group",
		topic:       "ledger.events.test",
		dlqTopic:    "ledger.events.test.dlq",
		dlq:         dlq,
		handler:     handler,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics:     observability.NewMetrics(prometheus.NewRegistry()),
		maxAttempts: 2,
		tracer:      otel.Tracer("ledger/test"),
	}
}

func TestConsumerMovesFailingEventToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	consumer := newTestConsumer(dlq, func(context.Context, outbox.Envelope) (bool, error) {
		return false, errors.New("simulated handler failure")
	})
	env, err := outbox.NewEnvelope(uuid.New(), "test.event", "test", uuid.New(), time.Now().UTC(), map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	rec := &kgo.Record{
		Topic: "ledger.events.test", Key: []byte("aggregate"),
		Value: env, Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("trace")}},
	}
	if err := consumer.process(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if dlq.count() != 1 {
		t.Fatalf("DLQ publishes = %d, want 1", dlq.count())
	}
	if dlq.topics[0] != "ledger.events.test.dlq" {
		t.Fatalf("topic = %s", dlq.topics[0])
	}
	if dlq.headers[0]["x-original-topic"] != "ledger.events.test" {
		t.Fatalf("missing x-original-topic: %+v", dlq.headers[0])
	}
	if dlq.headers[0]["x-error"] == "" {
		t.Fatal("missing x-error header")
	}
}

func TestConsumerMovesMalformedEnvelopeToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	consumer := newTestConsumer(dlq, func(context.Context, outbox.Envelope) (bool, error) {
		t.Fatal("handler must not be called for malformed events")
		return false, nil
	})
	rec := &kgo.Record{Topic: "ledger.events.test", Value: []byte("{not-json")}
	if err := consumer.process(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if dlq.count() != 1 {
		t.Fatalf("DLQ publishes = %d, want 1", dlq.count())
	}
}

func TestConsumerDoesNotDeadLetterSuccessfulEvent(t *testing.T) {
	dlq := &fakeDLQ{}
	consumer := newTestConsumer(dlq, func(context.Context, outbox.Envelope) (bool, error) {
		return true, nil
	})
	env, err := outbox.NewEnvelope(uuid.New(), "test.event", "test", uuid.New(), time.Now().UTC(), map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.process(context.Background(), &kgo.Record{Value: env}); err != nil {
		t.Fatal(err)
	}
	if dlq.count() != 0 {
		t.Fatalf("DLQ publishes = %d, want 0", dlq.count())
	}
}
