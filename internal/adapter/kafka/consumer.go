package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/yugrinovD/ledger/internal/observability"
	"github.com/yugrinovD/ledger/internal/outbox"
)

type Handler func(ctx context.Context, env outbox.Envelope) (processed bool, err error)

type Consumer struct {
	client      *kgo.Client
	group       string
	topic       string
	dlqTopic    string
	dlq         outbox.Publisher
	handler     Handler
	logger      *slog.Logger
	metrics     *observability.Metrics
	maxAttempts int
	tracer      trace.Tracer
}

func NewConsumer(brokers []string, clientID, group, topic, dlqTopic string, dlq outbox.Publisher, handler Handler, logger *slog.Logger, metrics *observability.Metrics) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID+"-consumer"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{
		client: client, group: group, topic: topic, dlqTopic: dlqTopic, dlq: dlq,
		handler: handler, logger: logger, metrics: metrics, maxAttempts: 3,
		tracer: otel.Tracer("ledger/kafka"),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("kafka consumer started", "topic", c.topic, "group", c.group)
	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.Error("kafka fetch error", "topic", topic, "partition", partition, "err", err)
		})
		var toCommit []*kgo.Record
		var processErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if processErr != nil {
				return
			}
			if err := c.process(ctx, rec); err != nil {
				processErr = err
				return
			}
			toCommit = append(toCommit, rec)
		})
		if processErr != nil {
			return processErr
		}
		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				return fmt.Errorf("commit kafka offsets: %w", err)
			}
		}
	}
}

func (c *Consumer) process(ctx context.Context, rec *kgo.Record) error {
	headers := headerMap(rec.Headers)
	carrier := propagation.MapCarrier(headers)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := c.tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.Int("messaging.kafka.partition", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
	defer span.End()

	var env outbox.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "poison").Inc()
		return c.deadLetter(ctx, rec, headers, fmt.Errorf("decode envelope: %w", err))
	}
	if _, err := uuid.Parse(env.EventID); err != nil {
		c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "poison").Inc()
		return c.deadLetter(ctx, rec, headers, fmt.Errorf("invalid event_id: %w", err))
	}

	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		processed, err := c.handler(ctx, env)
		if err == nil {
			if processed {
				c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "processed").Inc()
			} else {
				c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "duplicate").Inc()
			}
			return nil
		}
		lastErr = err
		c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "retried").Inc()
		c.logger.Warn("consumer handler failed",
			"event_id", env.EventID, "attempt", attempt, "err", err)
		if attempt < c.maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 100 * time.Millisecond):
			}
		}
	}
	c.metrics.ConsumerProcessed.WithLabelValues(c.topic, "dead_letter").Inc()
	return c.deadLetter(ctx, rec, headers, lastErr)
}

func (c *Consumer) deadLetter(ctx context.Context, rec *kgo.Record, headers map[string]string, cause error) error {
	dlqHeaders := make(map[string]string, len(headers)+2)
	for k, v := range headers {
		dlqHeaders[k] = v
	}
	dlqHeaders["x-original-topic"] = rec.Topic
	dlqHeaders["x-error"] = cause.Error()
	if err := c.dlq.Publish(ctx, c.dlqTopic, string(rec.Key), rec.Value, dlqHeaders); err != nil {
		return fmt.Errorf("publish to dead letter topic: %w", err)
	}
	c.logger.Error("event moved to dead letter topic",
		"topic", c.dlqTopic, "original_topic", rec.Topic, "offset", rec.Offset, "err", cause)
	return nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

func headerMap(headers []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[string(h.Key)] = string(h.Value)
	}
	return out
}
