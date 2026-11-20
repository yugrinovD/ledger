package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yugrinovD/ledger/internal/outbox"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string, clientID string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID+"-producer"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	for _, err := range p.PublishBatch(ctx, topic, []outbox.Message{{Key: key, Value: value, Headers: headers}}) {
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, topic string, messages []outbox.Message) []error {
	if len(messages) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(messages))
	for _, m := range messages {
		records = append(records, &kgo.Record{
			Topic:   topic,
			Key:     []byte(m.Key),
			Value:   m.Value,
			Headers: recordHeaders(m.Headers),
		})
	}
	results := p.client.ProduceSync(ctx, records...)
	errs := make([]error, len(messages))
	if len(results) != len(messages) {
		err := fmt.Errorf("kafka producer returned %d results for %d messages", len(results), len(messages))
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	for i := range results {
		errs[i] = results[i].Err
	}
	return errs
}

func (p *Producer) Close() error {
	p.client.Close()
	return nil
}

func recordHeaders(in map[string]string) []kgo.RecordHeader {
	if len(in) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(in))
	for k, v := range in {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

var _ outbox.Publisher = (*Producer)(nil)

func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }
