package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID            int64
	EventID       uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	Type          string
	Payload       json.RawMessage
	Headers       map[string]string
	CreatedAt     time.Time
	AvailableAt   time.Time
	Attempts      int
	LastError     string
	ParkedAt      *time.Time
}

// Envelope is the stable wire format for every ledger event. Consumers must be
// able to add fields without breaking; event type + version identify the schema.
type Envelope struct {
	EventID       string          `json:"event_id"`
	Type          string          `json:"type"`
	Version       int             `json:"version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Data          json.RawMessage `json:"data"`
}

func NewEnvelope(eventID uuid.UUID, eventType string, aggregateType string, aggregateID uuid.UUID, occurredAt time.Time, data any) (json.RawMessage, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Envelope{
		EventID:       eventID.String(),
		Type:          eventType,
		Version:       1,
		OccurredAt:    occurredAt.UTC(),
		AggregateType: aggregateType,
		AggregateID:   aggregateID.String(),
		Data:          raw,
	})
}
