package idempotency

import (
	"time"

	"github.com/google/uuid"
)

type Record struct {
	OwnerID      string
	Key          string
	RequestHash  []byte
	Method       string
	Path         string
	StatusCode   *int
	ResponseBody []byte
	ResourceType string
	ResourceID   *uuid.UUID
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

func (r Record) Completed() bool {
	return r.StatusCode != nil && len(r.ResponseBody) > 0
}
