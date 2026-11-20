package reconciliation

import (
	"time"

	"github.com/google/uuid"
)

type Discrepancy struct {
	ID            int64          `json:"id,omitempty"`
	Kind          string         `json:"kind"`
	AccountID     *uuid.UUID     `json:"account_id,omitempty"`
	TransactionID *uuid.UUID     `json:"transaction_id,omitempty"`
	Expected      *int64         `json:"expected,omitempty"`
	Actual        *int64         `json:"actual,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

type Run struct {
	ID               uuid.UUID      `json:"id"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       *time.Time     `json:"finished_at,omitempty"`
	Status           string         `json:"status"`
	CheckedAccounts  int64          `json:"checked_accounts"`
	DiscrepancyCount int64          `json:"discrepancy_count"`
	Error            string         `json:"error,omitempty"`
	Stats            map[string]any `json:"stats"`
}
