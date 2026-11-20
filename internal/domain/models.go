package domain

import (
	"time"

	"github.com/google/uuid"
)

type Direction string

const (
	Debit  Direction = "debit"
	Credit Direction = "credit"
)

func (d Direction) Valid() bool { return d == Debit || d == Credit }

func (d Direction) Opposite() Direction {
	if d == Debit {
		return Credit
	}
	return Debit
}

type AccountType string

const (
	AccountAsset     AccountType = "asset"
	AccountLiability AccountType = "liability"
	AccountEquity    AccountType = "equity"
	AccountRevenue   AccountType = "revenue"
	AccountExpense   AccountType = "expense"
)

func ParseAccountType(v string) (AccountType, error) {
	switch AccountType(v) {
	case AccountAsset, AccountLiability, AccountEquity, AccountRevenue, AccountExpense:
		return AccountType(v), nil
	default:
		return "", Validation("unknown account type %q", v)
	}
}

// NormalSide is the side on which an account type increases. Assets and
// expenses are debit-normal; liabilities, equity and revenue are credit-normal.
func (t AccountType) NormalSide() Direction {
	switch t {
	case AccountAsset, AccountExpense:
		return Debit
	default:
		return Credit
	}
}

type AccountStatus string

const (
	AccountActive AccountStatus = "active"
	AccountFrozen AccountStatus = "frozen"
	AccountClosed AccountStatus = "closed"
)

type Account struct {
	ID            uuid.UUID
	OwnerID       string
	Name          string
	Currency      Currency
	Type          AccountType
	Status        AccountStatus
	AllowNegative bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (a Account) NormalSide() Direction { return a.Type.NormalSide() }

func (a Account) Spendable() bool {
	return a.Status == AccountActive
}

// Balance is the normal-side projection of an account. For a liability account
// a positive balance means the customer has funds; for an asset account it means
// the ledger holds funds.
type Balance struct {
	AccountID uuid.UUID
	Balance   int64
	Held      int64
	Version   int64
	UpdatedAt time.Time
}

func (b Balance) Available() int64 { return b.Balance - b.Held }

type TransactionType string

const (
	TransactionTransfer    TransactionType = "transfer"
	TransactionHoldCapture TransactionType = "hold_capture"
	TransactionAdjustment  TransactionType = "adjustment"
	TransactionFee         TransactionType = "fee"
	TransactionReversal    TransactionType = "reversal"
)

type JournalTransaction struct {
	ID          uuid.UUID
	Type        TransactionType
	Status      string
	Description string
	Metadata    map[string]any
	OccurredAt  time.Time
	CreatedAt   time.Time
}

type Entry struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	AccountID     uuid.UUID
	Direction     Direction
	AmountMinor   int64
	Currency      Currency
	CreatedAt     time.Time
}

type TransferStatus string

const (
	TransferPosted   TransferStatus = "posted"
	TransferReversed TransferStatus = "reversed"
)

type Transfer struct {
	ID                   uuid.UUID
	OwnerID              string
	IdempotencyKey       string
	TransactionID        uuid.UUID
	SourceAccountID      uuid.UUID
	DestinationAccountID uuid.UUID
	AmountMinor          int64
	Currency             Currency
	Description          string
	Status               TransferStatus
	CreatedAt            time.Time
}

type HoldStatus string

const (
	HoldActive   HoldStatus = "active"
	HoldCaptured HoldStatus = "captured"
	HoldReleased HoldStatus = "released"
	HoldExpired  HoldStatus = "expired"
)

type Hold struct {
	ID                    uuid.UUID
	OwnerID               string
	IdempotencyKey        string
	AccountID             uuid.UUID
	AmountMinor           int64
	Currency              Currency
	Status                HoldStatus
	ExpiresAt             time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CapturedTransactionID *uuid.UUID
}
