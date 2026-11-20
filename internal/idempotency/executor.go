package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yugrinovD/ledger/internal/domain"
	"github.com/yugrinovD/ledger/internal/retry"
)

type Store interface {
	AcquireIdempotency(ctx context.Context, tx pgx.Tx, r Record) (Record, bool, error)
	CompleteIdempotency(ctx context.Context, tx pgx.Tx, ownerID, key string, statusCode int, body []byte, resourceType string, resourceID *uuid.UUID) error
}

type Result struct {
	StatusCode   int
	Body         []byte
	ResourceType string
	ResourceID   *uuid.UUID
	Replayed     bool
}

type Executor struct {
	pool     *pgxpool.Pool
	store    Store
	ttl      time.Duration
	onReplay func(endpoint string)
	onRetry  func(attempt int, err error)
}

func NewExecutor(pool *pgxpool.Pool, store Store, ttl time.Duration, onReplay func(string), onRetry func(int, error)) *Executor {
	return &Executor{pool: pool, store: store, ttl: ttl, onReplay: onReplay, onRetry: onRetry}
}

// Execute runs fn exactly once per (owner, key) pair. The idempotency row, the
// business transaction and the outbox event are committed atomically, so there
// is no window where a key exists without its effect or vice versa.
func (e *Executor) Execute(
	ctx context.Context,
	ownerID, key, endpoint, method, path string,
	request any,
	fn func(context.Context, pgx.Tx) (Result, error),
) (Result, error) {
	if err := ValidateKey(key); err != nil {
		return Result{}, err
	}
	hash, err := HashRequest(method, path, request)
	if err != nil {
		return Result{}, err
	}

	var out Result
	err = retry.Do(ctx, retry.Default(), func(ctx context.Context) error {
		return pgx.BeginTxFunc(ctx, e.pool, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, func(tx pgx.Tx) error {
			now := time.Now().UTC()
			rec, inserted, err := e.store.AcquireIdempotency(ctx, tx, Record{
				OwnerID:     ownerID,
				Key:         key,
				RequestHash: hash,
				Method:      method,
				Path:        path,
				CreatedAt:   now,
				ExpiresAt:   now.Add(e.ttl),
			})
			if err != nil {
				return err
			}
			if !inserted {
				if !bytes.Equal(rec.RequestHash, hash) {
					return &domain.Error{
						Code:    domain.CodeIdempotencyConflict,
						Message: "the same Idempotency-Key was used with a different request payload",
						Err:     domain.ErrIdempotencyConflict,
					}
				}
				if !rec.Completed() {
					return domain.Conflict("request with this Idempotency-Key is still being processed")
				}
				status := *rec.StatusCode
				out = Result{
					StatusCode:   status,
					Body:         rec.ResponseBody,
					ResourceType: rec.ResourceType,
					ResourceID:   rec.ResourceID,
					Replayed:     true,
				}
				return nil
			}

			out, err = fn(ctx, tx)
			if err != nil {
				return err
			}
			return e.store.CompleteIdempotency(ctx, tx, ownerID, key, out.StatusCode, out.Body, out.ResourceType, out.ResourceID)
		})
	}, e.onRetry)
	if err != nil {
		return Result{}, err
	}
	if out.Replayed && e.onReplay != nil {
		e.onReplay(endpoint)
	}
	return out, nil
}

func ValidateKey(key string) error {
	if key == "" {
		return &domain.Error{Code: domain.CodeValidation, Message: "Idempotency-Key header is required", Field: "Idempotency-Key", Err: domain.ErrValidation}
	}
	if len(key) > 255 {
		return &domain.Error{Code: domain.CodeValidation, Message: "Idempotency-Key must be at most 255 bytes", Field: "Idempotency-Key", Err: domain.ErrValidation}
	}
	return nil
}

func HashRequest(method, path string, request any) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("canonicalize request: %w", err)
	}
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return h.Sum(nil), nil
}
