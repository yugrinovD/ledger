package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type APIKey struct {
	ID        uuid.UUID
	OwnerID   string
	Name      string
	Scopes    []string
	CreatedAt time.Time
}

func (s *Store) InsertAPIKey(ctx context.Context, id uuid.UUID, ownerID, name, prefix string, hash []byte, scopes []string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO api_keys (id, owner_id, name, key_prefix, key_hash, scopes)
		VALUES ($1, $2, $3, $4, $5, $6)`, id, ownerID, name, prefix, hash, scopes)
	return err
}

func (s *Store) FindAPIKey(ctx context.Context, hash []byte) (APIKey, error) {
	var k APIKey
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_id, name, scopes, created_at
		FROM api_keys
		WHERE key_hash = $1 AND revoked_at IS NULL`, hash).Scan(&k.ID, &k.OwnerID, &k.Name, &k.Scopes, &k.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, pgx.ErrNoRows
	}
	return k, err
}
