package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/yugrinovD/ledger/migrations"
)

type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
}

func Open(ctx context.Context, dsn string, cfg PoolConfig) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		pc.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		pc.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	pc.ConnConfig.RuntimeParams["application_name"] = "ledger"
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "15000"

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

type MigrationDirection string

const (
	MigrateUp     MigrationDirection = "up"
	MigrateDown   MigrationDirection = "down"
	MigrateStatus MigrationDirection = "status"
)

func Migrate(ctx context.Context, dsn string, direction MigrationDirection) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping migration connection: %w", err)
	}
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	goose.SetLogger(goose.NopLogger())
	switch direction {
	case MigrateUp:
		err = goose.UpContext(ctx, db, ".")
	case MigrateDown:
		err = goose.DownContext(ctx, db, ".")
	case MigrateStatus:
		err = goose.StatusContext(ctx, db, ".")
	default:
		return fmt.Errorf("unknown migration direction %q", direction)
	}
	if err != nil {
		return fmt.Errorf("migrate %s: %w", direction, err)
	}
	return nil
}
