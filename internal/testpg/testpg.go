// Package testpg starts a throwaway PostgreSQL for integration tests. Each test
// package calls Start once from TestMain and shares the database, so tests that
// use it must not call t.Parallel().
package testpg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mubtasim/tasklane/internal/pgstore"
)

// DB is a migrated Postgres 17 running in a container.
type DB struct {
	Pool *pgxpool.Pool
	DSN  string
	ctr  *tcpostgres.PostgresContainer
}

// Start runs the container, connects and applies the migrations.
func Start(ctx context.Context) (*DB, error) {
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("tasklane"),
		tcpostgres.WithUsername("tasklane"),
		tcpostgres.WithPassword("tasklane"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres: %w", err)
	}
	db := &DB{ctr: ctr}
	db.DSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("connection string: %w", err)
	}
	db.Pool, err = pgxpool.New(ctx, db.DSN)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pgstore.Migrate(ctx, db.Pool); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// Reset empties all Tasklane tables and restarts job IDs at 1.
func (db *DB) Reset(ctx context.Context) error {
	_, err := db.Pool.Exec(ctx, "TRUNCATE jobs, queues RESTART IDENTITY")
	return err
}

// Close disconnects and removes the container.
func (db *DB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
	_ = testcontainers.TerminateContainer(db.ctr)
}
