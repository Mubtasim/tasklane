package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID is an arbitrary constant. Every server takes this advisory
// lock while migrating, so servers starting at the same time apply each
// migration exactly once.
const migrationLockID = 7_316_244_001

// Migrate applies any migrations not yet recorded in tasklane_migrations, in
// filename order, inside one transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasklane_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, e := range entries { // ReadDir sorts by name: 0001_ runs before 0002_
		version := e.Name()
		var applied bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM tasklane_migrations WHERE version = $1)", version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + version)
		if err != nil {
			return err
		}
		// No arguments, so pgx uses the simple protocol, which allows multiple statements.
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO tasklane_migrations (version) VALUES ($1)", version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
