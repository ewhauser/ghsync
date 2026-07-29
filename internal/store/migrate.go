package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/ewhauser/ghsync/db"
)

// Migrate applies River's migrations followed by our own plain-SQL files, in
// lexical order, tracked in schema_migrations. Each of our migrations runs in
// its own transaction and is applied at most once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Serialize the whole migration pipeline across processes. The integration
	// tests exercise Migrate from separate packages against the same database,
	// and production replicas may also start together.
	lockConn, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if err != nil {
		return fmt.Errorf("migration lock connection: %w", err)
	}
	defer lockConn.Close(context.Background()) //nolint:errcheck // close releases the session lock
	if _, err := lockConn.Exec(ctx,
		`SELECT pg_advisory_lock(hashtextextended('ghsync_schema_migrations', 0))`); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("river migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{}); err != nil {
		return fmt.Errorf("river migrations: %w", err)
	}

	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		checksum BYTEA NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}
	// Older M0 databases may have the pre-checksum ledger shape. Leave any
	// existing rows NULL so applyOne fails closed instead of silently blessing
	// migration contents it cannot verify.
	if _, err := pool.Exec(ctx,
		`ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum BYTEA`); err != nil {
		return fmt.Errorf("ensure schema_migrations checksum: %w", err)
	}

	names, err := migrationNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := applyOne(ctx, pool, name); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	if _, err := pool.Exec(ctx,
		`ALTER TABLE schema_migrations ALTER COLUMN checksum SET NOT NULL`); err != nil {
		return fmt.Errorf("enforce schema_migrations checksum: %w", err)
	}
	return nil
}

func migrationNames() ([]string, error) {
	entries, err := fs.ReadDir(db.Migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func applyOne(ctx context.Context, pool *pgxpool.Pool, name string) error {
	sql, err := fs.ReadFile(db.Migrations, "migrations/"+name)
	if err != nil {
		return err
	}
	checksum := sha256.Sum256(sql)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	// The insert doubles as a lock: a concurrent migrator blocks here and
	// then sees the row exists.
	tag, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (name, checksum)
		 VALUES ($1, $2)
		 ON CONFLICT (name) DO NOTHING`,
		name, checksum[:])
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var appliedChecksum []byte
		if err := tx.QueryRow(ctx,
			`SELECT checksum FROM schema_migrations WHERE name = $1`, name).
			Scan(&appliedChecksum); err != nil {
			return err
		}
		if err := verifyChecksum(name, appliedChecksum, checksum[:]); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func verifyChecksum(name string, applied, current []byte) error {
	if len(applied) == 0 {
		return fmt.Errorf("applied migration has no checksum; cannot verify %q", name)
	}
	if !bytes.Equal(applied, current) {
		return fmt.Errorf(
			"checksum mismatch: applied %x, embedded %x",
			applied,
			current,
		)
	}
	return nil
}
