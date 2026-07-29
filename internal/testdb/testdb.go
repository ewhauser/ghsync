// Package testdb provides isolated-schema Postgres databases for integration
// test packages that otherwise run concurrently under "go test ./...".
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/store"
)

// Database owns one migrated schema and its search-path-scoped pool.
type Database struct {
	Pool   *pgxpool.Pool
	URL    string
	admin  *pgxpool.Pool
	schema string
}

// Open creates and migrates a schema private to one test.
func Open(
	ctx context.Context,
	databaseURL string,
	prefix string,
) (*Database, error) {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return nil, fmt.Errorf("test schema suffix: %w", err)
	}
	prefix = strings.ToLower(prefix)
	schema := "frontier_" + prefix + "_" + hex.EncodeToString(suffix)
	admin, err := store.Connect(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if _, err := admin.Exec(
		ctx,
		"CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize(),
	); err != nil {
		admin.Close()
		return nil, fmt.Errorf("create test schema: %w", err)
	}

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		admin.Close()
		return nil, fmt.Errorf("parse test database URL: %w", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	pool, err := store.Connect(ctx, parsed.String())
	if err != nil {
		dropSchema(admin, schema)
		admin.Close()
		return nil, err
	}
	if err := store.Migrate(ctx, pool); err != nil {
		pool.Close()
		dropSchema(admin, schema)
		admin.Close()
		return nil, err
	}
	return &Database{
		Pool:   pool,
		URL:    parsed.String(),
		admin:  admin,
		schema: schema,
	}, nil
}

// Close drops the isolated schema and all test data.
func (d *Database) Close() {
	if d == nil {
		return
	}
	d.Pool.Close()
	dropSchema(d.admin, d.schema)
	d.admin.Close()
}

func dropSchema(admin *pgxpool.Pool, schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = admin.Exec(
		ctx,
		"DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE",
	)
}
