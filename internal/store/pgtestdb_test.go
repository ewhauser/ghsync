package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" database/sql driver for pgtestdb
	"github.com/peterldowns/pgtestdb"

	"github.com/ewhauser/ghsync/db"
)

// This file mirrors internal/testdb, which package store's own tests cannot
// import because testdb depends on store.

// testDatabaseConfig returns connection details for a migrated database
// private to the calling test, cloned from a template that is migrated once
// per package run. Skips the test when TEST_DATABASE_URL is unset.
func testDatabaseConfig(t *testing.T) *pgtestdb.Config {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	port := parsed.Port()
	if port == "" {
		port = "5432"
	}
	password, _ := parsed.User.Password()
	return pgtestdb.Custom(t, pgtestdb.Config{
		DriverName:                "pgx",
		Host:                      parsed.Hostname(),
		Port:                      port,
		User:                      parsed.User.Username(),
		Password:                  password,
		Database:                  strings.TrimPrefix(parsed.Path, "/"),
		Options:                   parsed.RawQuery,
		ForceTerminateConnections: true,
	}, storeMigrator{})
}

// testDatabasePool is testDatabaseConfig plus a Connect-configured pool.
func testDatabasePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := Connect(context.Background(), testDatabaseConfig(t).URL())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// storeMigrator adapts Migrate to pgtestdb's template-provisioning hook.
type storeMigrator struct{}

// Hash fingerprints everything Migrate applies: the embedded SQL migrations
// plus the River version providing River's own migrations.
func (storeMigrator) Hash() (string, error) {
	digest := sha256.New()
	entries, err := fs.ReadDir(db.Migrations, "migrations")
	if err != nil {
		return "", fmt.Errorf("read migrations: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		contents, err := fs.ReadFile(db.Migrations, "migrations/"+entry.Name())
		if err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(digest, "%s\n", entry.Name())
		digest.Write(contents)
	}
	_, _ = fmt.Fprintf(digest, "river:%s\n", riverTestVersion())
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func (storeMigrator) Migrate(
	ctx context.Context,
	_ *sql.DB,
	conf pgtestdb.Config, //nolint:gocritic // pgtestdb.Migrator requires Config by value
) error {
	pool, err := Connect(ctx, conf.URL())
	if err != nil {
		return err
	}
	defer pool.Close()
	return Migrate(ctx, pool)
}

func riverTestVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/riverqueue/river/riverdriver/riverpgxv5" {
			return dep.Version
		}
	}
	return "unknown"
}
