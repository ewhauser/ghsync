// Package testdb provides fully isolated, migrated Postgres databases for
// integration tests. Each test gets its own database cloned from a migrated
// template (via pgtestdb), so creation is cheap and tests can run in
// parallel.
package testdb

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
	"github.com/ewhauser/ghsync/internal/store"
)

// Database owns one migrated per-test database and a pool connected to it.
type Database struct {
	Pool *pgxpool.Pool
	URL  string
}

// New returns a migrated database private to the calling test, cloned from a
// template that is migrated once per package run. Skips the test when
// TEST_DATABASE_URL is unset. Cleanup is automatic; a failed test's database
// is kept for inspection (pgtestdb logs its connection string).
func New(t testing.TB) *Database {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	conf, err := parseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	testConf := pgtestdb.Custom(t, conf, migrator{})
	pool, err := store.Connect(context.Background(), testConf.URL())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return &Database{Pool: pool, URL: testConf.URL()}
}

func parseConfig(databaseURL string) (pgtestdb.Config, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return pgtestdb.Config{}, fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}
	port := parsed.Port()
	if port == "" {
		port = "5432"
	}
	password, _ := parsed.User.Password()
	return pgtestdb.Config{
		DriverName: "pgx",
		Host:       parsed.Hostname(),
		Port:       port,
		User:       parsed.User.Username(),
		Password:   password,
		Database:   strings.TrimPrefix(parsed.Path, "/"),
		Options:    parsed.RawQuery,
		// Tests may hand Database.URL to helpers that own their own
		// connections; terminate stragglers instead of failing the drop.
		ForceTerminateConnections: true,
	}, nil
}

// migrator adapts store.Migrate to pgtestdb's template-provisioning hook.
type migrator struct{}

// Hash fingerprints everything store.Migrate applies: the embedded SQL
// migrations plus the River version providing River's own migrations.
func (migrator) Hash() (string, error) {
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
	_, _ = fmt.Fprintf(digest, "river:%s\n", riverVersion())
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func (migrator) Migrate(
	ctx context.Context,
	_ *sql.DB,
	conf pgtestdb.Config, //nolint:gocritic // pgtestdb.Migrator requires Config by value
) error {
	pool, err := store.Connect(ctx, conf.URL())
	if err != nil {
		return err
	}
	defer pool.Close()
	return store.Migrate(ctx, pool)
}

func riverVersion() string {
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
