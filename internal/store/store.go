// Package store owns Postgres connectivity and schema migration.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	// C-I1 requires the commit acknowledged to ingress to have reached durable
	// WAL. Override role/database defaults as a startup parameter for every
	// pooled connection.
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	var synchronousCommit string
	if err := pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(
		&synchronousCommit,
	); err != nil {
		pool.Close()
		return nil, fmt.Errorf("verify synchronous_commit: %w", err)
	}
	if synchronousCommit != "on" {
		pool.Close()
		return nil, fmt.Errorf(
			"verify synchronous_commit: got %q, require %q",
			synchronousCommit,
			"on",
		)
	}
	return pool, nil
}
