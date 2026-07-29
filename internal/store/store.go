// Package store owns Postgres connectivity and schema migration.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

const defaultIdleInTransactionSessionTimeout = 30 * time.Second

type connectOptions struct {
	tracerProvider trace.TracerProvider
}

// ConnectOption customizes Postgres connectivity.
type ConnectOption func(*connectOptions)

// WithTracerProvider instruments pgx operations with the supplied provider.
func WithTracerProvider(provider trace.TracerProvider) ConnectOption {
	return func(options *connectOptions) {
		options.tracerProvider = provider
	}
}

// Connect opens and verifies a Postgres pool with the cache durability
// invariants enabled.
func Connect(
	ctx context.Context,
	databaseURL string,
	options ...ConnectOption,
) (*pgxpool.Pool, error) {
	var configured connectOptions
	for _, option := range options {
		option(&configured)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if configured.tracerProvider != nil {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer(
			otelpgx.WithTracerProvider(configured.tracerProvider),
			otelpgx.WithTrimSQLInSpanName(),
		)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	// C-I1 requires the commit acknowledged to ingress to have reached durable
	// WAL. Override role/database defaults as a startup parameter for every
	// pooled connection.
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	// Bound abandoned writer transactions at the pool boundary. DATABASE_URL
	// may override the default with PostgreSQL's
	// idle_in_transaction_session_timeout runtime parameter.
	if _, configured := cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"]; !configured {
		cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = defaultIdleInTransactionSessionTimeout.String()
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	synchronousCommit, err := dbgen.New(pool).ShowSynchronousCommit(ctx)
	if err != nil {
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
