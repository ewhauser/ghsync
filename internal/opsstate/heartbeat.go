// Package opsstate persists trust-critical operation completion heartbeats.
package opsstate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// RecordSuccess increments a durable success count and stamps it from the
// PostgreSQL clock. Callers that need the heartbeat atomic with their work pass
// it the transaction that commits that pass.
func RecordSuccess(
	ctx context.Context,
	db execer,
	installationID int64,
	component string,
	operation string,
) error {
	return RecordSuccessN(
		ctx,
		db,
		installationID,
		component,
		operation,
		0,
	)
}

// RecordSuccessN records a completed pass and its bounded sample count.
func RecordSuccessN(
	ctx context.Context,
	db execer,
	installationID int64,
	component string,
	operation string,
	samples int64,
) error {
	if installationID <= 0 || component == "" || operation == "" {
		return fmt.Errorf("invalid operation heartbeat identity")
	}
	if samples < 0 {
		return fmt.Errorf("operation heartbeat sample count cannot be negative")
	}
	_, err := db.Exec(ctx, `
		INSERT INTO operation_heartbeats (
		    installation_id, component, operation,
		    success_count, sample_count, last_success_at, last_sample_at
		)
		VALUES (
		    $1, $2, $3, 1, $4::bigint, clock_timestamp(),
		    CASE
		        WHEN $4::bigint > 0 THEN clock_timestamp()
		        ELSE NULL
		    END
		)
		ON CONFLICT (installation_id, component, operation) DO UPDATE
		SET success_count = operation_heartbeats.success_count + 1,
		    sample_count = operation_heartbeats.sample_count
		        + EXCLUDED.sample_count,
		    last_success_at = EXCLUDED.last_success_at,
		    last_sample_at = COALESCE(
		        EXCLUDED.last_sample_at,
		        operation_heartbeats.last_sample_at
		    )
	`, installationID, component, operation, samples)
	if err != nil {
		return fmt.Errorf(
			"record %s/%s operation heartbeat: %w",
			component,
			operation,
			err,
		)
	}
	return nil
}
