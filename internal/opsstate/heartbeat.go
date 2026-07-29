// Package opsstate persists trust-critical operation completion heartbeats.
package opsstate

import (
	"context"
	"fmt"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// RecordSuccess increments a durable success count and stamps it from the
// PostgreSQL clock. Callers that need the heartbeat atomic with their work pass
// it the transaction that commits that pass.
func RecordSuccess(
	ctx context.Context,
	db dbgen.DBTX,
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
	db dbgen.DBTX,
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
	err := dbgen.New(db).RecordOperationHeartbeat(
		ctx,
		dbgen.RecordOperationHeartbeatParams{
			InstallationID: installationID,
			Component:      component,
			Operation:      operation,
			Samples:        samples,
		},
	)
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
