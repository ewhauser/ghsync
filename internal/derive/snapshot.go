package derive

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
)

// SnapshotLoader loads all cache state for a claimed C-D2 scope set with one
// set-oriented query inside the derivation transaction.
type SnapshotLoader struct{}

// Load returns repository, rule, stack/PR, review-thread, and check-run rows
// for every requested scope from the transaction's single snapshot.
func (SnapshotLoader) Load(
	ctx context.Context,
	tx pgx.Tx,
	scopeKeys []string,
) (Snapshot, error) {
	requested := make([]parsedScope, 0, len(scopeKeys))
	for _, key := range scopeKeys {
		scope, err := parseScope(key)
		if err != nil {
			return Snapshot{}, err
		}
		requested = append(requested, scope)
	}
	encoded, err := json.Marshal(requested)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode derivation scopes: %w", err)
	}
	rows, err := dbgen.New(tx).LoadDerivationSnapshot(ctx, encoded)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load derivation snapshot: %w", err)
	}
	scopes := make([]ScopeSnapshot, 0, len(rows))
	for _, row := range rows {
		scopes = append(scopes, ScopeSnapshot{
			ScopeKey: row.ScopeKey,
			OrgID:    row.OrgID,
			RepoID:   row.RepoID,
			Data:     row.Data,
		})
	}
	return Snapshot{Scopes: scopes}, nil
}
