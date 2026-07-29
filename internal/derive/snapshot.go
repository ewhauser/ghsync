package derive

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
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
	rows, err := tx.Query(ctx, `
		WITH requested AS (
		    SELECT scope_key, kind, installation_id, repo_id, number
		    FROM jsonb_to_recordset($1::jsonb) AS request(
		        scope_key text,
		        kind text,
		        installation_id bigint,
		        repo_id bigint,
		        number int
		    )
		),
		selected_prs AS (
		    SELECT requested.scope_key, pull_requests.*
		    FROM requested
		    JOIN repos
		      ON repos.installation_id = requested.installation_id
		     AND repos.gh_id = requested.repo_id
		     AND repos.tombstoned_at IS NULL
		    JOIN pull_requests ON pull_requests.repo_id = repos.id
		    WHERE pull_requests.tombstoned_at IS NULL
		      AND (
		          (
		              requested.kind = 'stack'
		              AND pull_requests.stack_number = requested.number
		          ) OR (
		              requested.kind = 'pr'
		              AND pull_requests.number = requested.number
		              AND pull_requests.stack_number IS NULL
		          )
		    )
		)
		SELECT requested.scope_key,
		       COALESCE(repos.org_id, 0),
		       requested.repo_id,
		       jsonb_build_object(
		           'version', 1,
		           'scope', jsonb_build_object(
		               'kind', requested.kind,
		               'number', requested.number
		           ),
		           'repository', to_jsonb(repos),
		           'repo_rules', COALESCE((
		               SELECT jsonb_agg(to_jsonb(repo_rules)
		                                ORDER BY repo_rules.rule_key)
		               FROM repo_rules
		               WHERE repo_rules.repo_id = repos.id
		                 AND repo_rules.tombstoned_at IS NULL
		           ), '[]'::jsonb),
		           'stack', (
		               SELECT to_jsonb(stacks)
		               FROM stacks
		               WHERE requested.kind = 'stack'
		                 AND stacks.repo_id = repos.id
		                 AND stacks.number = requested.number
		                 AND stacks.tombstoned_at IS NULL
		           ),
		           'pull_requests', COALESCE((
		               SELECT jsonb_agg(to_jsonb(selected_prs)
		                                - 'scope_key'
		                                ORDER BY selected_prs.number)
		               FROM selected_prs
		               WHERE selected_prs.scope_key = requested.scope_key
		           ), '[]'::jsonb),
		           'review_threads', COALESCE((
		               SELECT jsonb_agg(to_jsonb(review_threads)
		                                ORDER BY review_threads.id)
		               FROM review_threads
		               WHERE review_threads.repo_id = repos.id
		                 AND review_threads.tombstoned_at IS NULL
		                 AND EXISTS (
		                     SELECT 1
		                     FROM selected_prs
		                     WHERE selected_prs.scope_key =
		                           requested.scope_key
		                       AND selected_prs.number =
		                           review_threads.pr_number
		                 )
		           ), '[]'::jsonb),
		           'check_runs', COALESCE((
		               SELECT jsonb_agg(to_jsonb(check_runs)
		                                ORDER BY check_runs.gh_id)
		               FROM check_runs
		               WHERE check_runs.repo_id = repos.id
		                 AND check_runs.tombstoned_at IS NULL
		                 AND EXISTS (
		                     SELECT 1
		                     FROM selected_prs
		                     WHERE selected_prs.scope_key =
		                           requested.scope_key
		                       AND selected_prs.head_sha =
		                           check_runs.head_sha
		                 )
		           ), '[]'::jsonb)
		       ) AS data
		FROM requested
		LEFT JOIN repos
		  ON repos.installation_id = requested.installation_id
		 AND repos.gh_id = requested.repo_id
		 AND repos.tombstoned_at IS NULL
		ORDER BY requested.scope_key
	`, encoded)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load derivation snapshot: %w", err)
	}
	scopes, err := pgx.CollectRows(
		rows,
		func(row pgx.CollectableRow) (ScopeSnapshot, error) {
			var scope ScopeSnapshot
			err := row.Scan(
				&scope.ScopeKey,
				&scope.OrgID,
				&scope.RepoID,
				&scope.Data,
			)
			return scope, err
		},
	)
	if err != nil {
		return Snapshot{}, fmt.Errorf("scan derivation snapshot: %w", err)
	}
	return Snapshot{Scopes: scopes}, nil
}
