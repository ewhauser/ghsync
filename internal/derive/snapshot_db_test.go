package derive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ewhauser/ghsync/internal/testdb"
)

func TestLoadDerivationSnapshotMatchesCorrelatedQuery(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	pool := database.Pool
	ctx := context.Background()
	repositoryID, localRepoID, scopes := seedSnapshotCorpus(t, pool, 8)

	batch := &pgx.Batch{}
	batch.Queue(`
		UPDATE pull_requests
		SET stack_number = CASE WHEN number IN (1, 2) THEN 7 ELSE 8 END,
		    stack_position = number
		WHERE repo_id = $1 AND number IN (1, 2, 4)
	`, localRepoID)
	batch.Queue(`
		INSERT INTO stacks (
		    repo_id, gh_id, node_id, number, base_ref, base_sha, open,
		    entries, head_sha, synced_at, last_checked_at, etag, sync_source,
		    tombstoned_at
		) VALUES
		    ($1, 700007, 'S_equivalence_live', 7, 'main', 'base-sha', true,
		     '[]'::jsonb, 'head-2', clock_timestamp(), clock_timestamp(), '',
		     'manual', NULL),
		    ($1, 700008, 'S_equivalence_dead', 8, 'main', 'base-sha', true,
		     '[]'::jsonb, 'head-4', clock_timestamp(), clock_timestamp(), '',
		     'manual', clock_timestamp())
	`, localRepoID)
	batch.Queue(`
		UPDATE pull_requests
		SET tombstoned_at = clock_timestamp()
		WHERE repo_id = $1 AND number = 5
	`, localRepoID)
	batch.Queue(`
		UPDATE repo_rules
		SET tombstoned_at = clock_timestamp()
		WHERE repo_id = $1 AND rule_key = 'rule-08'
	`, localRepoID)
	batch.Queue(`
		UPDATE review_threads
		SET tombstoned_at = clock_timestamp()
		WHERE repo_id = $1 AND id = 'thread-3-2'
	`, localRepoID)
	batch.Queue(`
		UPDATE check_runs
		SET tombstoned_at = clock_timestamp()
		WHERE repo_id = $1 AND head_sha = 'head-3' AND name = 'check-4'
	`, localRepoID)
	batch.Queue(`
		INSERT INTO repos (
		    installation_id, org_id, gh_id, node_id, owner, name, full_name,
		    default_branch, archived, head_sha, synced_at, last_checked_at,
		    etag, sync_source, tombstoned_at
		) VALUES (
		    1, 38, $1, 'R_snapshot_dead', 'issue-38', 'snapshot-dead',
		    'issue-38/snapshot-dead', 'main', false, 'base-sha',
		    clock_timestamp(), clock_timestamp(), '', 'manual', clock_timestamp()
		)
	`, repositoryID+1)
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}

	requested := []string{
		scopes[7],
		fmt.Sprintf("stack:1:%d:7", repositoryID),
		scopes[0], // stack-owned PR: its loose scope must be empty.
		scopes[2],
		scopes[2], // duplicate input must preserve legacy row multiplicity.
		fmt.Sprintf("pr:1:%d:99", repositoryID),
		fmt.Sprintf("stack:1:%d:8", repositoryID), // tombstoned stack.
		scopes[4],                                 // tombstoned PR.
		fmt.Sprintf("pr:1:%d:1", repositoryID+1),  // tombstoned repository.
		fmt.Sprintf("pr:1:%d:99", repositoryID+2), // unknown repository.
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // test cleanup
	assertSnapshotMatchesLegacy(t, ctx, tx, requested)
	assertSnapshotMatchesLegacy(t, ctx, tx, []string{})
}

func assertSnapshotMatchesLegacy(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	requested []string,
) {
	t.Helper()
	snapshot, err := (SnapshotLoader{}).Load(ctx, tx, requested)
	if err != nil {
		t.Fatal(err)
	}
	actual := make([]snapshotEquivalenceRow, 0, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		actual = append(actual, snapshotEquivalenceRow(scope))
	}
	encodedActual, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("encode optimized snapshot: %v", err)
	}
	encodedScopes := encodeSnapshotScopes(t, requested)

	var mismatches int
	if err := tx.QueryRow(ctx, `
		WITH expected AS (
		`+legacyLoadDerivationSnapshot+`
		),
		actual AS (
		    SELECT scope_key, org_id, repo_id, data
		    FROM jsonb_to_recordset($2::jsonb) AS row(
		        scope_key text,
		        org_id bigint,
		        repo_id bigint,
		        data jsonb
		    )
		)
		SELECT count(*)
		FROM (
		    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
		    UNION ALL
		    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
		) AS diff
	`, encodedScopes, encodedActual).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf(
			"set-based snapshot diverges from correlated query in %d rows",
			mismatches,
		)
	}

	legacyRows, err := tx.Query(ctx, legacyLoadDerivationSnapshot, encodedScopes)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyRows.Close()
	expected := make([]snapshotEquivalenceRow, 0, len(requested))
	for legacyRows.Next() {
		var row snapshotEquivalenceRow
		var data []byte
		if err := legacyRows.Scan(
			&row.ScopeKey,
			&row.OrgID,
			&row.RepoID,
			&data,
		); err != nil {
			t.Fatal(err)
		}
		row.Data = data
		expected = append(expected, row)
	}
	if err := legacyRows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("set-based snapshot returned %d rows, legacy returned %d", len(actual), len(expected))
	}

	orderedScopes := make([]string, len(requested))
	copy(orderedScopes, requested)
	sort.Strings(orderedScopes)
	for index := range expected {
		if actual[index].ScopeKey != orderedScopes[index] {
			t.Fatalf(
				"set-based snapshot row %d scope = %q, want lexical order %q",
				index,
				actual[index].ScopeKey,
				orderedScopes[index],
			)
		}
		if actual[index].ScopeKey != expected[index].ScopeKey ||
			actual[index].OrgID != expected[index].OrgID ||
			actual[index].RepoID != expected[index].RepoID {
			t.Fatalf(
				"set-based snapshot row %d identity = %+v, legacy = %+v",
				index,
				actual[index],
				expected[index],
			)
		}
		if !bytes.Equal(actual[index].Data, expected[index].Data) {
			t.Fatalf(
				"set-based snapshot data text changed for row %d scope %q",
				index,
				actual[index].ScopeKey,
			)
		}
	}
}

func TestLoadDerivationSnapshotPlanScalesSetWise(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	pool := database.Pool
	ctx := context.Background()
	_, _, scopes := seedSnapshotCorpus(t, pool, 500)
	if _, err := pool.Exec(ctx, `
		ANALYZE repos, repo_rules, pull_requests, review_threads, check_runs
	`); err != nil {
		t.Fatal(err)
	}

	query := captureSnapshotQuery(t, database, scopes[:1])
	var largePlan explainResult
	for _, scopeCount := range []int{1, 100, 500} {
		plan := explainSnapshotQuery(
			t,
			pool,
			query,
			encodeSnapshotScopes(t, scopes[:scopeCount]),
		)
		if got := int(plan.Plan.ActualRows); got != scopeCount {
			t.Fatalf(
				"%d-scope plan returned %d rows",
				scopeCount,
				got,
			)
		}
		assertSetWiseSnapshotPlan(t, scopeCount, &plan.Plan)
		t.Logf(
			"scopes=%d execution_ms=%.3f shared_blocks=%d",
			scopeCount,
			plan.ExecutionTime,
			plan.Plan.sharedBlocks(),
		)
		if scopeCount == 500 {
			largePlan = plan
		}
	}

	legacyPlan := explainSnapshotQuery(
		t,
		pool,
		legacyLoadDerivationSnapshot,
		encodeSnapshotScopes(t, scopes),
	)
	optimizedBlocks := largePlan.Plan.sharedBlocks()
	legacyBlocks := legacyPlan.Plan.sharedBlocks()
	if optimizedBlocks*2 >= legacyBlocks {
		t.Fatalf(
			"500-scope shared-buffer work optimized=%d legacy=%d; want at least 2x reduction",
			optimizedBlocks,
			legacyBlocks,
		)
	}
	t.Logf(
		"legacy scopes=500 execution_ms=%.3f shared_blocks=%d",
		legacyPlan.ExecutionTime,
		legacyBlocks,
	)
}

type snapshotEquivalenceRow struct {
	ScopeKey string          `json:"scope_key"`
	OrgID    int64           `json:"org_id"`
	RepoID   int64           `json:"repo_id"`
	Data     json.RawMessage `json:"data"`
}

type snapshotQueryTracer struct {
	query string
}

func (tracer *snapshotQueryTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if strings.Contains(data.SQL, "name: LoadDerivationSnapshot") {
		tracer.query = data.SQL
	}
	return ctx
}

func (*snapshotQueryTracer) TraceQueryEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceQueryEndData,
) {
}

func captureSnapshotQuery(
	t *testing.T,
	database *testdb.Database,
	scopes []string,
) string {
	t.Helper()
	ctx := context.Background()
	tracer := &snapshotQueryTracer{}
	config := database.Pool.Config().ConnConfig.Copy()
	config.Tracer = tracer
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect snapshot query tracer: %v", err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck // test cleanup
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin snapshot query trace: %v", err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // test cleanup
	if _, err := (SnapshotLoader{}).Load(ctx, tx, scopes); err != nil {
		t.Fatal(err)
	}
	if tracer.query == "" {
		t.Fatal("LoadDerivationSnapshot SQL was not traced")
	}
	return tracer.query
}

type explainResult struct {
	Plan          explainNode `json:"Plan"`
	ExecutionTime float64     `json:"Execution Time"`
}

type explainNode struct {
	NodeType           string        `json:"Node Type"`
	ParentRelationship string        `json:"Parent Relationship"`
	SubplanName        string        `json:"Subplan Name"`
	ActualLoops        float64       `json:"Actual Loops"`
	ActualRows         float64       `json:"Actual Rows"`
	SharedHitBlocks    int64         `json:"Shared Hit Blocks"`
	SharedReadBlocks   int64         `json:"Shared Read Blocks"`
	Plans              []explainNode `json:"Plans"`
}

func (node *explainNode) sharedBlocks() int64 {
	return node.SharedHitBlocks + node.SharedReadBlocks
}

func explainSnapshotQuery(
	t *testing.T,
	pool *pgxpool.Pool,
	query string,
	encodedScopes []byte,
) explainResult {
	t.Helper()
	var raw json.RawMessage
	if err := pool.QueryRow(
		context.Background(),
		"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query,
		encodedScopes,
	).Scan(&raw); err != nil {
		t.Fatalf("explain derivation snapshot: %v", err)
	}
	var plans []explainResult
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatalf("decode derivation snapshot plan: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("EXPLAIN returned %d plans, want 1", len(plans))
	}
	return plans[0]
}

func assertSetWiseSnapshotPlan(
	t *testing.T,
	scopeCount int,
	plan *explainNode,
) {
	t.Helper()
	var visit func(*explainNode)
	visit = func(node *explainNode) {
		if node.ParentRelationship == "SubPlan" {
			t.Errorf(
				"%d-scope plan contains correlated child subplan %q",
				scopeCount,
				node.SubplanName,
			)
		}
		if node.NodeType == "Aggregate" && node.ActualLoops > 1 {
			t.Errorf(
				"%d-scope aggregate executed %.0f loops",
				scopeCount,
				node.ActualLoops,
			)
		}
		for index := range node.Plans {
			visit(&node.Plans[index])
		}
	}
	visit(plan)
}

func encodeSnapshotScopes(t *testing.T, scopeKeys []string) []byte {
	t.Helper()
	requested := make([]parsedScope, 0, len(scopeKeys))
	for _, key := range scopeKeys {
		scope, err := parseScope(key)
		if err != nil {
			t.Fatal(err)
		}
		requested = append(requested, scope)
	}
	encoded, err := json.Marshal(requested)
	if err != nil {
		t.Fatalf("encode derivation scopes: %v", err)
	}
	return encoded
}

func seedSnapshotCorpus(
	t *testing.T,
	pool *pgxpool.Pool,
	scopeCount int,
) (int64, int64, []string) {
	t.Helper()
	ctx := context.Background()
	repositoryID := int64(38_000_000 + scopeCount)
	var localRepoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO repos (
		    installation_id, org_id, gh_id, node_id, owner, name, full_name,
		    default_branch, archived, head_sha, synced_at, last_checked_at,
		    etag, sync_source
		) VALUES (
		    1, 38, $1, 'R_snapshot', 'issue-38', 'snapshot',
		    'issue-38/snapshot', 'main', false, 'base-sha', clock_timestamp(),
		    clock_timestamp(), '', 'manual'
		)
		RETURNING id
	`, repositoryID).Scan(&localRepoID); err != nil {
		t.Fatal(err)
	}
	batch := &pgx.Batch{}
	batch.Queue(`
		INSERT INTO repo_rules (
		    repo_id, rule_key, rule, head_sha, synced_at, last_checked_at,
		    etag, sync_source
		)
		SELECT $1,
		       format('rule-%s', lpad(rule_number::text, 2, '0')),
		       jsonb_build_object('position', rule_number),
		       'base-sha', clock_timestamp(), clock_timestamp(), '', 'manual'
		FROM generate_series(1, 8) AS rule_number
	`, localRepoID)
	batch.Queue(`
		INSERT INTO pull_requests (
		    repo_id, gh_id, node_id, number, title, state, draft,
		    author_login, head_ref, head_sha, base_ref, base_sha,
		    review_decision, mergeable_state, synced_at, last_checked_at,
		    etag, sync_source
		)
		SELECT $1,
		       380000000 + pr_number,
		       format('PR_snapshot_%s', pr_number),
		       pr_number,
		       format('pull request %s', pr_number),
		       'open', false, 'tester', format('feature-%s', pr_number),
		       format('head-%s', pr_number), 'main', 'base-sha', '', '',
		       clock_timestamp(), clock_timestamp(), '', 'manual'
		FROM generate_series(1, $2::int) AS pr_number
	`, localRepoID, scopeCount)
	batch.Queue(`
		INSERT INTO review_threads (
		    id, repo_id, pr_number, path, line, comments, head_sha, synced_at,
		    last_checked_at, etag, sync_source
		)
		SELECT format('thread-%s-%s', pr_number, thread_number),
		       $1, pr_number, format('file-%s.go', thread_number),
		       thread_number, '[]'::jsonb, format('head-%s', pr_number),
		       clock_timestamp(), clock_timestamp(), '', 'manual'
		FROM generate_series(1, $2::int) AS pr_number
		CROSS JOIN generate_series(1, 2) AS thread_number
	`, localRepoID, scopeCount)
	batch.Queue(`
		INSERT INTO check_runs (
		    gh_id, repo_id, node_id, name, status, conclusion, head_sha,
		    synced_at, last_checked_at, etag, sync_source
		)
		SELECT 3800000000 + pr_number * 10 + check_number,
		       $1,
		       format('CR_snapshot_%s_%s', pr_number, check_number),
		       format('check-%s', check_number),
		       'completed', 'success', format('head-%s', pr_number),
		       clock_timestamp(), clock_timestamp(), '', 'manual'
		FROM generate_series(1, $2::int) AS pr_number
		CROSS JOIN generate_series(1, 4) AS check_number
	`, localRepoID, scopeCount)
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}

	scopes := make([]string, 0, scopeCount)
	for number := 1; number <= scopeCount; number++ {
		scopes = append(
			scopes,
			fmt.Sprintf("pr:1:%d:%d", repositoryID, number),
		)
	}
	return repositoryID, localRepoID, scopes
}

const legacyLoadDerivationSnapshot = `
WITH requested AS (
    SELECT element->>'scope_key' AS scope_key,
           element->>'kind' AS kind,
           (element->>'installation_id')::bigint AS installation_id,
           (element->>'repo_id')::bigint AS repo_id,
           (element->>'number')::int AS number
    FROM jsonb_array_elements($1::jsonb) AS element
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
SELECT requested.scope_key::text AS scope_key,
       COALESCE(repos.org_id, 0)::bigint AS org_id,
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
               SELECT jsonb_agg(to_jsonb(selected_prs) - 'scope_key'
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
                     WHERE selected_prs.scope_key = requested.scope_key
                       AND selected_prs.number = review_threads.pr_number
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
                     WHERE selected_prs.scope_key = requested.scope_key
                       AND selected_prs.head_sha = check_runs.head_sha
                 )
           ), '[]'::jsonb)
       ) AS data
FROM requested
LEFT JOIN repos
  ON repos.installation_id = requested.installation_id
 AND repos.gh_id = requested.repo_id
 AND repos.tombstoned_at IS NULL
ORDER BY requested.scope_key`
