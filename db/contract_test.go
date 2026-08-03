package db_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ewhauser/ghsync/internal/outbox"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/testdb"
)

type documentedColumn struct {
	Table    string
	Column   string
	Type     string
	Nullable string
}

func TestPublicSchemaManifestMatchesMigratedDatabase(t *testing.T) {
	t.Parallel()
	rows := parseManifest(
		t, "CONTRACT.md", "v1-schema", 5,
	)
	documented := make(map[string]documentedColumn, len(rows))
	tables := make(map[string]struct{})
	for _, row := range rows {
		column := documentedColumn{
			Table:    row[0],
			Column:   row[1],
			Type:     row[2],
			Nullable: row[3],
		}
		documented[column.Table+"."+column.Column] = column
		tables[column.Table] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database := testdb.New(t)
	tableNames := make([]string, 0, len(tables))
	for table := range tables {
		tableNames = append(tableNames, table)
	}
	sort.Strings(tableNames)
	queryRows, err := database.Pool.Query(ctx, `
		SELECT class.relname,
		       attribute.attname,
		       format_type(attribute.atttypid, attribute.atttypmod),
		       CASE WHEN attribute.attnotnull THEN 'no' ELSE 'yes' END
		FROM pg_catalog.pg_class AS class
		JOIN pg_catalog.pg_namespace AS namespace
		  ON namespace.oid = class.relnamespace
		JOIN pg_catalog.pg_attribute AS attribute
		  ON attribute.attrelid = class.oid
		WHERE namespace.nspname = current_schema()
		  AND class.relname = ANY($1::text[])
		  AND attribute.attnum > 0
		  AND NOT attribute.attisdropped
		ORDER BY class.relname, attribute.attnum
	`, tableNames)
	if err != nil {
		t.Fatal(err)
	}
	live, err := pgx.CollectRows(
		queryRows,
		func(row pgx.CollectableRow) (documentedColumn, error) {
			var column documentedColumn
			err := row.Scan(
				&column.Table,
				&column.Column,
				&column.Type,
				&column.Nullable,
			)
			return column, err
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	privateColumns := map[string]struct{}{
		"change_events.outbox_txid":      {},
		"stream_watermark.candidate_seq": {},
		"stream_watermark.candidate_xid": {},
		"stream_watermark.lease_token":   {},
		"stream_watermark.lease_until":   {},
	}
	livePublic := make(map[string]documentedColumn)
	for _, column := range live {
		key := column.Table + "." + column.Column
		if _, private := privateColumns[key]; private {
			continue
		}
		livePublic[key] = column
	}
	if !reflect.DeepEqual(documented, livePublic) {
		t.Fatalf(
			"db/CONTRACT.md public schema differs from migrated schema\n"+
				"documented-only: %s\nlive-only: %s\nchanged: %s",
			mapDifference(documented, livePublic),
			mapDifference(livePublic, documented),
			changedColumns(documented, livePublic),
		)
	}
}

func TestDocumentedDiffToOwnerSQLRunsAgainstMigratedDatabase(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("CONTRACT.md")
	if err != nil {
		t.Fatal(err)
	}
	const start = "<!-- diff-to-owner-sql:start -->\n```sql\n"
	const end = "\n```\n<!-- diff-to-owner-sql:end -->"
	_, query, ok := bytes.Cut(content, []byte(start))
	if !ok {
		t.Fatal("missing documented diff-to-owner SQL start marker")
	}
	query, _, ok = bytes.Cut(query, []byte(end))
	if !ok {
		t.Fatal("missing documented diff-to-owner SQL end marker")
	}
	database := testdb.New(t)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	repository := store.RepositoryRecord{
		InstallationID: 1, OrgID: 1, GitHubID: 11001,
		NodeID: "repo-contract-query", Owner: "acme", Name: "contract-query",
		FullName: "acme/contract-query", DefaultBranch: "main",
		DefaultHeadSHA: "base-contract", GitHubUpdatedAt: now,
	}
	pull := store.PullRequestRecord{
		Repository: repository, GitHubID: 11002, NodeID: "pr-contract-query",
		Number: 7, Title: "contract query", State: "open",
		HeadRef: "feature", HeadSHA: "head-contract",
		BaseRef: "main", BaseSHA: "base-contract", MembershipKnown: true,
		GitHubUpdatedAt: now, SyncedAt: now, Source: store.SyncSourceReconcile,
		ChangeInputsKnown: true,
		ChangeSnapshot: &store.PullRequestChangeSnapshotRecord{
			BaseSHA: "base-contract", HeadSHA: "head-contract",
			FilesTotalCount: 1, CodeownersRef: "main",
			CodeownersSHA: "base-contract", CodeownersPath: "CODEOWNERS",
			CodeownersState: "present", CodeownersSource: "*.go @owner",
			CodeownersHash: "contract-source-hash",
			Files: []store.ChangedFileRecord{{
				Path: "src/main.go", ChangeType: "modified",
			}},
			Owners: []store.FileOwnerRecord{{
				Path: "src/main.go", OwnerToken: "@owner", OwnerType: "user",
				OwnerName: "owner", ResolutionState: "unresolved",
				SourcePattern: "*.go", SourceLine: 1,
			}},
		},
	}
	if _, err := store.NewEntityWriter(database.Pool).ApplyPullRequest(
		t.Context(), pull,
	); err != nil {
		t.Fatal(err)
	}
	var repoID int64
	if err := database.Pool.QueryRow(
		t.Context(), "SELECT id FROM repos WHERE gh_id = $1", repository.GitHubID,
	).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	rows, err := database.Pool.Query(
		t.Context(), string(query), repoID, int32(pull.Number),
	)
	if err != nil {
		t.Fatalf("documented diff-to-owner SQL failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("documented diff-to-owner SQL returned no row: %v", rows.Err())
	}
	values, err := rows.Values()
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 19 || values[8] != "src/main.go" || values[11] != "@owner" {
		t.Fatalf("documented diff-to-owner SQL row = %#v", values)
	}
	if rows.Next() {
		t.Fatal("documented diff-to-owner SQL returned duplicate rows")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerGrantIncludesChangeInputTables(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("CONTRACT.md")
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(content, []byte("GRANT SELECT ON TABLE\n"))
	if start < 0 {
		t.Fatal("missing consumer SELECT grant")
	}
	grant := content[start:]
	if end := bytes.Index(grant, []byte("\nTO ghsync_consumer;")); end >= 0 {
		grant = grant[:end]
	} else {
		t.Fatal("unterminated consumer SELECT grant")
	}
	for _, table := range []string{
		"pull_request_change_snapshots",
		"pull_request_changed_files",
		"pull_request_file_owners",
	} {
		if !bytes.Contains(grant, []byte(table)) {
			t.Fatalf("consumer SELECT grant omits %s", table)
		}
	}
}

func TestEventManifestMatchesWriterDefinitions(t *testing.T) {
	t.Parallel()
	rows := parseManifest(t, "CONTRACT.md", "v1-events", 5)
	got := make([]outbox.Definition, 0, len(rows))
	for _, row := range rows {
		got = append(got, outbox.Definition{
			Stream:           row[0],
			Kind:             row[1],
			EntityKeyGrammar: row[2],
			LookupTarget:     row[3],
			PayloadRule:      row[4],
		})
	}
	if !reflect.DeepEqual(got, outbox.V1Definitions) {
		t.Fatalf(
			"db/CONTRACT.md event manifest differs from writer definitions\n"+
				"documented: %#v\nwriters: %#v",
			got,
			outbox.V1Definitions,
		)
	}
}

func TestParticipationContractUsesFactIdentityAndExcludesBodies(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	for _, table := range []string{
		"pull_request_reviews",
		"pull_request_comments",
	} {
		var identityConstraints string
		var bodyColumns int
		if err := database.Pool.QueryRow(t.Context(), `
			SELECT string_agg(
			           pg_get_constraintdef(con.oid),
			           '; ' ORDER BY con.conname
			       ),
			       (SELECT count(*)
			        FROM information_schema.columns
			        WHERE table_schema = current_schema()
			          AND table_name = $1
			          AND column_name IN ('body', 'body_text', 'body_html'))
			FROM pg_catalog.pg_constraint AS con
			JOIN pg_catalog.pg_class AS class
			  ON class.oid = con.conrelid
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = class.relnamespace
			WHERE namespace.nspname = current_schema()
			  AND class.relname = $1
			  AND con.contype IN ('p', 'u')
		`, table).Scan(&identityConstraints, &bodyColumns); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(identityConstraints, "PRIMARY KEY (node_id)") ||
			!strings.Contains(identityConstraints, "UNIQUE (gh_id)") ||
			strings.Contains(identityConstraints, "author_") {
			t.Fatalf(
				"%s identity constraints = %q",
				table,
				identityConstraints,
			)
		}
		if bodyColumns != 0 {
			t.Fatalf("%s exposes %d body columns", table, bodyColumns)
		}
	}
}

func TestV1EntityKeyConstructors(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"repository":      outbox.RepositoryKey(7, 11),
		"pull request":    outbox.PullRequestKey(7, 11, 13),
		"stack":           outbox.StackKey(7, 11, 17),
		"checks":          outbox.ChecksKey(7, 11, "abc123"),
		"repo rules":      outbox.RepoRulesKey(7, 11),
		"stack work item": outbox.StackWorkItemKey(11, 17),
		"PR work item":    outbox.PullRequestWorkItemKey(11, 13),
	}
	want := map[string]string{
		"repository":      "repo:7:11",
		"pull request":    "pr:7:11:13",
		"stack":           "stack:7:11:17",
		"checks":          "checks:7:11:abc123",
		"repo rules":      "repo_rules:7:11",
		"stack work item": "repo:11:stack:17",
		"PR work item":    "repo:11:pr:13",
	}
	if !reflect.DeepEqual(tests, want) {
		t.Fatalf("v1 entity keys = %#v, want %#v", tests, want)
	}
}

func parseManifest(
	t *testing.T,
	path string,
	name string,
	fields int,
) [][]string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	start := "<!-- " + name + ":start -->"
	end := "<!-- " + name + ":end -->"
	startIndex := bytes.Index(content, []byte(start))
	if startIndex < 0 {
		t.Fatalf("missing %s start marker", name)
	}
	section := string(content[startIndex+len(start):])
	section = strings.TrimPrefix(section, "\n")
	endIndex := strings.Index(section, end)
	if endIndex < 0 {
		t.Fatalf("missing %s manifest markers", name)
	}
	section = section[:endIndex]
	var rows [][]string
	for line := range strings.SplitSeq(section, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) != fields+2 {
			t.Fatalf("%s manifest row has %d fields: %s", name, len(parts)-2, line)
		}
		row := make([]string, fields)
		for index := range fields {
			row[index] = strings.Trim(
				strings.TrimSpace(parts[index+1]),
				"`",
			)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		t.Fatalf("%s manifest is empty", name)
	}
	return rows
}

func mapDifference(
	left map[string]documentedColumn,
	right map[string]documentedColumn,
) string {
	var keys []string
	for key := range left {
		if _, ok := right[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

func changedColumns(
	documented map[string]documentedColumn,
	live map[string]documentedColumn,
) string {
	var changed []string
	for key, want := range documented {
		if got, ok := live[key]; ok && got != want {
			changed = append(
				changed,
				fmt.Sprintf("%s documented=%+v live=%+v", key, want, got),
			)
		}
	}
	sort.Strings(changed)
	return strings.Join(changed, "; ")
}
