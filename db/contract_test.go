package db_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/acme/frontier/internal/outbox"
	"github.com/acme/frontier/internal/testdb"
)

type documentedColumn struct {
	Table    string
	Column   string
	Type     string
	Nullable string
}

func TestPublicSchemaManifestMatchesMigratedDatabase(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
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
	database, err := testdb.Open(ctx, databaseURL, "contract")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
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

func TestEventManifestMatchesWriterDefinitions(t *testing.T) {
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

func TestV1EntityKeyConstructors(t *testing.T) {
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
	startIndex := strings.Index(string(content), start)
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
	for _, line := range strings.Split(section, "\n") {
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
