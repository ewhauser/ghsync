//nolint:gocritic // Conformance cases intentionally keep decoded webhook snapshots by value.
package fetch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/ewhauser/ghsync/internal/conformance"
	"github.com/ewhauser/ghsync/internal/dispatch"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/queue"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/testdb"
)

const (
	pullRequestCorpusCount = 28
	checkRunCorpusCount    = 8
	checkSuiteCorpusCount  = 8
	projectionETag         = `"projection-etag"`
)

func TestPullRequestCorpusProjection(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	examples, err := conformance.PayloadExamples("pull_request")
	if err != nil {
		t.Fatal(err)
	}
	if len(examples) != pullRequestCorpusCount {
		t.Fatalf(
			"pull_request projection examples = %d, want %d",
			len(examples),
			pullRequestCorpusCount,
		)
	}

	var sawLongString, sawNull bool
	for index, example := range examples { //nolint:paralleltest // cases update shared corpus-coverage sentinels
		t.Run(example.Filename, func(t *testing.T) {
			repo := fmt.Sprintf("acme/projection-pull-%02d", index)
			fixture := projectionFixture(index, repo)
			overlayCorpusRepository(
				t,
				corpusObject(t, example.Payload, "repository"),
				fixture.Repository,
			)
			wirePull := corpusObject(t, example.Payload, "pull_request")
			number := 60_000 + index
			wirePull["id"] = int64(8_000_000_000 + index)
			wirePull["node_id"] = fmt.Sprintf("conformance-pull-%d", index)
			wirePull["number"] = number
			wirePull["future_projection_field"] = map[string]any{
				"nested": []any{true, nil, "preserved"},
			}
			example.Payload["number"] = number
			if wirePull["body"] == nil {
				sawNull = true
			}
			if strings.HasSuffix(
				example.Filename,
				"opened.with-null-body.json",
			) {
				wirePull["title"] = strings.Repeat(
					"long projection title ",
					4096,
				)
				sawLongString = true
			}

			eventBody := marshalJSON(t, example.Payload)
			intent := requireProjectionIntent(
				t,
				"pull_request",
				eventBody,
				queue.KindResolveStackMembership,
			)
			responseBody := marshalJSON(t, wirePull)
			path := fmt.Sprintf(
				"/repos/%s/%s/pulls/%d",
				fixture.Owner,
				fixture.Repo,
				number,
			)
			middleware, requests := corpusRESTResponse(path, responseBody)
			_, server, handler, riverClient := newDirectHandlerWithMiddleware(
				t,
				database.Pool,
				fixture,
				time.Millisecond,
				100,
				middleware,
			)
			defer server.Close()
			handler.SetRiverClient(riverClient)
			if err := handler.ResolveStackMembership(
				t.Context(),
				queue.RefreshRequest{
					Args: queue.NewResolveStackMembershipArgs(
						intent.Key,
					).RefreshArgs,
					Queue: queue.QueueEvent,
				},
			); err != nil {
				t.Fatalf("project pull request through handler: %v", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("pull REST requests = %d, want 1", got)
			}

			row, err := dbgen.New(database.Pool).GetPullRequestByIdentity(
				t.Context(),
				dbgen.GetPullRequestByIdentityParams{
					RepoGhID: fixture.Repository.ID,
					PrNumber: int32(number),
				},
			)
			if err != nil {
				t.Fatalf("read projected pull request: %v", err)
			}
			assertPullRequestProjection(
				t,
				row,
				wirePull,
				int64(8_000_000_000+index),
				number,
			)
		})
	}
	if !sawNull {
		t.Fatal("pull_request corpus projection did not exercise a null body")
	}
	if !sawLongString {
		t.Fatal("pull_request corpus projection did not exercise a long string")
	}
}

func TestCheckRunCorpusProjection(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	examples, err := conformance.PayloadExamples("check_run")
	if err != nil {
		t.Fatal(err)
	}
	if len(examples) != checkRunCorpusCount {
		t.Fatalf(
			"check_run projection examples = %d, want %d",
			len(examples),
			checkRunCorpusCount,
		)
	}

	var sawLongString, sawNull bool
	for index, example := range examples { //nolint:paralleltest // cases update shared corpus-coverage sentinels
		t.Run(example.Filename, func(t *testing.T) {
			repo := fmt.Sprintf("acme/projection-check-%02d", index)
			fixture := projectionFixture(100+index, repo)
			overlayCorpusRepository(
				t,
				corpusObject(t, example.Payload, "repository"),
				fixture.Repository,
			)
			wireRun := corpusObject(t, example.Payload, "check_run")
			runID := int64(9_000_000_000 + index)
			headSHA := fmt.Sprintf("conformance-check-head-%02d", index)
			wireRun["id"] = runID
			wireRun["node_id"] = fmt.Sprintf("conformance-check-%d", index)
			wireRun["head_sha"] = headSHA
			wireRun["future_projection_field"] = map[string]any{
				"nested": []any{true, nil, "preserved"},
			}
			if wireRun["conclusion"] == nil ||
				wireRun["started_at"] == nil ||
				wireRun["completed_at"] == nil {
				sawNull = true
			}
			if index == 0 {
				wireRun["name"] = strings.Repeat(
					"long-check-name-",
					4096,
				)
				sawLongString = true
			}

			eventBody := marshalJSON(t, example.Payload)
			intent := requireProjectionIntent(
				t,
				"check_run",
				eventBody,
				queue.KindRefreshChecks,
			)
			responseBody := marshalJSON(t, map[string]any{
				"total_count": 1,
				"check_runs":  []any{wireRun},
			})
			path := fmt.Sprintf(
				"/repos/%s/%s/commits/%s/check-runs",
				fixture.Owner,
				fixture.Repo,
				headSHA,
			)
			middleware, requests := corpusRESTResponse(path, responseBody)
			_, server, handler, riverClient := newDirectHandlerWithMiddleware(
				t,
				database.Pool,
				fixture,
				time.Millisecond,
				100,
				middleware,
			)
			defer server.Close()
			handler.SetRiverClient(riverClient)
			if err := handler.RefreshChecks(
				t.Context(),
				queue.RefreshRequest{
					Args:  queue.NewRefreshChecksArgs(intent.Key).RefreshArgs,
					Queue: queue.QueueEvent,
				},
			); err != nil {
				t.Fatalf("project check run through handler: %v", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("check-run REST requests = %d, want 1", got)
			}

			row := readCheckRun(t, database, runID)
			assertCheckRunProjection(t, row, wireRun, runID, headSHA)
			observed := readCheckObservation(t, database, runID)
			assertJSONEqual(t, observed, marshalJSON(t, wireRun))
			var decoded map[string]any
			if err := json.Unmarshal(observed, &decoded); err != nil {
				t.Fatalf("decode stored check observation: %v", err)
			}
			if _, ok := decoded["future_projection_field"]; !ok {
				t.Fatal("stored check observation lost the unknown field")
			}
		})
	}
	if !sawNull {
		t.Fatal("check_run corpus projection did not exercise a null field")
	}
	if !sawLongString {
		t.Fatal("check_run corpus projection did not exercise a long string")
	}
}

func TestCheckSuiteCorpusProjection(t *testing.T) {
	t.Parallel()
	database := testdb.New(t)
	examples, err := conformance.PayloadExamples("check_suite")
	if err != nil {
		t.Fatal(err)
	}
	if len(examples) != checkSuiteCorpusCount {
		t.Fatalf(
			"check_suite projection examples = %d, want %d",
			len(examples),
			checkSuiteCorpusCount,
		)
	}
	runExamples, err := conformance.PayloadExamples("check_run")
	if err != nil {
		t.Fatal(err)
	}
	if len(runExamples) != checkRunCorpusCount {
		t.Fatalf(
			"check_suite backing check examples = %d, want %d",
			len(runExamples),
			checkRunCorpusCount,
		)
	}

	var sawNull bool
	for index, example := range examples { //nolint:paralleltest // cases update shared corpus-coverage sentinels
		t.Run(example.Filename, func(t *testing.T) {
			repo := fmt.Sprintf("acme/projection-suite-%02d", index)
			fixture := projectionFixture(200+index, repo)
			overlayCorpusRepository(
				t,
				corpusObject(t, example.Payload, "repository"),
				fixture.Repository,
			)
			suite := corpusObject(t, example.Payload, "check_suite")
			headSHA := fmt.Sprintf("conformance-suite-head-%02d", index)
			suite["head_sha"] = headSHA
			suite["future_projection_field"] = map[string]any{
				"nested": []any{true, nil, strings.Repeat("suite-extra-", 512)},
			}
			if suite["conclusion"] == nil {
				sawNull = true
			}

			eventBody := marshalJSON(t, example.Payload)
			intent := requireProjectionIntent(
				t,
				"check_suite",
				eventBody,
				queue.KindRefreshChecks,
			)
			wireRun := cloneJSONObject(
				t,
				corpusObject(
					t,
					runExamples[index%len(runExamples)].Payload,
					"check_run",
				),
			)
			runID := int64(9_100_000_000 + index)
			wireRun["id"] = runID
			wireRun["node_id"] = fmt.Sprintf("suite-check-%d", index)
			wireRun["head_sha"] = headSHA
			wireRun["name"] = fmt.Sprintf("suite-check-%d", index)
			wireRun["future_projection_field"] = "preserved through suite fetch"
			responseBody := marshalJSON(t, map[string]any{
				"total_count": 1,
				"check_runs":  []any{wireRun},
			})
			path := fmt.Sprintf(
				"/repos/%s/%s/commits/%s/check-runs",
				fixture.Owner,
				fixture.Repo,
				headSHA,
			)
			middleware, requests := corpusRESTResponse(path, responseBody)
			_, server, handler, riverClient := newDirectHandlerWithMiddleware(
				t,
				database.Pool,
				fixture,
				time.Millisecond,
				100,
				middleware,
			)
			defer server.Close()
			handler.SetRiverClient(riverClient)
			if err := handler.RefreshChecks(
				t.Context(),
				queue.RefreshRequest{
					Args:  queue.NewRefreshChecksArgs(intent.Key).RefreshArgs,
					Queue: queue.QueueEvent,
				},
			); err != nil {
				t.Fatalf("project check suite through handler: %v", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("check-suite REST requests = %d, want 1", got)
			}

			row := readCheckRun(t, database, runID)
			if row.HeadSha != headSHA ||
				row.Name != fmt.Sprintf("suite-check-%d", index) {
				t.Fatalf("check-suite projected row = %+v", row)
			}
			metadata, err := handler.writer.ChecksMetadata(
				t.Context(),
				repo,
				headSHA,
			)
			if err != nil {
				t.Fatalf("read check-suite projection metadata: %v", err)
			}
			if metadata.RepoGitHubID != fixture.Repository.ID ||
				metadata.RepoFullName != repo ||
				metadata.ETag != projectionETag {
				t.Fatalf("check-suite projection metadata = %+v", metadata)
			}
			assertJSONEqual(
				t,
				readCheckObservation(t, database, runID),
				marshalJSON(t, wireRun),
			)
		})
	}
	if !sawNull {
		t.Fatal("check_suite corpus projection did not exercise a null field")
	}
}

func projectionFixture(index int, fullName string) fakegithub.Fixture {
	fixture := fixtureForRepo(fakegithub.DefaultFixture(), fullName)
	fixture.Repository.ID = int64(7_000_000 + index)
	fixture.Repository.NodeID = fmt.Sprintf("projection-repo-%d", index)
	fixture.Repositories = []fakegithub.Repository{fixture.Repository}
	return fixture
}

func overlayCorpusRepository(
	t *testing.T,
	payload map[string]any,
	repository fakegithub.Repository,
) {
	t.Helper()
	payload["id"] = repository.ID
	payload["node_id"] = repository.NodeID
	payload["name"] = repository.Name
	payload["full_name"] = repository.FullName
	payload["default_branch"] = repository.DefaultBranch
	payload["archived"] = repository.Archived
	payload["updated_at"] = repository.UpdatedAt.UTC().Format(time.RFC3339)
	payload["pushed_at"] = repository.PushedAt.UTC().Format(time.RFC3339)
	owner := corpusObject(t, payload, "owner")
	owner["login"] = repository.Owner
}

func corpusObject(
	t *testing.T,
	parent map[string]any,
	key string,
) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("corpus field %q = %T, want object", key, parent[key])
	}
	return value
}

func cloneJSONObject(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	body := marshalJSON(t, value)
	var cloned map[string]any
	if err := json.Unmarshal(body, &cloned); err != nil {
		t.Fatalf("clone corpus object: %v", err)
	}
	return cloned
}

func marshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode JSON: %v", err)
	}
	return body
}

func requireProjectionIntent(
	t *testing.T,
	event string,
	body []byte,
	kind string,
) dispatch.Intent {
	t.Helper()
	intents, err := dispatch.DefaultClassifier().Classify(event, body)
	if err != nil {
		t.Fatalf("classify %s corpus payload: %v", event, err)
	}
	var matches []dispatch.Intent
	for _, intent := range intents {
		if intent.Kind == kind {
			matches = append(matches, intent)
		}
	}
	if len(matches) != 1 || matches[0].Key == "" {
		t.Fatalf(
			"%s intents = %+v, want one non-empty %s intent",
			event,
			intents,
			kind,
		)
	}
	return matches[0]
}

func corpusRESTResponse(
	path string,
	body []byte,
) (func(http.Handler) http.Handler, *atomic.Int32) {
	requests := &atomic.Int32{}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.Path != path {
				next.ServeHTTP(w, r)
				return
			}
			requests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("ETag", projectionETag)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		})
	}, requests
}

func assertPullRequestProjection(
	t *testing.T,
	row dbgen.GetPullRequestByIdentityRow,
	wire map[string]any,
	gitHubID int64,
	number int,
) {
	t.Helper()
	updatedAt := requiredJSONTime(t, wire, "updated_at")
	if !row.GhID.Valid ||
		row.GhID.Int64 != gitHubID ||
		row.NodeID != requiredJSONString(t, wire, "node_id") ||
		int(row.Number) != number ||
		row.Title != requiredJSONString(t, wire, "title") ||
		row.State != strings.ToLower(requiredJSONString(t, wire, "state")) ||
		row.Draft != optionalJSONBool(wire, "draft") ||
		row.AuthorLogin != requiredNestedJSONString(t, wire, "user", "login") ||
		row.HeadRef != requiredNestedJSONString(t, wire, "head", "ref") ||
		row.HeadSha != requiredNestedJSONString(t, wire, "head", "sha") ||
		row.BaseRef != requiredNestedJSONString(t, wire, "base", "ref") ||
		row.BaseSha != requiredNestedJSONString(t, wire, "base", "sha") ||
		row.ReviewDecision != optionalJSONString(wire, "review_decision") ||
		row.MergeableState != optionalJSONString(wire, "mergeable_state") ||
		row.StackNumber.Valid ||
		row.StackPosition.Valid ||
		!row.GhUpdatedAt.Valid ||
		!row.GhUpdatedAt.Time.Equal(updatedAt) ||
		row.Etag != projectionETag ||
		row.SyncSource != string(store.SyncSourceWebhook) {
		t.Fatalf("projected pull request = %+v", row)
	}
}

func readCheckRun(
	t *testing.T,
	database *testdb.Database,
	gitHubID int64,
) dbgen.CheckRun {
	t.Helper()
	var row dbgen.CheckRun
	if err := database.Pool.QueryRow(t.Context(), `
		SELECT gh_id, repo_id, node_id, name, status, conclusion,
		       details_url, app_slug, started_at, completed_at,
		       gh_updated_at, head_sha, synced_at, etag, sync_source,
		       tombstoned_at, semantic_version, last_checked_at
		FROM check_runs
		WHERE gh_id = $1
	`, gitHubID).Scan(
		&row.GhID,
		&row.RepoID,
		&row.NodeID,
		&row.Name,
		&row.Status,
		&row.Conclusion,
		&row.DetailsUrl,
		&row.AppSlug,
		&row.StartedAt,
		&row.CompletedAt,
		&row.GhUpdatedAt,
		&row.HeadSha,
		&row.SyncedAt,
		&row.Etag,
		&row.SyncSource,
		&row.TombstonedAt,
		&row.SemanticVersion,
		&row.LastCheckedAt,
	); err != nil {
		t.Fatalf("read projected check run: %v", err)
	}
	return row
}

func readCheckObservation(
	t *testing.T,
	database *testdb.Database,
	gitHubID int64,
) []byte {
	t.Helper()
	var observed []byte
	if err := database.Pool.QueryRow(t.Context(), `
		SELECT observed
		FROM check_history
		WHERE check_run_gh_id = $1
		ORDER BY id DESC
		LIMIT 1
	`, gitHubID).Scan(&observed); err != nil {
		t.Fatalf("read check observation: %v", err)
	}
	return observed
}

func assertCheckRunProjection(
	t *testing.T,
	row dbgen.CheckRun,
	wire map[string]any,
	gitHubID int64,
	headSHA string,
) {
	t.Helper()
	startedAt := optionalJSONTime(t, wire, "started_at")
	completedAt := optionalJSONTime(t, wire, "completed_at")
	semanticTime := completedAt
	if semanticTime == nil {
		semanticTime = startedAt
	}
	if row.GhID != gitHubID ||
		row.NodeID != optionalJSONString(wire, "node_id") ||
		row.Name != requiredJSONString(t, wire, "name") ||
		row.Status != requiredJSONString(t, wire, "status") ||
		row.Conclusion != optionalJSONString(wire, "conclusion") ||
		row.DetailsUrl != optionalJSONString(wire, "details_url") ||
		row.AppSlug != optionalNestedJSONString(wire, "app", "slug") ||
		!timestampsEqual(row.StartedAt, startedAt) ||
		!timestampsEqual(row.CompletedAt, completedAt) ||
		!timestampsEqual(row.GhUpdatedAt, semanticTime) ||
		row.HeadSha != headSHA ||
		row.Etag != projectionETag ||
		row.SyncSource != string(store.SyncSourceWebhook) ||
		row.TombstonedAt.Valid ||
		row.SemanticVersion == "" {
		t.Fatalf("projected check run = %+v", row)
	}
}

func requiredJSONString(
	t *testing.T,
	object map[string]any,
	key string,
) string {
	t.Helper()
	value, ok := object[key].(string)
	if !ok {
		t.Fatalf("JSON field %q = %#v, want string", key, object[key])
	}
	return value
}

func optionalJSONString(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return value
}

func requiredNestedJSONString(
	t *testing.T,
	object map[string]any,
	parent string,
	key string,
) string {
	t.Helper()
	return requiredJSONString(t, corpusObject(t, object, parent), key)
}

func optionalNestedJSONString(
	object map[string]any,
	parent string,
	key string,
) string {
	nested, _ := object[parent].(map[string]any)
	return optionalJSONString(nested, key)
}

func optionalJSONBool(object map[string]any, key string) bool {
	value, _ := object[key].(bool)
	return value
}

func requiredJSONTime(
	t *testing.T,
	object map[string]any,
	key string,
) time.Time {
	t.Helper()
	value := optionalJSONTime(t, object, key)
	if value == nil {
		t.Fatalf("JSON field %q is null or missing", key)
	}
	return *value
}

func optionalJSONTime(
	t *testing.T,
	object map[string]any,
	key string,
) *time.Time {
	t.Helper()
	raw, exists := object[key]
	if !exists || raw == nil {
		return nil
	}
	value, ok := raw.(string)
	if !ok {
		t.Fatalf("JSON field %q = %#v, want timestamp or null", key, raw)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("parse JSON field %q: %v", key, err)
	}
	return &parsed
}

func timestampsEqual(
	actual pgtype.Timestamptz,
	expected *time.Time,
) bool {
	if expected == nil {
		return !actual.Valid
	}
	return actual.Valid && actual.Time.Equal(*expected)
}

func assertJSONEqual(t *testing.T, actual []byte, expected []byte) {
	t.Helper()
	var actualValue, expectedValue any
	if err := json.Unmarshal(actual, &actualValue); err != nil {
		t.Fatalf("decode actual JSON: %v", err)
	}
	if err := json.Unmarshal(expected, &expectedValue); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	if !reflect.DeepEqual(actualValue, expectedValue) {
		t.Fatalf("JSON differs\nactual: %s\nexpected: %s", actual, expected)
	}
}
