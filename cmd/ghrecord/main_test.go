package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ewhauser/ghsync/internal/replay"
)

func TestCrawlerRecordingIsDeterministic(t *testing.T) {
	fixture := newGraphQLFixture(false)
	server := httptest.NewServer(fixture)
	defer server.Close()
	directory := t.TempDir()
	first := filepath.Join(directory, "first.ndjson")
	second := filepath.Join(directory, "second.ndjson")
	for _, output := range []string{first, second} {
		var stdout bytes.Buffer
		if err := runContext(
			t.Context(),
			testArguments(output),
			server.Client(),
			server.URL,
			&stdout,
		); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stdout.String(), "recorded ") {
			t.Fatalf("stdout = %q", stdout.String())
		}
	}
	firstBody, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBody, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBody, secondBody) {
		t.Fatal("same GraphQL input produced different recording bytes")
	}
	recording, err := replay.Read(bytes.NewReader(firstBody))
	if err != nil {
		t.Fatal(err)
	}
	if len(recording.Events) < 10 {
		t.Fatalf("recording has only %d events", len(recording.Events))
	}
}

func TestCrawlerResumesAfterRateLimit(t *testing.T) {
	fixture := newGraphQLFixture(true)
	server := httptest.NewServer(fixture)
	defer server.Close()
	output := filepath.Join(t.TempDir(), "resume.ndjson")
	args := testArguments(output)
	err := runContext(
		t.Context(),
		args,
		server.Client(),
		server.URL,
		io.Discard,
	)
	var limited *rateLimitError
	if err == nil || !errors.As(err, &limited) {
		t.Fatalf("first crawl error = %v, want rateLimitError", err)
	}
	if _, err := os.Stat(output + ".cursor.json"); err != nil {
		t.Fatalf("resume cursor was not persisted: %v", err)
	}
	if err := runContext(
		t.Context(),
		args,
		server.Client(),
		server.URL,
		io.Discard,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(output + ".cursor.json"); !os.IsNotExist(err) {
		t.Fatalf("completed cursor still exists: %v", err)
	}
	if fixture.operationCount("FrontierRecordingPulls") != 2 {
		t.Fatalf(
			"pull discovery calls = %d, want two pages",
			fixture.operationCount("FrontierRecordingPulls"),
		)
	}
	if fixture.requestCount("FrontierRecordingPull", "", 7) != 1 ||
		fixture.requestCount(
			"FrontierRecordingPull",
			"timeline-7-1",
			7,
		) != 1 {
		t.Fatalf(
			"completed timeline pages were refetched: first=%d second=%d",
			fixture.requestCount("FrontierRecordingPull", "", 7),
			fixture.requestCount(
				"FrontierRecordingPull",
				"timeline-7-1",
				7,
			),
		)
	}
}

func TestCrawlerResumesAfterCancellationWithoutRefetch(t *testing.T) {
	fixture := newGraphQLFixture(false)
	ctx, cancel := context.WithCancel(t.Context())
	fixture.cancelNested = cancel
	server := httptest.NewServer(fixture)
	defer server.Close()
	output := filepath.Join(t.TempDir(), "cancel.ndjson")
	args := testArguments(output)
	err := runContext(
		ctx,
		args,
		server.Client(),
		server.URL,
		io.Discard,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted crawl error = %v, want context.Canceled", err)
	}
	if err := runContext(
		t.Context(),
		args,
		server.Client(),
		server.URL,
		io.Discard,
	); err != nil {
		t.Fatal(err)
	}
	if fixture.requestCount("FrontierRecordingPulls", "", 0) != 1 ||
		fixture.requestCount(
			"FrontierRecordingPulls",
			"pulls-1",
			0,
		) != 1 ||
		fixture.requestCount("FrontierRecordingPull", "", 7) != 1 ||
		fixture.requestCount(
			"FrontierRecordingPull",
			"timeline-7-1",
			7,
		) != 1 {
		t.Fatalf("resume refetched completed pages: counts=%v", fixture.counts)
	}
	recordingFile, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer recordingFile.Close()
	if _, err := replay.Read(recordingFile); err != nil {
		t.Fatalf("resumed recording is corrupt: %v", err)
	}
}

func TestCrawlerPaginatesAndRecordsTrackTwoTimeline(t *testing.T) {
	fixture := newGraphQLFixture(false)
	server := httptest.NewServer(fixture)
	defer server.Close()
	output := filepath.Join(t.TempDir(), "coverage.ndjson")
	if err := runContext(
		t.Context(),
		testArguments(output),
		server.Client(),
		server.URL,
		io.Discard,
	); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recording, err := replay.Read(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.FirstLap(recording, replay.CompileOptions{
		Speed: 100,
	}); err != nil {
		t.Fatalf("compile fixture recording: %v", err)
	}
	required := map[string]bool{
		"commit":                        false,
		"pull_request/edited":           false,
		"pull_request/synchronize":      false,
		"pull_request/closed":           false,
		"pull_request/merged":           false,
		"pull_request/reopened":         false,
		"pull_request_review/submitted": false,
		"pull_request_review/dismissed": false,
		"review_thread/resolved":        false,
		"review_comment/created":        false,
		"review_comment/edited":         false,
		"check_suite/requested":         false,
		"check_suite/completed":         false,
		"check_run/created":             false,
		"check_run/completed":           false,
		"push/forced":                   false,
		"push/default":                  false,
		"stack":                         false,
	}
	var previousAt int64
	for _, event := range recording.Events {
		if event.AtMS < previousAt {
			t.Fatalf("event %d moved backward in time", event.Seq)
		}
		previousAt = event.AtMS
		key := event.Kind
		if event.Action != "" {
			key += "/" + event.Action
		}
		required[key] = true
		if event.Push != nil && event.Push.Forced {
			required["push/forced"] = true
		}
		if event.PullRequest != nil &&
			event.Action == "closed" &&
			event.PullRequest.Merged {
			required["pull_request/merged"] = true
		}
		if event.Kind == "pull_request" &&
			event.PullRequest != nil &&
			event.Action == "edited" {
			if event.PreviousBase == nil ||
				event.PreviousBase.Ref != "release" ||
				event.PullRequest.Base.Ref != "main" {
				t.Errorf(
					"base-ref change = previous %+v current %+v",
					event.PreviousBase,
					event.PullRequest.Base,
				)
			}
		}
		if (event.Kind == "review_comment" ||
			event.Kind == "review_thread") &&
			event.PullRequest.State != "open" {
			t.Errorf(
				"%s at %dms carried future PR state %+v",
				event.Kind,
				event.AtMS,
				*event.PullRequest,
			)
		}
		if event.Push != nil && event.Push.DefaultBranch {
			required["push/default"] = true
		}
		if event.Push != nil &&
			event.Push.Before == strings.Repeat("0", 40) {
			t.Errorf(
				"reconstructed push %s has an avoidable zero before SHA",
				event.Push.After,
			)
		}
	}
	for kind, found := range required {
		if !found {
			t.Errorf("recording is missing %s", kind)
		}
	}
	var stack *replay.Stack
	for index := range recording.Events {
		if recording.Events[index].Stack != nil {
			stack = recording.Events[index].Stack
			break
		}
	}
	if stack == nil ||
		!reflect.DeepEqual(stack.PullRequests, []int{7, 8}) {
		t.Fatalf("base-ref stack = %+v, want pull requests [7 8]", stack)
	}
	for _, request := range []struct {
		operation string
		after     string
		number    int
	}{
		{"FrontierRecordingPulls", "", 0},
		{"FrontierRecordingPulls", "pulls-1", 0},
		{"FrontierRecordingPull", "", 7},
		{"FrontierRecordingPull", "timeline-7-1", 7},
		{"FrontierRecordingThreads", "", 7},
		{"FrontierRecordingThreads", "threads-1", 7},
		{"FrontierRecordingCommitChecks", "suites-1", 0},
		{"FrontierRecordingCheckRuns", "runs-1", 0},
		{"FrontierRecordingThreadComments", "comments-1", 0},
		{"FrontierRecordingDefaultHistory", "", 0},
		{"FrontierRecordingDefaultHistory", "history-1", 0},
	} {
		if count := fixture.requestCount(
			request.operation,
			request.after,
			request.number,
		); count != 1 {
			t.Errorf(
				"%s after=%q number=%d calls = %d, want 1",
				request.operation,
				request.after,
				request.number,
				count,
			)
		}
	}
}

func TestParseBoundaryTreatsDateUntilAsInclusiveDay(t *testing.T) {
	since, err := parseBoundary("2026-07-01", false)
	if err != nil {
		t.Fatal(err)
	}
	until, err := parseBoundary("2026-07-01", true)
	if err != nil {
		t.Fatal(err)
	}
	if until.Sub(since) != 24*time.Hour {
		t.Fatalf("date window = %s, want 24h", until.Sub(since))
	}
}

func TestRunRejectsOutputCursorCollisionAndNonFiniteSynthesis(t *testing.T) {
	output := filepath.Join(t.TempDir(), "recording.ndjson")
	for _, args := range [][]string{
		append(testArguments(output), "--cursor="+output),
		append(testArguments(output), "--synthesize-stacks=NaN"),
	} {
		err := runContext(
			t.Context(),
			args,
			http.DefaultClient,
			"http://127.0.0.1:1/graphql",
			io.Discard,
		)
		if err == nil {
			t.Fatalf("runContext(%v) succeeded", args)
		}
	}
}

func TestCrawlerRejectsInvalidCursorPhase(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "recording.ndjson")
	cursorPath := output + ".cursor.json"
	cursor := crawlCursor{
		Version:          2,
		Owner:            "acme",
		Name:             "widgets",
		Since:            time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Until:            time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC),
		Phase:            "bogus",
		SynthesizeStacks: 100,
		Seed:             7,
	}
	body, err := json.Marshal(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cursorPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	fixture := newGraphQLFixture(false)
	server := httptest.NewServer(fixture)
	defer server.Close()
	err = runContext(
		t.Context(),
		testArguments(output),
		server.Client(),
		server.URL,
		io.Discard,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid phase") {
		t.Fatalf("invalid cursor error = %v", err)
	}
	if fixture.operationCount("FrontierRecordingPulls") != 0 {
		t.Fatal("invalid cursor reached GraphQL")
	}
}

func TestGraphQLForbiddenIsNotMisclassifiedAsRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			http.Error(
				writer,
				"Resource not accessible by personal access token",
				http.StatusForbidden,
			)
		},
	))
	defer server.Close()
	client, err := newGraphQLClient(server.URL, "token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.call(
		t.Context(),
		"query Test { viewer { login } }",
		nil,
		nil,
	)
	var limited *rateLimitError
	if err == nil || errors.As(err, &limited) {
		t.Fatalf("forbidden response error = %v, want non-rate-limit error", err)
	}
}

func TestGraphQLRateLimitUsesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Retry-After", "60")
			http.Error(writer, "secondary rate limit", http.StatusForbidden)
		},
	))
	defer server.Close()
	client, err := newGraphQLClient(server.URL, "token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().UTC().Add(59 * time.Second)
	_, err = client.call(
		t.Context(),
		"query Test { viewer { login } }",
		nil,
		nil,
	)
	var limited *rateLimitError
	if !errors.As(err, &limited) {
		t.Fatalf("rate-limit response error = %v", err)
	}
	if limited.ResetAt.Before(before) ||
		limited.ResetAt.After(time.Now().UTC().Add(61*time.Second)) {
		t.Fatalf("retry-after reset = %s, want about 60s", limited.ResetAt)
	}
}

func testArguments(output string) []string {
	return []string{
		"--repo=acme/widgets",
		"--since=2026-07-01",
		"--until=2026-07-01",
		"--token=test-token",
		"--out=" + output,
		"--synthesize-stacks=100",
		"--seed=7",
	}
}

type graphQLFixture struct {
	mu              sync.Mutex
	counts          map[string]int
	failFirstNested bool
	cancelNested    context.CancelFunc
	interrupted     bool
}

func newGraphQLFixture(failFirstNested bool) *graphQLFixture {
	return &graphQLFixture{
		counts:          make(map[string]int),
		failFirstNested: failFirstNested,
	}
}

func (f *graphQLFixture) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	if request.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(writer, "missing token", http.StatusUnauthorized)
		return
	}
	var call struct {
		Query     string                     `json:"query"`
		Variables map[string]json.RawMessage `json:"variables"`
	}
	if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	operation := fixtureOperation(call.Query)
	after := fixtureStringVariable(call.Variables, "after")
	number := fixtureIntVariable(call.Variables, "number")
	key := fixtureRequestKey(operation, after, number)
	f.mu.Lock()
	f.counts[operation]++
	f.counts[key]++
	fail := operation == "FrontierRecordingCheckRuns" &&
		f.failFirstNested && !f.interrupted
	cancel := operation == "FrontierRecordingCheckRuns" &&
		f.cancelNested != nil && !f.interrupted
	if fail || cancel {
		f.interrupted = true
	}
	cancelFunc := f.cancelNested
	f.mu.Unlock()
	if fail {
		writer.Header().Set("X-RateLimit-Reset", "1782925200")
		http.Error(writer, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	if cancel {
		cancelFunc()
		<-request.Context().Done()
		return
	}
	data := f.response(operation, call.Variables)
	if data == nil {
		http.Error(writer, "unknown query "+operation, http.StatusBadRequest)
		return
	}
	data["rateLimit"] = map[string]any{
		"cost": 1, "remaining": 4999, "resetAt": "2026-07-01T13:00:00Z",
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{"data": data})
}

func (f *graphQLFixture) operationCount(operation string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[operation]
}

func (f *graphQLFixture) requestCount(
	operation string,
	after string,
	number int,
) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[fixtureRequestKey(operation, after, number)]
}

func fixtureRequestKey(operation string, after string, number int) string {
	return fmt.Sprintf("%s\x00%s\x00%d", operation, after, number)
}

func fixtureStringVariable(
	variables map[string]json.RawMessage,
	name string,
) string {
	var result string
	_ = json.Unmarshal(variables[name], &result)
	return result
}

func fixtureIntVariable(
	variables map[string]json.RawMessage,
	name string,
) int {
	var result int
	_ = json.Unmarshal(variables[name], &result)
	return result
}

func (f *graphQLFixture) response(
	operation string,
	variables map[string]json.RawMessage,
) map[string]any {
	after := fixtureStringVariable(variables, "after")
	number := fixtureIntVariable(variables, "number")
	switch operation {
	case "FrontierRecordingPulls":
		nodes := []any{map[string]any{"number": 7}}
		page := map[string]any{
			"hasNextPage": true, "endCursor": "pulls-1",
		}
		if after == "pulls-1" {
			nodes = []any{map[string]any{"number": 8}}
			page = map[string]any{
				"hasNextPage": false, "endCursor": nil,
			}
		}
		return map[string]any{
			"repository": map[string]any{
				"id": "R_widget", "databaseId": 100, "name": "widgets",
				"nameWithOwner": "acme/widgets",
				"updatedAt":     "2026-07-01T20:00:00Z",
				"owner":         map[string]any{"login": "acme"},
				"defaultBranchRef": map[string]any{
					"name": "main",
					"target": map[string]any{
						"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					},
				},
			},
			"search": map[string]any{
				"issueCount": 2,
				"pageInfo":   page,
				"nodes":      nodes,
			},
		}
	case "FrontierRecordingPull":
		return map[string]any{
			"repository": map[string]any{
				"pullRequest": fixturePullRequest(number, after),
			},
		}
	case "FrontierRecordingThreads":
		nodes := []any{}
		page := map[string]any{
			"hasNextPage": false, "endCursor": nil,
		}
		if number == 7 && after == "" {
			nodes = []any{fixtureThread()}
			page = map[string]any{
				"hasNextPage": true, "endCursor": "threads-1",
			}
		} else if number == 7 && after == "threads-1" {
			nodes = []any{fixtureThreadTwo()}
		}
		return map[string]any{
			"repository": map[string]any{
				"pullRequest": map[string]any{
					"reviewThreads": map[string]any{
						"pageInfo": page,
						"nodes":    nodes,
					},
				},
			},
		}
	case "FrontierRecordingCommitChecks":
		return fixtureCommitChecksResponse()
	case "FrontierRecordingCheckRuns":
		return fixtureCheckRunsResponse()
	case "FrontierRecordingThreadComments":
		return fixtureThreadCommentsResponse()
	case "FrontierRecordingDefaultHistory":
		commits := []any{
			fixtureCommit(
				"dddddddddddddddddddddddddddddddddddddddd",
				900,
				false,
			),
		}
		page := map[string]any{
			"hasNextPage": true, "endCursor": "history-1",
		}
		if after == "history-1" {
			commits = []any{
				fixtureCommit(
					"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
					910,
					false,
				),
			}
			page = map[string]any{
				"hasNextPage": false, "endCursor": nil,
			}
		}
		return map[string]any{
			"repository": map[string]any{
				"defaultBranchRef": map[string]any{
					"name": "main",
					"target": map[string]any{
						"history": map[string]any{
							"pageInfo": page,
							"nodes":    commits,
						},
					},
				},
			},
		}
	default:
		return nil
	}
}

func fixtureOperation(query string) string {
	for _, operation := range []string{
		"FrontierRecordingPulls",
		"FrontierRecordingPull",
		"FrontierRecordingThreads",
		"FrontierRecordingDefaultHistory",
		"FrontierRecordingCommitChecks",
		"FrontierRecordingCheckRuns",
		"FrontierRecordingThreadComments",
	} {
		if strings.Contains(query, "query "+operation+"(") {
			return operation
		}
	}
	return ""
}

func fixturePullRequest(number int, after string) map[string]any {
	if number == 8 {
		return map[string]any{
			"id": "PR_child", "databaseId": int64(708), "number": 8,
			"title": "Stacked child", "state": "OPEN", "isDraft": false,
			"merged": false, "createdAt": "2026-07-01T02:00:00Z",
			"updatedAt": "2026-07-01T11:00:00Z", "closedAt": nil,
			"mergedAt": nil, "reviewDecision": "REVIEW_REQUIRED",
			"mergeable": "MERGEABLE", "headRefName": "feature/child",
			"headRefOid":  "cccccccccccccccccccccccccccccccccccccccc",
			"baseRefName": "feature/widget",
			"baseRefOid":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"headRepository": map[string]any{
				"nameWithOwner": "acme/widgets",
			},
			"baseRepository": map[string]any{
				"nameWithOwner": "acme/widgets",
			},
			"author": map[string]any{"login": "octocat"},
			"timelineItems": map[string]any{
				"pageInfo": map[string]any{
					"hasNextPage": false, "endCursor": nil,
				},
				"nodes": []any{
					map[string]any{
						"__typename": "PullRequestCommit",
						"commit": fixtureCommit(
							"cccccccccccccccccccccccccccccccccccccccc",
							720,
							false,
						),
					},
				},
			},
		}
	}
	if after == "timeline-7-1" {
		return fixturePullRequestPageTwo()
	}
	return map[string]any{
		"id": "PR_widget", "databaseId": 700, "number": 7,
		"title": "Widget change", "state": "OPEN", "isDraft": false,
		"merged": false, "createdAt": "2026-07-01T01:00:00Z",
		"updatedAt": "2026-07-01T10:00:00Z", "closedAt": nil,
		"mergedAt": nil, "reviewDecision": "APPROVED",
		"mergeable": "MERGEABLE", "headRefName": "feature/widget",
		"headRefOid":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"baseRefName": "main",
		"baseRefOid":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"headRepository": map[string]any{
			"nameWithOwner": "acme/widgets",
		},
		"baseRepository": map[string]any{
			"nameWithOwner": "acme/widgets",
		},
		"author": map[string]any{"login": "octocat"},
		"timelineItems": map[string]any{
			"pageInfo": map[string]any{
				"hasNextPage": true, "endCursor": "timeline-7-1",
			},
			"nodes": []any{
				map[string]any{
					"__typename": "PullRequestCommit",
					"commit": fixtureCommit(
						"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
						700,
						true,
					),
				},
				map[string]any{
					"__typename": "PullRequestReview",
					"id":         "__PRR__", "databaseId": 800,
					"body": "Looks good.", "state": "APPROVED",
					"submittedAt": "2026-07-01T06:00:00Z",
					"updatedAt":   "2026-07-01T06:00:00Z",
					"author":      map[string]any{"login": "reviewer"},
					"commit": map[string]any{
						"oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					},
				},
				map[string]any{
					"__typename":      "BaseRefChangedEvent",
					"createdAt":       "2026-07-01T06:30:00Z",
					"previousRefName": "release",
					"currentRefName":  "main",
				},
			},
		},
	}
}

func fixturePullRequestPageTwo() map[string]any {
	pull := fixturePullRequest(7, "")
	pull["timelineItems"] = map[string]any{
		"pageInfo": map[string]any{
			"hasNextPage": false, "endCursor": nil,
		},
		"nodes": []any{
			fixtureThread(),
			map[string]any{
				"__typename":          "ReviewDismissedEvent",
				"createdAt":           "2026-07-01T07:00:00Z",
				"previousReviewState": "APPROVED",
				"review": map[string]any{
					"id": "__PRR__", "databaseId": int64(800),
					"body": "Looks good.", "state": "DISMISSED",
					"submittedAt": "2026-07-01T06:00:00Z",
					"updatedAt":   "2026-07-01T07:00:00Z",
					"author":      map[string]any{"login": "reviewer"},
					"commit": map[string]any{
						"oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					},
				},
			},
			map[string]any{
				"__typename": "HeadRefForcePushedEvent",
				"createdAt":  "2026-07-01T08:00:00Z",
				"beforeCommit": map[string]any{
					"oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
				"afterCommit": map[string]any{
					"oid": "ffffffffffffffffffffffffffffffffffffffff",
					"parents": map[string]any{
						"nodes": []any{map[string]any{
							"oid": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
						}},
					},
				},
			},
			map[string]any{
				"__typename": "ClosedEvent",
				"createdAt":  "2026-07-01T08:30:00Z",
			},
			map[string]any{
				"__typename": "ReopenedEvent",
				"createdAt":  "2026-07-01T09:00:00Z",
			},
			map[string]any{
				"__typename": "MergedEvent",
				"createdAt":  "2026-07-01T10:00:00Z",
				"commit": map[string]any{
					"oid": "dddddddddddddddddddddddddddddddddddddddd",
				},
			},
		},
	}
	return pull
}

func fixtureCommit(sha string, idBase int64, paginate bool) map[string]any {
	started := "2026-07-01T03:00:00Z"
	completed := "2026-07-01T03:01:00Z"
	suitesPage := map[string]any{
		"hasNextPage": false, "endCursor": nil,
	}
	runsPage := map[string]any{
		"hasNextPage": false, "endCursor": nil,
	}
	if paginate {
		suitesPage = map[string]any{
			"hasNextPage": true, "endCursor": "suites-1",
		}
		runsPage = map[string]any{
			"hasNextPage": true, "endCursor": "runs-1",
		}
	}
	parent := "1111111111111111111111111111111111111111"
	switch sha {
	case "cccccccccccccccccccccccccccccccccccccccc":
		parent = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	case "dddddddddddddddddddddddddddddddddddddddd":
		parent = "cccccccccccccccccccccccccccccccccccccccc"
	case "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee":
		parent = "dddddddddddddddddddddddddddddddddddddddd"
	}
	return map[string]any{
		"oid": sha, "committedDate": "2026-07-01T02:00:00Z",
		"parents": map[string]any{
			"nodes": []any{map[string]any{"oid": parent}},
		},
		"checkSuites": map[string]any{
			"pageInfo": suitesPage,
			"nodes": []any{
				map[string]any{
					"id": "__CS__", "databaseId": idBase + 1,
					"status": "COMPLETED", "conclusion": "SUCCESS",
					"createdAt": started, "updatedAt": completed,
					"app": map[string]any{"slug": "github-actions"},
					"checkRuns": map[string]any{
						"pageInfo": runsPage,
						"nodes": []any{
							map[string]any{
								"id": "__CR__", "databaseId": idBase + 2,
								"name": "unit", "status": "COMPLETED",
								"conclusion": "SUCCESS",
								"detailsUrl": "https://example.test/actions",
								"startedAt":  started, "completedAt": completed,
							},
						},
					},
				},
			},
		},
	}
}

func fixtureThread() map[string]any {
	return map[string]any{
		"__typename": "PullRequestReviewThread",
		"id":         "PRRT_widget", "isResolved": true, "isOutdated": false,
		"path": "widget.go", "line": 12,
		"comments": map[string]any{
			"pageInfo": map[string]any{
				"hasNextPage": true, "endCursor": "comments-1",
			},
			"nodes": []any{
				map[string]any{
					"id": "PRRC_widget", "databaseId": 801,
					"pullRequestReview": map[string]any{"databaseId": 800},
					"body":              "Please test this.", "path": "widget.go", "line": 12,
					"createdAt": "2026-07-01T05:00:00Z",
					"updatedAt": "2026-07-01T05:00:00Z",
					"author":    map[string]any{"login": "reviewer"},
				},
			},
		},
	}
}

func fixtureThreadTwo() map[string]any {
	return map[string]any{
		"id": "PRRT_widget_2", "isResolved": false, "isOutdated": false,
		"path": "widget.go", "line": 20,
		"comments": map[string]any{
			"pageInfo": map[string]any{
				"hasNextPage": false, "endCursor": nil,
			},
			"nodes": []any{
				map[string]any{
					"id": "PRRC_widget_3", "databaseId": int64(803),
					"pullRequestReview": map[string]any{
						"databaseId": int64(800),
					},
					"body": "One more thought.", "path": "widget.go", "line": 20,
					"createdAt": "2026-07-01T05:45:00Z",
					"updatedAt": "2026-07-01T05:45:00Z",
					"author":    map[string]any{"login": "reviewer"},
				},
			},
		},
	}
}

func fixtureCommitChecksResponse() map[string]any {
	completed := "2026-07-01T03:02:00Z"
	return map[string]any{
		"repository": map[string]any{
			"object": map[string]any{
				"checkSuites": map[string]any{
					"pageInfo": map[string]any{
						"hasNextPage": false, "endCursor": nil,
					},
					"nodes": []any{
						map[string]any{
							"id": "__CS_2__", "databaseId": int64(711),
							"status": "COMPLETED", "conclusion": "SKIPPED",
							"createdAt": "2026-07-01T03:01:00Z",
							"updatedAt": completed,
							"app":       map[string]any{"slug": "github-actions"},
							"checkRuns": map[string]any{
								"pageInfo": map[string]any{
									"hasNextPage": false, "endCursor": nil,
								},
								"nodes": []any{
									map[string]any{
										"id": "__CR_3__", "databaseId": int64(712),
										"name": "lint", "status": "COMPLETED",
										"conclusion":  "SUCCESS",
										"detailsUrl":  "https://example.test/lint",
										"startedAt":   "2026-07-01T03:01:00Z",
										"completedAt": completed,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func fixtureCheckRunsResponse() map[string]any {
	return map[string]any{
		"node": map[string]any{
			"checkRuns": map[string]any{
				"pageInfo": map[string]any{
					"hasNextPage": false, "endCursor": nil,
				},
				"nodes": []any{
					map[string]any{
						"id": "__CR_2__", "databaseId": int64(703),
						"name": "integration", "status": "COMPLETED",
						"conclusion":  "SUCCESS",
						"detailsUrl":  "https://example.test/integration",
						"startedAt":   "2026-07-01T03:00:00Z",
						"completedAt": "2026-07-01T03:01:30Z",
					},
				},
			},
		},
	}
}

func fixtureThreadCommentsResponse() map[string]any {
	return map[string]any{
		"node": map[string]any{
			"comments": map[string]any{
				"pageInfo": map[string]any{
					"hasNextPage": false, "endCursor": nil,
				},
				"nodes": []any{
					map[string]any{
						"id": "PRRC_widget_2", "databaseId": int64(802),
						"pullRequestReview": map[string]any{
							"databaseId": int64(800),
						},
						"body": "Done.", "path": "widget.go", "line": 12,
						"createdAt": "2026-07-01T05:30:00Z",
						"updatedAt": "2026-07-01T05:31:00Z",
						"author":    map[string]any{"login": "octocat"},
					},
				},
			},
		},
	}
}
