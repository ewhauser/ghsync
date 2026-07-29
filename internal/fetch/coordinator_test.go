package fetch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acme/frontier/internal/budget"
	"github.com/acme/frontier/internal/fakegithub"
	"github.com/acme/frontier/internal/gh"
	"github.com/acme/frontier/internal/store"
)

type recordingBatchWriter struct {
	mu   sync.Mutex
	keys []string
}

func (w *recordingBatchWriter) ApplyPullRequestBatch(
	_ context.Context,
	pulls []store.PullRequestRecord,
) (map[string]store.ApplyPullRequestResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.keys = w.keys[:0]
	results := make(map[string]store.ApplyPullRequestResult, len(pulls))
	for _, pull := range pulls {
		key := "pr:" + pull.Repository.FullName + ":" +
			pullNumber(pull.Number)
		w.keys = append(w.keys, key)
		results[key] = store.ApplyPullRequestResult{
			Applied:    true,
			NewHeadSHA: pull.HeadSHA,
		}
	}
	return results, nil
}

func pullNumber(number int) string {
	return fmt.Sprintf("%d", number)
}

func TestCoordinatorGangsNodesAndSortsApplyOrder(t *testing.T) {
	fake := fakegithub.New(fakegithub.DefaultFixture(), "secret")
	server := httptest.NewServer(fake)
	defer server.Close()
	gate := budget.New(server.Client(), budget.Options{})
	graphQL, err := gh.NewGraphQLClient(
		server.URL,
		gate,
		gh.StaticToken("test-token"),
	)
	if err != nil {
		t.Fatal(err)
	}
	writer := &recordingBatchWriter{}
	coordinator := newPRCoordinator(graphQL, writer, 1, 1, 20*time.Millisecond)

	fixture := fakegithub.DefaultFixture()
	pulls := []fakegithub.PullRequest{
		fixture.PullRequests[4],
		fixture.PullRequests[1],
		fixture.PullRequests[3],
	}
	start := make(chan struct{})
	errs := make(chan error, len(pulls))
	var wait sync.WaitGroup
	for _, pull := range pulls {
		pull := pull
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := coordinator.submit(
				context.Background(),
				entityKey{
					Kind:   "pr",
					Repo:   "acme/monolith",
					Value:  pullNumber(pull.Number),
					Number: pull.Number,
				},
				pull.NodeID,
				store.FetchMetadata{
					NodeID:  pull.NodeID,
					HeadSHA: pull.Head.SHA,
				},
				budget.Event,
				store.SyncSourceWebhook,
			)
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := fake.RequestCount(http.MethodPost, "/graphql"); got != 1 {
		t.Fatalf("GraphQL fetches = %d, want 1 nodes() batch", got)
	}
	want := []string{
		"pr:acme/monolith:4812",
		"pr:acme/monolith:4816",
		"pr:acme/monolith:4820",
	}
	writer.mu.Lock()
	got := append([]string(nil), writer.keys...)
	writer.mu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("apply lock order = %v, want sorted %v", got, want)
	}
}

func TestParseEntityKeys(t *testing.T) {
	tests := []struct {
		raw  string
		kind string
		want entityKey
	}{
		{
			"pr:acme/monolith:4812",
			"pr",
			entityKey{Kind: "pr", Repo: "acme/monolith", Value: "4812", Number: 4812},
		},
		{
			"checks:acme/monolith:abc123",
			"checks",
			entityKey{Kind: "checks", Repo: "acme/monolith", Value: "abc123"},
		},
		{
			"branch:acme/monolith:feature/one",
			"branch",
			entityKey{Kind: "branch", Repo: "acme/monolith", Value: "feature/one"},
		},
	}
	for _, test := range tests {
		got, err := parseEntityKey(test.raw, test.kind)
		if err != nil {
			t.Fatalf("%s: %v", test.raw, err)
		}
		if got != test.want {
			t.Fatalf("%s parsed as %+v, want %+v", test.raw, got, test.want)
		}
	}
	for _, raw := range []string{"pr:no-repo:1", "pr:acme/repo:no", "stack:acme/repo:0"} {
		if _, err := parseEntityKey(raw, strings.Split(raw, ":")[0]); err == nil {
			t.Fatalf("invalid key %q accepted", raw)
		}
	}
}
