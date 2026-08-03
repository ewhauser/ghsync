package gh_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
)

func TestPullRequestFilesPaginationAndCompletenessBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		count     int
		total     int
		omitted   bool
		wantNodes int
		truncated bool
	}{
		{
			name: "cursor complete across page boundary", count: 101,
			total: 101, wantNodes: 101,
		},
		{
			name: "exact documented cap is complete", count: 3000,
			total: 3000, wantNodes: gh.MaxPullRequestFiles,
		},
		{
			name: "documented cap is explicit", count: 3001,
			total: 3001, wantNodes: gh.MaxPullRequestFiles, truncated: true,
		},
		{
			name: "reported total mismatch is explicit", count: 100,
			total: 101, wantNodes: 100, truncated: true,
		},
		{
			name: "omitted connection is preserved", count: 1,
			total: 1, omitted: true, truncated: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := fakegithub.DefaultFixture()
			pull := &fixture.PullRequests[0]
			pull.ChangedFiles = make([]fakegithub.ChangedFile, test.count)
			for index := range pull.ChangedFiles {
				pull.ChangedFiles[index] = fakegithub.ChangedFile{
					Path:       fmt.Sprintf("src/file-%04d.go", index),
					ChangeType: "modified",
				}
			}
			pull.ChangedFilesTotal = test.total
			pull.ChangedFilesOmitted = test.omitted
			server := httptest.NewServer(fakegithub.New(fixture, "secret"))
			t.Cleanup(server.Close)
			client, err := gh.NewGraphQLClient(
				server.URL,
				budget.New(server.Client(), budget.Options{}),
				gh.StaticToken("fake-installation-files"),
			)
			if err != nil {
				t.Fatal(err)
			}
			nodes, _, err := client.BatchPullRequests(
				context.Background(),
				budget.Interactive,
				[]string{pull.NodeID},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) != 1 || nodes[0] == nil {
				t.Fatalf("nodes = %#v", nodes)
			}
			if test.omitted {
				if nodes[0].Files != nil {
					t.Fatalf("omitted files = %#v, want nil", nodes[0].Files)
				}
				return
			}
			files := nodes[0].Files
			if len(files.Nodes) != test.wantNodes ||
				files.Truncated != test.truncated ||
				files.TotalCount != test.total {
				t.Fatalf(
					"files nodes=%d total=%d truncated=%v, want %d/%d/%v",
					len(files.Nodes), files.TotalCount, files.Truncated,
					test.wantNodes, test.total, test.truncated,
				)
			}
		})
	}
}

func TestPullRequestFilesPaginationRejectsMidObservationSHARace(t *testing.T) {
	t.Parallel()
	fixture := fakegithub.DefaultFixture()
	pull := &fixture.PullRequests[0]
	pull.ChangedFiles = make([]fakegithub.ChangedFile, 101)
	for index := range pull.ChangedFiles {
		pull.ChangedFiles[index] = fakegithub.ChangedFile{
			Path: fmt.Sprintf("old/file-%03d.go", index), ChangeType: "modified",
		}
	}
	fake := fakegithub.New(
		fixture,
		"secret",
		fakegithub.WithRequestHook(func(method, path string, count int, fx *fakegithub.Fixture) {
			if method != "POST" || path != "/graphql" || count != 2 {
				return
			}
			fx.PullRequests[0].Head.SHA = "synchronized-head"
			fx.PullRequests[0].ChangedFiles[100] = fakegithub.ChangedFile{
				Path: "new/from-synchronize.go", ChangeType: "added",
			}
		}),
	)
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	client, err := gh.NewGraphQLClient(
		server.URL,
		budget.New(server.Client(), budget.Options{}),
		gh.StaticToken("fake-installation-files-race"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = client.BatchPullRequests(
		context.Background(),
		budget.Interactive,
		[]string{pull.NodeID},
	)
	if err == nil || !strings.Contains(err.Error(), "SHA fence changed") {
		t.Fatalf("pagination race error = %v", err)
	}
}
