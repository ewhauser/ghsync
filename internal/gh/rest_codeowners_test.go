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

func TestPullRequestFileRenamesPaginationBoundaries(t *testing.T) {
	t.Parallel()
	for _, count := range []int{101, gh.MaxPullRequestFiles, gh.MaxPullRequestFiles + 1} {
		t.Run(fmt.Sprintf("files-%d", count), func(t *testing.T) {
			t.Parallel()
			fixture := fakegithub.DefaultFixture()
			pull := &fixture.PullRequests[0]
			pull.ChangedFiles = make([]fakegithub.ChangedFile, count)
			for index := range pull.ChangedFiles {
				pull.ChangedFiles[index] = fakegithub.ChangedFile{
					Path:         fmt.Sprintf("new/file-%04d.go", index),
					PreviousPath: fmt.Sprintf("old/file-%04d.go", index),
					ChangeType:   "renamed",
				}
			}
			server := httptest.NewServer(fakegithub.New(fixture, "secret"))
			t.Cleanup(server.Close)
			client, err := gh.NewRESTClient(
				server.URL,
				budget.New(server.Client(), budget.Options{}),
				gh.StaticToken("fake-installation-renames"),
			)
			if err != nil {
				t.Fatal(err)
			}
			renames, truncated, err := client.PullRequestFileRenames(
				context.Background(), budget.Interactive,
				fixture.Owner, fixture.Repo, pull.Number,
			)
			if err != nil {
				t.Fatal(err)
			}
			want := min(count, gh.MaxPullRequestFiles)
			if len(renames) != want || truncated != (count > gh.MaxPullRequestFiles) {
				t.Fatalf(
					"renames=%d truncated=%v, want %d/%v",
					len(renames), truncated, want,
					count > gh.MaxPullRequestFiles,
				)
			}
		})
	}
}

func TestFindCodeownersPrecedenceMissingAndOversized(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content map[string]string
		path    string
		state   string
		source  string
	}{
		{
			name: "github directory wins",
			content: map[string]string{
				".github/CODEOWNERS": "* @github",
				"CODEOWNERS":         "* @root",
				"docs/CODEOWNERS":    "* @docs",
			},
			path: ".github/CODEOWNERS", state: gh.CodeownersPresent,
			source: "* @github",
		},
		{
			name: "root wins over docs",
			content: map[string]string{
				"CODEOWNERS":      "* @root",
				"docs/CODEOWNERS": "* @docs",
			},
			path: "CODEOWNERS", state: gh.CodeownersPresent, source: "* @root",
		},
		{name: "missing is empty truth", state: gh.CodeownersMissing},
		{
			name: "oversized winner does not fall through",
			content: map[string]string{
				".github/CODEOWNERS": strings.Repeat("x", gh.MaxCodeownersBytes),
				"CODEOWNERS":         "* @root",
			},
			path: ".github/CODEOWNERS", state: gh.CodeownersOversized,
		},
		{
			name: "byte below size boundary remains present",
			content: map[string]string{
				"CODEOWNERS": strings.Repeat("x", gh.MaxCodeownersBytes-1),
			},
			path: "CODEOWNERS", state: gh.CodeownersPresent,
			source: strings.Repeat("x", gh.MaxCodeownersBytes-1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := fakegithub.DefaultFixture()
			fixture.Contents = map[string]map[string]string{
				"exact-sha": test.content,
			}
			server := httptest.NewServer(fakegithub.New(fixture, "secret"))
			t.Cleanup(server.Close)
			client, err := gh.NewRESTClient(
				server.URL,
				budget.New(server.Client(), budget.Options{}),
				gh.StaticToken("fake-installation-codeowners"),
			)
			if err != nil {
				t.Fatal(err)
			}
			source, err := client.FindCodeowners(
				context.Background(), budget.Interactive,
				fixture.Owner, fixture.Repo, "exact-sha",
			)
			if err != nil {
				t.Fatal(err)
			}
			if source.Path != test.path || source.State != test.state ||
				source.Content != test.source {
				t.Fatalf("source = %#v, want %q/%q/%q", source, test.path, test.state, test.source)
			}
		})
	}
}
