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
			fake := fakegithub.New(fixture, "secret")
			server := httptest.NewServer(fake)
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
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if source.Ref != "exact-sha" || source.Path != test.path || source.State != test.state ||
				source.Content != test.source {
				t.Fatalf("source = %#v, want exact-sha/%q/%q/%q", source, test.path, test.state, test.source)
			}
			if source.State != gh.CodeownersMissing && source.ETag == "" {
				t.Fatal("effective source has no ETag")
			}
			before := len(fake.Requests())
			reused, err := client.FindCodeowners(
				context.Background(), budget.Interactive,
				fixture.Owner, fixture.Repo, "exact-sha", &source,
			)
			if err != nil {
				t.Fatal(err)
			}
			if reused != source {
				t.Fatalf("conditional source = %#v, want %#v", reused, source)
			}
			requests := fake.Requests()
			if source.State == gh.CodeownersMissing {
				if len(requests) != before {
					t.Fatalf("cached missing source made %d requests, want 0", len(requests)-before)
				}
				return
			}
			if len(requests) != before+1 {
				t.Fatalf("same-ref conditional probe made %d requests, want 1", len(requests)-before)
			}
			path := "/repos/" + fixture.Owner + "/" + fixture.Repo +
				"/contents/" + source.Path
			if got := fake.NotModifiedCount("GET", path); got != 1 {
				t.Fatalf("conditional CODEOWNERS 304s = %d, want 1", got)
			}
			last := requests[len(requests)-1]
			if last.Path != path || last.IfNoneMatch != source.ETag {
				t.Fatalf("conditional request = %+v, want path %q ETag %q", last, path, source.ETag)
			}
		})
	}
}

func TestFindCodeownersNewCommitInvalidatesPathAbsence(t *testing.T) {
	t.Parallel()
	fixture := fakegithub.DefaultFixture()
	fixture.Contents = map[string]map[string]string{
		"old-sha": {"CODEOWNERS": "* @root"},
		"new-sha": {
			".github/CODEOWNERS": "* @github",
			"CODEOWNERS":         "* @root",
		},
	}
	fake := fakegithub.New(fixture, "secret")
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	client, err := gh.NewRESTClient(
		server.URL,
		budget.New(server.Client(), budget.Options{}),
		gh.StaticToken("fake-installation-codeowners-invalidation"),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldSource, err := client.FindCodeowners(
		t.Context(), budget.Interactive,
		fixture.Owner, fixture.Repo, "old-sha", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if oldSource.Path != "CODEOWNERS" {
		t.Fatalf("old source = %#v", oldSource)
	}
	before := len(fake.Requests())
	newSource, err := client.FindCodeowners(
		t.Context(), budget.Interactive,
		fixture.Owner, fixture.Repo, "new-sha", &oldSource,
	)
	if err != nil {
		t.Fatal(err)
	}
	if newSource.Ref != "new-sha" || newSource.Path != ".github/CODEOWNERS" ||
		newSource.Content != "* @github" {
		t.Fatalf("new source = %#v", newSource)
	}
	requests := fake.Requests()[before:]
	if len(requests) != 1 ||
		requests[0].Path != "/repos/"+fixture.Owner+"/"+fixture.Repo+
			"/contents/.github/CODEOWNERS" ||
		requests[0].RawQuery != "ref=new-sha" {
		t.Fatalf("new-commit precedence probes = %+v", requests)
	}
}
