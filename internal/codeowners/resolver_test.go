package codeowners

import (
	"reflect"
	"testing"
)

// githubDocumentedExample is the sample CODEOWNERS file from GitHub's
// "About code owners" documentation. Keep the rules and ordering intact: the
// repeated /apps block is intentional and exercises whole-rule precedence.
const githubDocumentedExample = "# This is a comment.\n" +
	"# Each line is a file pattern followed by one or more owners.\n" +
	"\n" +
	"# These owners will be the default owners for everything in\n" +
	"# the repo. Unless a later match takes precedence,\n" +
	"# @global-owner1 and @global-owner2 will be requested for\n" +
	"# review when someone opens a pull request.\n" +
	"*       @global-owner1 @global-owner2\n" +
	"\n" +
	"# Order is important; the last matching pattern takes the most\n" +
	"# precedence. When someone opens a pull request that only\n" +
	"# modifies JS files, only @js-owner and not the global\n" +
	"# owner(s) will be requested for a review.\n" +
	"*.js    @js-owner #This is an inline comment.\n" +
	"\n" +
	"# You can also use email addresses if you prefer. They'll be\n" +
	"# used to look up users just like we do for commit author\n" +
	"# emails.\n" +
	"*.go docs@example.com\n" +
	"\n" +
	"# Teams can be specified as code owners as well. Teams should\n" +
	"# be identified in the format @org/team-name. Teams must have\n" +
	"# explicit write access to the repository. In this example,\n" +
	"# the octocats team in the octo-org organization owns all .txt files.\n" +
	"*.txt @octo-org/octocats\n" +
	"\n" +
	"# In this example, @doctocat owns any files in the build/logs\n" +
	"# directory at the root of the repository and any of its\n" +
	"# subdirectories.\n" +
	"/build/logs/ @doctocat\n" +
	"\n" +
	"# The `docs/*` pattern will match files like\n" +
	"# `docs/getting-started.md` but not further nested files like\n" +
	"# `docs/build-app/troubleshooting.md`.\n" +
	"docs/* docs@example.com\n" +
	"\n" +
	"# In this example, @octocat owns any file in an apps directory\n" +
	"# anywhere in your repository.\n" +
	"apps/ @octocat\n" +
	"\n" +
	"# In this example, @doctocat owns any file in the `/docs`\n" +
	"# directory in the root of your repository and any of its\n" +
	"# subdirectories.\n" +
	"/docs/ @doctocat\n" +
	"\n" +
	"# In this example, any change inside the `/scripts` directory\n" +
	"# will require approval from @doctocat or @octocat.\n" +
	"/scripts/ @doctocat @octocat\n" +
	"\n" +
	"# In this example, @octocat owns any file in a `/logs` directory such as\n" +
	"# `/build/logs`, `/scripts/logs`, and `/deeply/nested/logs`. Any changes\n" +
	"# in a `/logs` directory will require approval from @octocat.\n" +
	"**/logs @octocat\n" +
	"\n" +
	"# In this example, @octocat owns any file in the `/apps`\n" +
	"# directory in the root of your repository except for the `/apps/github`\n" +
	"# subdirectory, as its owners are left empty. Without an owner, changes\n" +
	"# to `apps/github` can be made with the approval of any user who has\n" +
	"# write access to the repository.\n" +
	"/apps/ @octocat\n" +
	"/apps/github\n" +
	"\n" +
	"# In this example, @octocat owns any file in the `/apps`\n" +
	"# directory in the root of your repository except for the `/apps/github`\n" +
	"# subdirectory, as this subdirectory has its own owner @doctocat\n" +
	"/apps/ @octocat\n" +
	"/apps/github @doctocat\n"

func TestGitHubDocumentedCODEOWNERSExample(t *testing.T) {
	t.Parallel()
	rules := Parse(githubDocumentedExample)
	tests := []struct {
		path    string
		pattern string
		owners  []string
	}{
		{"README.md", "*", []string{"@global-owner1", "@global-owner2"}},
		{"web/app.js", "*.js", []string{"@js-owner"}},
		{"pkg/tool.go", "*.go", []string{"docs@example.com"}},
		{"notes.txt", "*.txt", []string{"@octo-org/octocats"}},
		{"build/logs/output.log", "**/logs", []string{"@octocat"}},
		{"docs/getting-started.md", "/docs/", []string{"@doctocat"}},
		{"deep/apps/main.rb", "apps/", []string{"@octocat"}},
		{"scripts/release.sh", "/scripts/", []string{"@doctocat", "@octocat"}},
		{"deeply/nested/logs/output.log", "**/logs", []string{"@octocat"}},
		{"apps/github/api.go", "/apps/github", []string{"@doctocat"}},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			match, ok := Resolve(rules, test.path)
			if !ok {
				t.Fatal("path did not match")
			}
			owners := make([]string, 0, len(match.Owners))
			for _, owner := range match.Owners {
				owners = append(owners, owner.Token)
			}
			if match.Pattern != test.pattern ||
				!reflect.DeepEqual(owners, test.owners) {
				t.Fatalf(
					"match = %q %v, want %q %v",
					match.Pattern, owners, test.pattern, test.owners,
				)
			}
		})
	}
}

func TestResolveCODEOWNERSLastMatchAndWildcards(t *testing.T) {
	t.Parallel()
	rules := Parse(`# global
* @global
*.go @go-owner
/cmd/** @cli
internal/* @direct
internal/**/generated/*.go @generator
docs/ @docs
docs/My\ File/** @spaces
`)
	tests := []struct {
		path    string
		pattern string
		owners  []string
	}{
		{"README.md", "*", []string{"@global"}},
		{"pkg/cache/store.go", "*.go", []string{"@go-owner"}},
		{"cmd/ghsyncd/main.go", "/cmd/**", []string{"@cli"}},
		{"internal/top.go", "internal/*", []string{"@direct"}},
		{"internal/deep/top.go", "*.go", []string{"@go-owner"}},
		{"internal/a/generated/table.go", "internal/**/generated/*.go", []string{"@generator"}},
		{"internal/generated/table.go", "internal/**/generated/*.go", []string{"@generator"}},
		{"docs/nested/guide.md", "docs/", []string{"@docs"}},
		{"docs/My File/api/index.md", "docs/My\\ File/**", []string{"@spaces"}},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			match, ok := Resolve(rules, test.path)
			if !ok {
				t.Fatal("path did not match")
			}
			owners := make([]string, 0, len(match.Owners))
			for _, owner := range match.Owners {
				owners = append(owners, owner.Token)
			}
			if match.Pattern != test.pattern || !reflect.DeepEqual(owners, test.owners) {
				t.Fatalf("match = %q %v, want %q %v", match.Pattern, owners, test.pattern, test.owners)
			}
		})
	}
}

func TestParseCODEOWNERSCommentsUnsupportedNegationAndOwnerKinds(t *testing.T) {
	t.Parallel()
	rules := Parse(`# comment
\#not-a-pattern @ignored
!secret/** @negated
lib/[ab].go @range
*.js @octocat @acme/frontend bad-token docs@example.com # inline comment
*.txt @later
*.txt @final
`)
	if len(rules) != 3 {
		t.Fatalf("rules = %#v, want three valid rules", rules)
	}
	match, ok := Resolve(rules, "web/app.js")
	if !ok || match.Line != 5 {
		t.Fatalf("JavaScript match = %#v, %v", match, ok)
	}
	want := []Owner{
		{Token: "@octocat", Type: OwnerUser, Name: "octocat"},
		{Token: "@acme/frontend", Type: OwnerTeam, Name: "acme/frontend"},
		{Token: "bad-token", Type: OwnerMalformed},
		{Token: "docs@example.com", Type: OwnerEmail, Name: "docs@example.com"},
	}
	if !reflect.DeepEqual(match.Owners, want) {
		t.Fatalf("owners = %#v, want %#v", match.Owners, want)
	}
	match, ok = Resolve(rules, "notes.txt")
	if !ok || match.Line != 7 || match.Owners[0].Token != "@final" {
		t.Fatalf("last-match result = %#v, %v", match, ok)
	}
}

func TestCODEOWNERSStarDoesNotCrossDirectory(t *testing.T) {
	t.Parallel()
	rules := Parse("docs/* @direct\ndocs/** @recursive\n")
	match, ok := Resolve(rules[:1], "docs/api/index.md")
	if ok {
		t.Fatalf("single star unexpectedly matched %#v", match)
	}
	match, ok = Resolve(rules, "docs/api/index.md")
	if !ok || match.Owners[0].Token != "@recursive" {
		t.Fatalf("double star match = %#v, %v", match, ok)
	}
}

func TestCODEOWNERSSharpEdges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		source  string
		path    string
		matched bool
		pattern string
		owners  []string
	}{
		{
			name:   "slashless directory matches anywhere",
			source: "apps/ @anywhere\n", path: "nested/apps/file.go",
			matched: true, pattern: "apps/", owners: []string{"@anywhere"},
		},
		{
			name:   "leading slash anchors to root",
			source: "/apps/ @root\n", path: "nested/apps/file.go",
		},
		{
			name:   "single star does not cross slash",
			source: "docs/* @direct\n", path: "docs/api/index.md",
		},
		{
			name:   "double star crosses slash",
			source: "docs/** @recursive\n", path: "docs/api/index.md",
			matched: true, pattern: "docs/**", owners: []string{"@recursive"},
		},
		{
			name:   "literal directory without slash owns descendants",
			source: "/apps/github @github\n", path: "apps/github/api/main.go",
			matched: true, pattern: "/apps/github", owners: []string{"@github"},
		},
		{
			name:   "later ownerless rule clears ownership",
			source: "/apps/ @apps\n/apps/github\n", path: "apps/github/api.go",
			matched: true, pattern: "/apps/github", owners: []string{},
		},
		{
			name:   "paths are case sensitive",
			source: "Docs/ @docs\n", path: "docs/guide.md",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			match, ok := Resolve(Parse(test.source), test.path)
			if ok != test.matched {
				t.Fatalf("match = %#v, %v", match, ok)
			}
			if !ok {
				return
			}
			owners := make([]string, 0, len(match.Owners))
			for _, owner := range match.Owners {
				owners = append(owners, owner.Token)
			}
			if match.Pattern != test.pattern ||
				!reflect.DeepEqual(owners, test.owners) {
				t.Fatalf(
					"match = %q %v, want %q %v",
					match.Pattern, owners, test.pattern, test.owners,
				)
			}
		})
	}
}

func TestCODEOWNERSCRLFEmailAndOwnerTokenCase(t *testing.T) {
	t.Parallel()
	rules := Parse("*.go Docs@Example.com @OctoCat\r\n")
	match, ok := Resolve(rules, "pkg/main.go")
	if !ok {
		t.Fatal("CRLF rule did not match")
	}
	want := []Owner{
		{Token: "Docs@Example.com", Type: OwnerEmail, Name: "Docs@Example.com"},
		{Token: "@OctoCat", Type: OwnerUser, Name: "OctoCat"},
	}
	if !reflect.DeepEqual(match.Owners, want) {
		t.Fatalf("owners = %#v, want %#v", match.Owners, want)
	}
}
