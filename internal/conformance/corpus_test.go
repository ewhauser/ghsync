package conformance_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var corpusEvents = []string{
	"pull_request",
	"pull_request_review",
	"pull_request_review_comment",
	"pull_request_review_thread",
	"check_run",
	"check_suite",
	"push",
}

var corpusFileCounts = map[string]struct {
	payloads int
	schemas  int
}{
	"pull_request":                {payloads: 28, schemas: 21},
	"pull_request_review":         {payloads: 3, schemas: 3},
	"pull_request_review_comment": {payloads: 4, schemas: 3},
	"pull_request_review_thread":  {payloads: 2, schemas: 2},
	"check_run":                   {payloads: 8, schemas: 4},
	"check_suite":                 {payloads: 8, schemas: 3},
	"push":                        {payloads: 6, schemas: 1},
}

type corpusPayload struct {
	Event    string
	Filename string
	Body     []byte
}

var releaseTagPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

func TestCorpusLayout(t *testing.T) {
	version, err := os.ReadFile(filepath.Join("corpus", "VERSION"))
	if err != nil {
		t.Fatalf("read corpus version: %v", err)
	}
	releaseTag := strings.TrimSuffix(string(version), "\n")
	if string(version) != releaseTag+"\n" ||
		!releaseTagPattern.MatchString(releaseTag) {
		t.Fatalf("corpus version = %q, want a release tag", version)
	}
	license, err := os.ReadFile(filepath.Join("corpus", "LICENSE"))
	if err != nil {
		t.Fatalf("read corpus license: %v", err)
	}
	if !strings.Contains(string(license), "The MIT License") {
		t.Fatal("corpus license does not contain the upstream MIT notice")
	}

	entries, err := os.ReadDir("corpus")
	if err != nil {
		t.Fatalf("read corpus root: %v", err)
	}
	wantEntries := map[string]bool{
		"LICENSE": true,
		"VERSION": true,
	}
	for _, event := range corpusEvents {
		wantEntries[event] = true
	}
	for _, entry := range entries {
		if !wantEntries[entry.Name()] {
			t.Fatalf("unexpected corpus entry %q", entry.Name())
		}
		delete(wantEntries, entry.Name())
	}
	if len(wantEntries) != 0 {
		t.Fatalf("missing corpus entries: %v", wantEntries)
	}

	for _, event := range corpusEvents {
		t.Run(event, func(t *testing.T) {
			files, err := os.ReadDir(filepath.Join("corpus", event))
			if err != nil {
				t.Fatalf("read event corpus: %v", err)
			}
			var payloadCount, schemaCount int
			for _, file := range files {
				if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
					t.Fatalf("unexpected event corpus entry %q", file.Name())
				}
				body, err := os.ReadFile(
					filepath.Join("corpus", event, file.Name()),
				)
				if err != nil {
					t.Fatalf("read %s: %v", file.Name(), err)
				}
				if !json.Valid(body) {
					t.Fatalf("%s is not valid JSON", file.Name())
				}
				if strings.HasSuffix(file.Name(), ".schema.json") {
					schemaCount++
				} else {
					payloadCount++
				}
			}
			want := corpusFileCounts[event]
			if payloadCount != want.payloads || schemaCount != want.schemas {
				t.Fatalf(
					"payload files = %d, schema files = %d; want %d/%d",
					payloadCount,
					schemaCount,
					want.payloads,
					want.schemas,
				)
			}
		})
	}
}

func loadCorpusPayloads(t *testing.T) []corpusPayload {
	t.Helper()
	var payloads []corpusPayload
	for _, event := range corpusEvents {
		entries, err := os.ReadDir(filepath.Join("corpus", event))
		if err != nil {
			t.Fatalf("read %s corpus: %v", event, err)
		}
		for _, entry := range entries {
			if entry.IsDir() ||
				filepath.Ext(entry.Name()) != ".json" ||
				strings.HasSuffix(entry.Name(), ".schema.json") {
				continue
			}
			body, err := os.ReadFile(
				filepath.Join("corpus", event, entry.Name()),
			)
			if err != nil {
				t.Fatalf("read %s/%s: %v", event, entry.Name(), err)
			}
			payloads = append(payloads, corpusPayload{
				Event:    event,
				Filename: event + "/" + entry.Name(),
				Body:     body,
			})
		}
	}
	sort.Slice(payloads, func(i, j int) bool {
		return payloads[i].Filename < payloads[j].Filename
	})
	if len(payloads) == 0 {
		t.Fatal("webhook corpus has no payloads")
	}
	return payloads
}
