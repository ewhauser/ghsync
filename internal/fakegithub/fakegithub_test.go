package fakegithub

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/acme/frontier/internal/gh"
)

func TestServesStacksWithRateHeaders(t *testing.T) {
	srv := httptest.NewServer(New(DefaultFixture(), "secret"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/repos/acme/monolith/stacks")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("x-ratelimit-remaining") == "" {
		t.Fatal("missing rate-limit headers")
	}
	var stacks []Stack
	if err := json.NewDecoder(resp.Body).Decode(&stacks); err != nil {
		t.Fatal(err)
	}
	if len(stacks) != 1 || stacks[0].Number != 142 || len(stacks[0].PullRequests) != 5 {
		t.Fatalf("unexpected stacks payload: %+v", stacks)
	}
}

func TestUnknownRepoIs404(t *testing.T) {
	srv := httptest.NewServer(New(DefaultFixture(), "secret"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/repos/acme/other/stacks")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestEmitWebhookSignsAndDelivers(t *testing.T) {
	fake := New(DefaultFixture(), "secret")

	type received struct {
		event, guid, sig string
		body             []byte
	}
	got := make(chan received, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- received{
			event: r.Header.Get("X-GitHub-Event"),
			guid:  r.Header.Get("X-GitHub-Delivery"),
			sig:   r.Header.Get("X-Hub-Signature-256"),
			body:  body,
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer target.Close()

	payload := map[string]any{"action": "stacked", "number": 4815}
	guid, err := fake.EmitWebhook(context.Background(), target.URL, "pull_request", payload)
	if err != nil {
		t.Fatal(err)
	}

	rec := <-got
	if rec.event != "pull_request" || rec.guid != guid {
		t.Fatalf("delivery headers wrong: %+v", rec)
	}
	if !gh.VerifySignature([]byte("secret"), rec.body, rec.sig) {
		t.Fatal("signature does not verify")
	}
	if gh.VerifySignature([]byte("wrong"), rec.body, rec.sig) {
		t.Fatal("signature verified with wrong secret")
	}
}

func TestEmitWebhookFailsOnNon2xx(t *testing.T) {
	fake := New(DefaultFixture(), "secret")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	if _, err := fake.EmitWebhook(context.Background(), target.URL, "push", map[string]any{}); err == nil {
		t.Fatal("expected error on 503 target")
	}
}
