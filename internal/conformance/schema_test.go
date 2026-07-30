//nolint:gocritic // Schema fixtures intentionally remain immutable values within each test case.
package conformance_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jtacoma/uritemplates"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/ewhauser/ghsync/internal/budget"
	"github.com/ewhauser/ghsync/internal/conformance"
	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/gh"
)

const fakeSchemaWebhookSecret = "schema-test-secret"

type emittedWebhook struct {
	event       string
	guid        string
	signature   string
	contentType string
	body        []byte
}

func TestCorpusSchemasCompileOffline(t *testing.T) {
	t.Parallel()
	compiler := newCorpusSchemaCompiler(t)
	schemaFamilies := append([]string{"common"}, corpusEvents...)
	for _, event := range schemaFamilies { //nolint:paralleltest // cases share one offline schema compiler
		schemas, err := filepath.Glob(
			filepath.Join("corpus", event, "*.schema.json"),
		)
		if err != nil {
			t.Fatalf("list %s schemas: %v", event, err)
		}
		if len(schemas) == 0 {
			t.Fatalf("%s schema directory has no schemas", event)
		}
		for _, schemaPath := range schemas {
			t.Run(event+"/"+filepath.Base(schemaPath), func(t *testing.T) {
				if _, err := compiler.Compile(schemaPath); err != nil {
					t.Fatalf("compile schema offline: %v", err)
				}
			})
		}
	}
}

func TestFakeGitHubWebhookPayloadsValidateAgainstSchemas(t *testing.T) {
	t.Parallel()
	target, emitted := newWebhookCapture()
	defer target.Close()
	validator := conformance.NewWebhookSchemaValidator()

	for _, event := range corpusEvents { //nolint:paralleltest // cases share one ordered webhook capture channel
		actions := eventPayloadActions(t, event)
		for _, action := range actions {
			name := event + "/" + action
			if action == "" {
				name = event + "/event"
			}
			t.Run(name, func(t *testing.T) {
				fixture := fakeSchemaFixture(event, action)
				fake := fakegithub.New(fixture, fakeSchemaWebhookSecret)
				payload, err := buildFakeWebhookPayload(fake, event, action)
				if err != nil {
					t.Fatalf("build fake payload: %v", err)
				}
				guid, err := fake.EmitWebhook(
					context.Background(),
					target.URL,
					event,
					payload,
				)
				if err != nil {
					t.Fatalf("emit fake payload: %v", err)
				}
				received := receiveWebhook(t, emitted)
				if received.guid != guid {
					t.Fatalf(
						"emitted GUID = %q, want %q",
						received.guid,
						guid,
					)
				}
				validateEmittedWebhook(
					t,
					validator,
					received,
					event,
				)
				assertFakePayloadMatchesFixture(
					t,
					received.body,
					event,
					fixture,
				)
			})
		}
	}
}

func TestFakeGitHubWebhookEmissionPathsValidateAgainstSchemas(t *testing.T) {
	t.Parallel()
	t.Run("explicit GUID", func(t *testing.T) {
		t.Parallel()
		fake := fakegithub.New(
			fakegithub.DefaultFixture(),
			fakeSchemaWebhookSecret,
		)
		payload, err := fake.PushWebhookPayload(
			"refs/heads/main",
			"aaaa000",
			"bbbb000",
		)
		if err != nil {
			t.Fatal(err)
		}
		target, emitted := newWebhookCapture()
		defer target.Close()
		const guid = "explicit-schema-guid"
		returned, err := fake.EmitWebhookWithGUID(
			t.Context(),
			target.URL,
			"push",
			guid,
			payload,
		)
		if err != nil {
			t.Fatal(err)
		}
		if returned != guid {
			t.Fatalf("returned GUID = %q, want %q", returned, guid)
		}
		validateEmittedWebhook(
			t,
			conformance.NewWebhookSchemaValidator(),
			receiveWebhook(t, emitted),
			"push",
		)
	})

	t.Run("control endpoint", func(t *testing.T) {
		t.Parallel()
		fake := fakegithub.New(
			fakegithub.DefaultFixture(),
			fakeSchemaWebhookSecret,
		)
		api := httptest.NewServer(fake)
		defer api.Close()
		target, emitted := newWebhookCapture()
		defer target.Close()
		payload, err := fake.PullRequestWebhookPayload("synchronize", 4812)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{
			"target_url": target.URL,
			"event":      "pull_request",
			"guid":       "control-schema-guid",
			"payload":    payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			api.URL+fakegithub.ControlEmitPath,
			bytes.NewReader(body),
		)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := api.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = response.Body.Close()
		}()
		if response.StatusCode != http.StatusOK {
			message, _ := io.ReadAll(response.Body)
			t.Fatalf(
				"control emit status = %d: %s",
				response.StatusCode,
				message,
			)
		}
		validateEmittedWebhook(
			t,
			conformance.NewWebhookSchemaValidator(),
			receiveWebhook(t, emitted),
			"pull_request",
		)
	})

	t.Run("dropped delivery redelivery", func(t *testing.T) {
		t.Parallel()
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		fake := fakegithub.New(
			fakegithub.DefaultFixture(),
			fakeSchemaWebhookSecret,
			fakegithub.WithAppAuthentication(99, &privateKey.PublicKey),
		)
		api := httptest.NewServer(fake)
		defer api.Close()
		target, emitted := newWebhookCapture()
		defer target.Close()
		payload, err := fake.PushWebhookPayload(
			"refs/heads/main",
			"aaaa000",
			"bbbb000",
		)
		if err != nil {
			t.Fatal(err)
		}
		guid, err := fake.DropWebhook(target.URL, "push", payload)
		if err != nil {
			t.Fatal(err)
		}
		recorded := fake.Deliveries()
		if len(recorded) != 1 {
			t.Fatalf("recorded deliveries = %+v, want one", recorded)
		}
		privateKeyPEM := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		})
		appTokens, err := gh.NewAppTokens(99, privateKeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		deliveries, err := gh.NewDeliveriesClient(
			api.URL,
			budget.New(api.Client(), budget.Options{}),
			appTokens,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := deliveries.RedeliverAppHookDelivery(
			t.Context(),
			recorded[0].ID,
		); err != nil {
			t.Fatal(err)
		}
		received := receiveWebhook(t, emitted)
		if received.guid != guid {
			t.Fatalf(
				"redelivery GUID = %q, want %q",
				received.guid,
				guid,
			)
		}
		validateEmittedWebhook(
			t,
			conformance.NewWebhookSchemaValidator(),
			received,
			"push",
		)
		waitForRecordedRedelivery(t, fake, guid)
	})
}

func buildFakeWebhookPayload(
	fake *fakegithub.Server,
	event string,
	action string,
) (map[string]any, error) {
	switch event {
	case "pull_request":
		return fake.PullRequestWebhookPayload(action, 4812)
	case "pull_request_review":
		return fake.PullRequestReviewWebhookPayload(action, 4812)
	case "pull_request_review_comment":
		return fake.PullRequestReviewCommentWebhookPayload(action, 4812)
	case "pull_request_review_thread":
		return fake.PullRequestReviewThreadWebhookPayload(action, 4812)
	case "check_run":
		checkRunID := int64(99001)
		if action == "created" {
			checkRunID = 99003
		}
		return fake.CheckRunWebhookPayload(action, checkRunID)
	case "check_suite":
		return fake.CheckSuiteWebhookPayload(action, "8f31c2d")
	case "push":
		return fake.PushWebhookPayload(
			"refs/heads/main",
			"aaaa000",
			"bbbb000",
		)
	default:
		return nil, fmt.Errorf("no fake payload builder for event %q", event)
	}
}

func eventPayloadActions(t *testing.T, event string) []string {
	t.Helper()
	seen := make(map[string]struct{})
	for _, payload := range loadCorpusPayloads(t) {
		if payload.Event != event {
			continue
		}
		action := ""
		if event != "push" {
			var envelope struct {
				Action string `json:"action"`
			}
			if err := json.Unmarshal(payload.Body, &envelope); err != nil {
				t.Fatalf("decode action for %s: %v", payload.Filename, err)
			}
			if envelope.Action == "" {
				t.Fatalf("%s has no action", payload.Filename)
			}
			action = envelope.Action
		}
		seen[action] = struct{}{}
	}
	if len(seen) == 0 {
		t.Fatalf("%s payload directory has no examples", event)
	}
	actions := make([]string, 0, len(seen))
	for action := range seen {
		actions = append(actions, action)
	}
	sort.Strings(actions)
	return actions
}

func fakeSchemaFixture(event string, action string) fakegithub.Fixture {
	fixture := fakegithub.DefaultFixture()
	created := fixture.CheckRuns[0]
	created.ID = 99003
	created.NodeID = "CR_kwDOABCDEF99003"
	created.Status = "queued"
	created.Conclusion = ""
	created.CompletedAt = nil
	fixture.CheckRuns = append(fixture.CheckRuns, created)
	for index := range fixture.PullRequests {
		if fixture.PullRequests[index].Number != 4812 {
			continue
		}
		switch {
		case event == "pull_request" && action == "closed":
			fixture.PullRequests[index].State = "closed"
		case event == "pull_request" && action == "converted_to_draft":
			fixture.PullRequests[index].Draft = true
		}
	}
	for stackIndex := range fixture.Stacks {
		for pullIndex := range fixture.Stacks[stackIndex].PullRequests {
			pull := &fixture.Stacks[stackIndex].PullRequests[pullIndex]
			if pull.Number != 4812 {
				continue
			}
			if event == "pull_request" && action == "closed" {
				pull.State = "closed"
			}
			if event == "pull_request" && action == "converted_to_draft" {
				pull.Draft = true
			}
		}
	}
	return fixture
}

func newWebhookCapture() (*httptest.Server, <-chan emittedWebhook) {
	emitted := make(chan emittedWebhook, 1)
	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(w, "read payload", http.StatusBadRequest)
				return
			}
			emitted <- emittedWebhook{
				event:       request.Header.Get("X-GitHub-Event"),
				guid:        request.Header.Get("X-GitHub-Delivery"),
				signature:   request.Header.Get("X-Hub-Signature-256"),
				contentType: request.Header.Get("Content-Type"),
				body:        body,
			}
			w.WriteHeader(http.StatusAccepted)
		},
	))
	return target, emitted
}

func receiveWebhook(
	t *testing.T,
	emitted <-chan emittedWebhook,
) emittedWebhook {
	t.Helper()
	select {
	case received := <-emitted:
		return received
	case <-t.Context().Done():
		t.Fatal("timed out waiting for emitted webhook")
		return emittedWebhook{}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for emitted webhook")
		return emittedWebhook{}
	}
}

func waitForRecordedRedelivery(
	t *testing.T,
	fake *fakegithub.Server,
	guid string,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, delivery := range fake.Deliveries() {
			if delivery.GUID == guid && delivery.Redelivery {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("redelivery %q was not recorded", guid)
}

func validateEmittedWebhook(
	t *testing.T,
	validator *conformance.WebhookSchemaValidator,
	received emittedWebhook,
	event string,
) {
	t.Helper()
	if received.event != event {
		t.Fatalf("emitted event = %q, want %q", received.event, event)
	}
	if received.guid == "" {
		t.Fatal("emitted webhook has no delivery GUID")
	}
	if received.contentType != "application/json" {
		t.Fatalf(
			"emitted content type = %q, want application/json",
			received.contentType,
		)
	}
	if !gh.VerifySignature(
		[]byte(fakeSchemaWebhookSecret),
		received.body,
		received.signature,
	) {
		t.Fatalf("invalid emitted signature %q", received.signature)
	}
	if !json.Valid(received.body) {
		t.Fatalf("emitted body is invalid JSON: %q", received.body)
	}
	if err := validator.Validate(event, received.body); err != nil {
		t.Fatal(err)
	}
}

func assertFakePayloadMatchesFixture(
	t *testing.T,
	body []byte,
	event string,
	fixture fakegithub.Fixture,
) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode emitted payload: %v", err)
	}
	repository := requiredObject(t, payload, "repository")
	if requiredString(t, repository, "full_name") !=
		fixture.Repository.FullName ||
		jsonInt64(t, repository, "id") != fixture.Repository.ID {
		t.Fatalf(
			"emitted repository = %#v, want fixture %+v",
			repository,
			fixture.Repository,
		)
	}

	switch event {
	case "pull_request",
		"pull_request_review",
		"pull_request_review_comment",
		"pull_request_review_thread":
		var expected fakegithub.PullRequest
		for _, pull := range fixture.PullRequests {
			if pull.Number == 4812 {
				expected = pull
				break
			}
		}
		wire := requiredObject(t, payload, "pull_request")
		if jsonInt(t, wire, "number") != expected.Number ||
			jsonInt64(t, wire, "id") != expected.ID ||
			requiredString(t, wire, "title") != expected.Title ||
			requiredString(t, wire, "state") != expected.State ||
			requiredNestedString(t, wire, "head", "sha") != expected.Head.SHA ||
			requiredNestedString(t, wire, "base", "sha") != expected.Base.SHA {
			t.Fatalf("emitted pull request = %#v, want %+v", wire, expected)
		}
	case "check_run":
		wire := requiredObject(t, payload, "check_run")
		id := jsonInt64(t, wire, "id")
		var expected fakegithub.CheckRun
		for _, run := range fixture.CheckRuns {
			if run.ID == id {
				expected = run
				break
			}
		}
		if expected.ID == 0 ||
			requiredString(t, wire, "node_id") != expected.NodeID ||
			requiredString(t, wire, "head_sha") != expected.HeadSHA ||
			requiredString(t, wire, "name") != expected.Name ||
			requiredString(t, wire, "status") != expected.Status ||
			optionalString(wire, "conclusion") != expected.Conclusion ||
			requiredString(t, wire, "details_url") != expected.DetailsURL ||
			requiredNestedString(t, wire, "app", "slug") != expected.AppSlug {
			t.Fatalf("emitted check run = %#v, want %+v", wire, expected)
		}
	case "check_suite":
		wire := requiredObject(t, payload, "check_suite")
		if requiredString(t, wire, "head_sha") != "8f31c2d" {
			t.Fatalf("emitted check suite = %#v", wire)
		}
	case "push":
		if requiredString(t, payload, "ref") != "refs/heads/main" ||
			requiredString(t, payload, "before") != "aaaa000" ||
			requiredString(t, payload, "after") != "bbbb000" {
			t.Fatalf("emitted push = %#v", payload)
		}
	}
}

func requiredObject(
	t *testing.T,
	parent map[string]any,
	key string,
) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("field %q = %#v, want object", key, parent[key])
	}
	return value
}

func requiredString(
	t *testing.T,
	parent map[string]any,
	key string,
) string {
	t.Helper()
	value, ok := parent[key].(string)
	if !ok {
		t.Fatalf("field %q = %#v, want string", key, parent[key])
	}
	return value
}

func optionalString(parent map[string]any, key string) string {
	value, _ := parent[key].(string)
	return value
}

func requiredNestedString(
	t *testing.T,
	parent map[string]any,
	object string,
	key string,
) string {
	t.Helper()
	return requiredString(t, requiredObject(t, parent, object), key)
}

func jsonInt(t *testing.T, parent map[string]any, key string) int {
	t.Helper()
	return int(jsonInt64(t, parent, key))
}

func jsonInt64(t *testing.T, parent map[string]any, key string) int64 {
	t.Helper()
	number, ok := parent[key].(float64)
	if !ok || number != float64(int64(number)) {
		t.Fatalf("field %q = %#v, want integer", key, parent[key])
	}
	return int64(number)
}

func newCorpusSchemaCompiler(t *testing.T) *jsonschema.Compiler {
	t.Helper()
	root, err := filepath.Abs("corpus")
	if err != nil {
		t.Fatalf("resolve corpus root: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7)
	compiler.AssertFormat()
	// jsonschema's built-in checker rejects GitHub's valid RFC 6570 path
	// expansions, such as "{/sha}", so use a full URI-template parser.
	compiler.RegisterFormat(&jsonschema.Format{
		Name: "uri-template",
		Validate: func(value any) error {
			template, ok := value.(string)
			if !ok {
				return nil
			}
			_, err := uritemplates.Parse(template)
			return err
		},
	})
	compiler.UseLoader(corpusSchemaLoader{root: root})
	return compiler
}

type corpusSchemaLoader struct {
	root string
}

func (loader corpusSchemaLoader) Load(rawURL string) (any, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse schema URL: %w", err)
	}
	if parsed.Scheme != "file" {
		return nil, fmt.Errorf("offline schema loader rejected %q", rawURL)
	}
	filename := filepath.FromSlash(parsed.Path)
	if _, err := os.Stat(filename); err != nil {
		commonMarker := string(filepath.Separator) +
			"common" + string(filepath.Separator)
		if index := strings.LastIndex(filename, commonMarker); index >= 0 {
			filename = filepath.Join(
				loader.root,
				"common",
				filepath.Base(filename),
			)
		}
	}
	relative, err := filepath.Rel(loader.root, filename)
	if err != nil ||
		relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("schema URL escapes corpus: %q", rawURL)
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open schema %s: %w", relative, err)
	}
	defer func() {
		_ = file.Close()
	}()
	document, err := jsonschema.UnmarshalJSON(file)
	if err != nil {
		return nil, fmt.Errorf("decode schema %s: %w", relative, err)
	}
	return document, nil
}
