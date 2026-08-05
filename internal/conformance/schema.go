package conformance

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/jtacoma/uritemplates"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed corpus/*/*.schema.json
var schemaFiles embed.FS

type WebhookSchemaValidator struct {
	mu       sync.Mutex
	compiler *jsonschema.Compiler
	compiled map[string]*jsonschema.Schema
}

func NewWebhookSchemaValidator() *WebhookSchemaValidator {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft7)
	compiler.AssertFormat()
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
	compiler.UseLoader(embeddedSchemaLoader{})
	return &WebhookSchemaValidator{
		compiler: compiler,
		compiled: make(map[string]*jsonschema.Schema),
	}
}

func (v *WebhookSchemaValidator) Validate(
	event string,
	body []byte,
) error {
	schemaName := "event"
	if event != "push" {
		var envelope struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			return fmt.Errorf("decode %s action: %w", event, err)
		}
		if envelope.Action == "" {
			return fmt.Errorf("%s payload has no action", event)
		}
		schemaName = envelope.Action
	}
	location := "file:///corpus/" + event + "/" +
		schemaName + ".schema.json"
	v.mu.Lock()
	defer v.mu.Unlock()
	schema := v.compiled[location]
	if schema == nil {
		compiled, err := v.compiler.Compile(location)
		if err != nil {
			return fmt.Errorf(
				"compile %s/%s schema offline: %w",
				event,
				schemaName,
				err,
			)
		}
		schema = compiled
		v.compiled[location] = schema
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("decode %s payload for validation: %w", event, err)
	}
	if err := validateAndStripStackPreview(value); err != nil {
		return fmt.Errorf("validate pull_request stack preview: %w", err)
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf(
			"validate %s/%s payload: %w",
			event,
			schemaName,
			err,
		)
	}
	return nil
}

// validateAndStripStackPreview covers the private-preview field that is
// present on GitHub's production pull_request payload but intentionally absent
// from the vendored public webhook schemas. The public remainder still passes
// through the unmodified upstream schema.
func validateAndStripStackPreview(value any) error {
	payload, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	pull, ok := payload["pull_request"].(map[string]any)
	if !ok {
		return nil
	}
	rawStack, exists := pull["stack"]
	if !exists {
		return nil
	}
	if rawStack == nil {
		delete(pull, "stack")
		return nil
	}
	stack, ok := rawStack.(map[string]any)
	if !ok {
		return fmt.Errorf("stack must be an object or null")
	}
	if err := requireExactFields(
		stack,
		"stack",
		"id", "number", "size", "position", "base",
	); err != nil {
		return err
	}
	for _, field := range []string{"id", "number", "size", "position"} {
		if !positiveJSONInteger(stack[field]) {
			return fmt.Errorf("stack.%s must be a positive integer", field)
		}
	}
	// GitHub may retain a historical position after the current stack shrinks,
	// including on an open PR, so position and size are independently
	// constrained positive integers.
	base, ok := stack["base"].(map[string]any)
	if !ok {
		return fmt.Errorf("stack.base must be an object")
	}
	if err := requireExactFields(base, "stack.base", "ref", "sha"); err != nil {
		return err
	}
	ref, ok := base["ref"].(string)
	if !ok || ref == "" {
		return fmt.Errorf("stack.base.ref must be a non-empty string")
	}
	if base["sha"] != nil {
		sha, ok := base["sha"].(string)
		if !ok || sha == "" {
			return fmt.Errorf("stack.base.sha must be a non-empty string or null")
		}
	}
	delete(pull, "stack")
	return nil
}

func requireExactFields(
	object map[string]any,
	objectPath string,
	fields ...string,
) error {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
		if _, exists := object[field]; !exists {
			return fmt.Errorf("%s.%s is required", objectPath, field)
		}
	}
	for field := range object {
		if _, exists := allowed[field]; !exists {
			return fmt.Errorf("%s.%s is not supported", objectPath, field)
		}
	}
	return nil
}

func positiveJSONInteger(value any) bool {
	integer, ok := jsonInteger(value)
	return ok && integer > 0
}

func jsonInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case json.Number:
		integer, err := number.Int64()
		return integer, err == nil
	case float64:
		if number != math.Trunc(number) ||
			number < math.MinInt64 || number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	case int:
		return int64(number), true
	case int64:
		return number, true
	case uint64:
		if number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}

type embeddedSchemaLoader struct{}

func (embeddedSchemaLoader) Load(rawURL string) (any, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse schema URL: %w", err)
	}
	if parsed.Scheme != "file" {
		return nil, fmt.Errorf("offline schema loader rejected %q", rawURL)
	}
	filename := path.Clean(parsed.Path)
	const root = "/corpus/"
	if !strings.HasPrefix(filename, root) {
		return nil, fmt.Errorf("schema URL escapes corpus: %q", rawURL)
	}
	if marker := "/common/"; strings.Contains(filename, marker) {
		filename = root + "common/" + path.Base(filename)
	}
	document, err := schemaFiles.Open(strings.TrimPrefix(filename, "/"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf(
				"schema %s is not vendored",
				strings.TrimPrefix(filename, root),
			)
		}
		return nil, err
	}
	defer func() {
		_ = document.Close()
	}()
	decoded, err := jsonschema.UnmarshalJSON(document)
	if err != nil {
		return nil, fmt.Errorf(
			"decode schema %s: %w",
			strings.TrimPrefix(filename, root),
			err,
		)
	}
	return decoded, nil
}
