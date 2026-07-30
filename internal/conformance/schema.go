package conformance

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
	defer document.Close()
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
