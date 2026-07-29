package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestDeploymentReferenceContainsEveryEnvironmentVariable(t *testing.T) {
	source, err := parser.ParseFile(
		token.NewFileSet(),
		"config.go",
		nil,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	envName := regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)
	keys := make(map[string]struct{})
	ast.Inspect(source, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err == nil && envName.MatchString(value) {
			keys[value] = struct{}{}
		}
		return true
	})
	document, err := os.ReadFile("../../ops/DEPLOYMENT.md")
	if err != nil {
		t.Fatal(err)
	}
	for key := range keys {
		if !strings.Contains(string(document), "`"+key+"`") {
			t.Errorf("ops/DEPLOYMENT.md omits %s", key)
		}
	}
	if len(keys) < 30 {
		t.Fatalf("discovered only %d environment variables", len(keys))
	}
}
