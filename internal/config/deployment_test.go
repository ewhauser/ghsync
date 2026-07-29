package config

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

type documentedEnvironment struct {
	defaultValue string
	line         int
}

type defaultLabel string

const (
	defaultNone    defaultLabel = "none"
	defaultBuiltIn defaultLabel = "built in"
)

func TestDeploymentReferenceMatchesConfigEnvironment(t *testing.T) {
	configured := configEnvironmentVariables(t)
	cleared := clearedConfigEnvironmentVariables(t)
	documented := deploymentEnvironmentVariables(t)
	defaults := environmentDefaults()

	for key := range configured {
		if _, ok := cleared[key]; !ok {
			t.Errorf("clearConfigEnv omits config environment variable %s", key)
		}
		if _, ok := documented[key]; !ok {
			t.Errorf("ops/DEPLOYMENT.md omits config environment variable %s", key)
		}
		if _, ok := defaults[key]; !ok {
			t.Errorf("deployment default test omits config environment variable %s", key)
		}
	}
	for key := range cleared {
		if _, ok := configured[key]; !ok {
			t.Errorf("clearConfigEnv contains stale environment variable %s", key)
		}
	}
	for key, item := range documented {
		if _, ok := configured[key]; !ok {
			t.Errorf(
				"ops/DEPLOYMENT.md:%d documents unknown environment variable %s",
				item.line,
				key,
			)
		}
	}
	for key := range defaults {
		if _, ok := configured[key]; !ok {
			t.Errorf("deployment default test contains stale environment variable %s", key)
		}
	}

	for key, want := range defaults {
		item, ok := documented[key]
		if !ok {
			continue
		}
		if err := compareDocumentedDefault(item.defaultValue, want); err != nil {
			t.Errorf(
				"ops/DEPLOYMENT.md:%d %s default %q: %v",
				item.line,
				key,
				item.defaultValue,
				err,
			)
		}
	}
	if len(configured) < 40 {
		t.Fatalf("discovered only %d environment variables", len(configured))
	}
}

func configEnvironmentVariables(t *testing.T) map[string]struct{} {
	t.Helper()
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
		if err != nil {
			t.Fatal(err)
		}
		if envName.MatchString(value) {
			keys[value] = struct{}{}
		}
		return true
	})
	return keys
}

func clearedConfigEnvironmentVariables(t *testing.T) map[string]struct{} {
	t.Helper()
	source, err := parser.ParseFile(
		token.NewFileSet(),
		"config_test.go",
		nil,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	envName := regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)
	keys := make(map[string]struct{})
	for _, declaration := range source.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "clearConfigEnv" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if envName.MatchString(value) {
				keys[value] = struct{}{}
			}
			return true
		})
		return keys
	}
	t.Fatal("config_test.go has no clearConfigEnv")
	return nil
}

func deploymentEnvironmentVariables(
	t *testing.T,
) map[string]documentedEnvironment {
	t.Helper()
	document, err := os.ReadFile("../../ops/DEPLOYMENT.md")
	if err != nil {
		t.Fatal(err)
	}
	row := regexp.MustCompile(
		"^\\|\\s*`([A-Z][A-Z0-9_]*)`\\s*\\|\\s*([^|]+?)\\s*\\|",
	)
	result := make(map[string]documentedEnvironment)
	for index, line := range strings.Split(string(document), "\n") {
		match := row.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		if prior, exists := result[match[1]]; exists {
			t.Errorf(
				"ops/DEPLOYMENT.md:%d duplicates %s from line %d",
				index+1,
				match[1],
				prior.line,
			)
		}
		result[match[1]] = documentedEnvironment{
			defaultValue: strings.TrimSpace(match[2]),
			line:         index + 1,
		}
	}
	return result
}

func environmentDefaults() map[string]any {
	return map[string]any{
		"DATABASE_URL":                        defaultNone,
		"HTTP_ADDR":                           defaultHTTPAddr,
		"GITHUB_APP_ID":                       int64(0),
		"GITHUB_INSTALLATION_ID":              int64(0),
		"GITHUB_ORG_ID":                       int64(0),
		"GITHUB_TOKEN":                        defaultNone,
		"GITHUB_PRIVATE_KEY_PATH":             defaultNone,
		"GITHUB_WEBHOOK_SECRET":               defaultNone,
		"GITHUB_BASE_URL":                     defaultGitHubBaseURL,
		"WEBHOOK_MAX_BODY_BYTES":              defaultWebhookMaxBodyBytes,
		"DISPATCH_BATCH_SIZE":                 defaultDispatchBatchSize,
		"DISPATCH_MAX_ATTEMPTS":               defaultDispatchMaxAttempts,
		"DISPATCH_DEBOUNCE":                   defaultDispatchDebounce,
		"DISPATCH_POLL_INTERVAL":              defaultDispatchPoll,
		"DISPATCH_RULES_FILE":                 defaultBuiltIn,
		"FETCH_BATCH_WINDOW":                  defaultFetchBatchWindow,
		"BACKFILL_PAGE_SIZE":                  defaultBackfillPageSize,
		"BUDGET_SWEEP_FLOOR":                  defaultBudgetSweepFloor,
		"BUDGET_EVENT_FLOOR":                  defaultBudgetEventFloor,
		"BUDGET_MAX_CONCURRENT":               defaultBudgetMaxConcurrent,
		"BUDGET_REST_LIMIT":                   defaultBudgetRESTLimit,
		"BUDGET_GRAPHQL_LIMIT":                defaultBudgetGraphQLLimit,
		"BUDGET_SECONDARY_FALLBACK":           defaultBudgetSecondaryFallback,
		"SWEEP_OPEN_STACK_MAX_STALENESS":      defaultSweepOpenStackStaleness,
		"SWEEP_OPEN_PR_MAX_STALENESS":         defaultSweepOpenPRStaleness,
		"SWEEP_REPO_RULES_MAX_STALENESS":      defaultSweepRepoRulesStaleness,
		"SWEEP_CLOSED_MAX_STALENESS":          defaultSweepClosedStaleness,
		"SWEEP_REPOSITORY_LIST_PERIOD":        defaultSweepRepositoryPeriod,
		"SWEEP_PAGE_SIZE":                     defaultSweepPageSize,
		"GAP_HEAL_PERIOD":                     defaultGapHealPeriod,
		"GAP_COMPARISON_WINDOW":               defaultGapWindow,
		"GAP_PAGE_SIZE":                       defaultGapPageSize,
		"GAP_MAX_PAGES":                       defaultGapMaxPages,
		"DRIFT_PERIOD":                        defaultDriftPeriod,
		"DRIFT_SAMPLE_SIZE":                   defaultDriftSampleSize,
		"DRIFT_PAGE_SIZE":                     defaultDriftPageSize,
		"DRIFT_RESOLVED_RETENTION":            defaultDriftResolvedRetention,
		"RETENTION_PERIOD":                    defaultRetentionPeriod,
		"RETENTION_AGE":                       defaultRetentionAge,
		"RETENTION_BATCH_SIZE":                defaultRetentionBatchSize,
		"STREAM_WATERMARK_REFRESH":            defaultWatermarkRefresh,
		"STREAM_WATERMARK_LEASE_TTL":          defaultWatermarkLeaseTTL,
		"STREAM_WATERMARK_FENCE_LOCK_TIMEOUT": defaultWatermarkFenceTimeout,
		"STREAM_RETENTION_PERIOD":             defaultStreamRetentionPeriod,
		"STREAM_RETENTION_AGE":                defaultStreamRetentionAge,
		"STREAM_RETENTION_BATCH_SIZE":         defaultStreamRetentionBatch,
		"DERIVER_POLL_INTERVAL":               defaultDeriverPoll,
		"DERIVER_DIRTY_CAP":                   defaultDeriverDirtyCap,
	}
}

func compareDocumentedDefault(raw string, want any) error {
	value := strings.TrimSpace(raw)
	if strings.HasPrefix(value, "`") && strings.HasSuffix(value, "`") {
		value = strings.TrimSuffix(strings.TrimPrefix(value, "`"), "`")
	}
	switch expected := want.(type) {
	case defaultLabel:
		if value != string(expected) {
			return fmt.Errorf("want %q", expected)
		}
	case string:
		if value != expected {
			return fmt.Errorf("want %q", expected)
		}
	case int:
		got, err := strconv.Atoi(value)
		if err != nil || got != expected {
			return fmt.Errorf("want %d", expected)
		}
	case int64:
		got, err := strconv.ParseInt(value, 10, 64)
		if err != nil || got != expected {
			return fmt.Errorf("want %d", expected)
		}
	case float64:
		got, err := strconv.ParseFloat(value, 64)
		if err != nil || got != expected {
			return fmt.Errorf("want %g", expected)
		}
	case time.Duration:
		got, err := time.ParseDuration(value)
		if err != nil || got != expected {
			return fmt.Errorf("want %s", expected)
		}
	default:
		return fmt.Errorf("unsupported expected type %T", want)
	}
	return nil
}
