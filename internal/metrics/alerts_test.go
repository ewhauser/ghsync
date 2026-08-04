package metrics

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAlertRulesReferenceConstraintsAndLockedThresholds(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile("../../ops/alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Groups []struct {
			Rules []struct {
				Alert       string            `yaml:"alert"`
				Expr        string            `yaml:"expr"`
				For         string            `yaml:"for"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	count := 0
	expressions := make(map[string]string)
	durations := make(map[string]string)
	for _, group := range document.Groups {
		for _, rule := range group.Rules {
			count++
			if rule.Alert == "" || rule.Expr == "" {
				t.Fatalf("incomplete alert rule: %#v", rule)
			}
			constraint := rule.Annotations["constraint"]
			if !strings.HasPrefix(constraint, "C-") {
				t.Fatalf("%s constraint = %q", rule.Alert, constraint)
			}
			expressions[rule.Alert] = rule.Expr
			durations[rule.Alert] = rule.For
		}
	}
	if count < 15 {
		t.Fatalf("alert count = %d, want broad C-O4 coverage", count)
	}
	for alert, threshold := range map[string]string{
		"GhsyncSweepConditionalHitRateLow": "0.80",
		"GhsyncEventCacheP95SLOBreach":     "> 20",
		"GhsyncEventCacheP99SLOBreach":     "> 60",
		"GhsyncDriftOpen":                  "> 0",
		"GhsyncResyncStorm":                "> 3",
	} {
		if !strings.Contains(expressions[alert], threshold) {
			t.Fatalf("%s expression = %q, missing %q", alert, expressions[alert], threshold)
		}
	}
	if _, exists := expressions["GhsyncBudgetFloorBreached"]; exists {
		t.Fatal("lower-priority budget floor is still treated as an invariant")
	}
	for _, alert := range []string{
		"GhsyncDriftOpen",
		"GhsyncDriftPassMissing",
		"GhsyncWatermarkStalled",
		"GhsyncWatermarkPassMissing",
		"GhsyncDeriverPassMissing",
		"GhsyncRequiredRoleAbsent",
	} {
		if !strings.Contains(expressions[alert], "absent(") {
			t.Fatalf("%s does not fail closed on metric absence", alert)
		}
	}
	if durations["GhsyncSecondaryGateClosed"] != "5m" ||
		!strings.Contains(
			expressions["GhsyncSecondaryGateClosed"],
			"ghsync_c_b2_gate_closed",
		) || !strings.Contains(
		expressions["GhsyncSecondaryGateClosed"],
		"max by (installation_id, resource, auth_context)",
	) {
		t.Fatal("secondary gate alert is not a five-minute boolean condition")
	}
	if !strings.Contains(
		expressions["GhsyncBudgetClassStarved"],
		"sum by (class, resource, auth_context)",
	) {
		t.Fatal("budget starvation alert drops auth-context series identity")
	}
	for _, role := range []string{
		"metrics", "ingress", "dispatch", "fetch", "sweep", "drift",
		"pruner", "watermarker", "deriver",
	} {
		if !strings.Contains(
			expressions["GhsyncRequiredRoleAbsent"],
			`role="`+role+`"`,
		) {
			t.Fatalf("required-role alert omits %s", role)
		}
	}
	if !strings.Contains(
		expressions["GhsyncCacheStalenessBoundBreached"],
		"max by (entity_class)",
	) || !strings.Contains(
		expressions["GhsyncSweepOverrun"],
		"max by (sweep_kind)",
	) {
		t.Fatal("constraint comparisons are not reduced to one series per key")
	}

	for alert, expected := range map[string]struct {
		operation string
		threshold string
	}{
		"GhsyncStackSweepPassMissing": {
			operation: "stacks",
			threshold: "> 675",
		},
		"GhsyncPullRequestSweepPassMissing": {
			operation: "pull_requests",
			threshold: "> 1350",
		},
		"GhsyncRepoRulesSweepPassMissing": {
			operation: "repo_rules",
			threshold: "> 8100",
		},
		"GhsyncRepositorySweepPassMissing": {
			operation: "repositories",
			threshold: "> 8100",
		},
		"GhsyncClosedSweepPassMissing": {
			operation: "closed_tracked",
			threshold: "> 194400",
		},
	} {
		expression := expressions[alert]
		if !strings.Contains(
			expression,
			`component="sweep",operation="`+expected.operation+`"`,
		) || !strings.Contains(expression, expected.threshold) {
			t.Fatalf(
				"%s expression = %q, want operation %q threshold %q",
				alert,
				expression,
				expected.operation,
				expected.threshold,
			)
		}
		if durations[alert] != "5m" {
			t.Fatalf("%s for = %q, want 5m", alert, durations[alert])
		}
	}
	if _, exists := expressions["GhsyncSweepPassMissing"]; exists {
		t.Fatal("aggregate 24-hour sweep pass alert still exists")
	}
	if !strings.Contains(
		expressions["GhsyncDeriverPassMissing"],
		`component="deriver",operation="dirty_sets"`,
	) || !strings.Contains(
		expressions["GhsyncDeriverPassMissing"],
		"> 30",
	) {
		t.Fatal("deriver pass alert is not tied to its durable heartbeat")
	}
}

func TestAlertConditionalRatioExpressionsUseGatedWindowCounts(t *testing.T) {
	t.Parallel()
	body, err := os.ReadFile("../../ops/alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	expressions := make(map[string]string)
	for _, group := range document.Groups {
		for _, rule := range group.Rules {
			expressions[rule.Alert] = rule.Expr
		}
	}

	dashboard, err := os.ReadFile("../../ops/DASHBOARD.md")
	if err != nil {
		t.Fatal(err)
	}
	dashboardExpr := promQLBlock(t, string(dashboard))
	cases := []struct {
		name string
		expr string
	}{
		{
			name: "GhsyncSweepConditionalHitRateLow",
			expr: expressions["GhsyncSweepConditionalHitRateLow"],
		},
		{name: "dashboard 304 ratio", expr: dashboardExpr},
	}
	numerator := `sum(increase(ghsync_c_b4_conditional_304s_total{class="sweep",resource="rest"}[15m]))`
	denominator := `sum(increase(ghsync_c_b4_conditional_requests_total{class="sweep",resource="rest"}[15m]))`
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expr := strings.Join(strings.Fields(tc.expr), " ")
			if !parenthesesBalanced(expr) {
				t.Fatalf("ratio has unbalanced parentheses: %q", expr)
			}
			if strings.Contains(expr, "clamp_min(") {
				t.Fatalf("ratio floors a per-second denominator: %q", expr)
			}
			if strings.Contains(expr, "rate(") {
				t.Fatalf("ratio uses rates instead of window counts: %q", expr)
			}
			if !strings.Contains(expr, numerator) {
				t.Fatalf("ratio does not use increased 304 count: %q", expr)
			}
			if strings.Count(expr, denominator) < 2 {
				t.Fatalf("ratio lacks a separate request-count gate: %q", expr)
			}
			if !strings.Contains(expr, denominator+" > 0") {
				t.Fatalf("ratio does not require a non-empty denominator: %q", expr)
			}
		})
	}

	alertExpr := expressions["GhsyncSweepConditionalHitRateLow"]
	for _, series := range []string{
		"ghsync_c_b4_conditional_requests_total",
		"ghsync_c_b4_conditional_304s_total",
	} {
		if !strings.Contains(alertExpr, "absent("+series) {
			t.Fatalf("conditional-ratio alert lost absence check for %s", series)
		}
	}
	if 48.0/60.0 < 0.80 {
		t.Fatal("a healthy 48-of-60 sweep profile would breach the ratio")
	}

	resyncExpr := expressions["GhsyncResyncStorm"]
	if strings.Contains(resyncExpr, "absent(") {
		t.Fatalf("page-severity resync alert still fires on absence: %q", resyncExpr)
	}
}

func promQLBlock(t *testing.T, markdown string) string {
	t.Helper()
	const opener = "```promql"
	_, after, ok := strings.Cut(markdown, opener)
	if !ok {
		t.Fatal("dashboard has no PromQL block")
	}
	block := after
	before0, _, ok0 := strings.Cut(block, "```")
	if !ok0 {
		t.Fatal("dashboard PromQL block is not closed")
	}
	return before0
}

func parenthesesBalanced(expression string) bool {
	depth := 0
	for _, char := range expression {
		switch char {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}
