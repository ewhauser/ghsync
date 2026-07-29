package metrics

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAlertRulesReferenceConstraintsAndLockedThresholds(t *testing.T) {
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
		"FrontierSweepConditionalHitRateLow": "0.80",
		"FrontierEventCacheP95SLOBreach":     "> 20",
		"FrontierEventCacheP99SLOBreach":     "> 60",
		"FrontierDriftOpen":                  "> 0",
		"FrontierResyncStorm":                "> 3",
	} {
		if !strings.Contains(expressions[alert], threshold) {
			t.Fatalf("%s expression = %q, missing %q", alert, expressions[alert], threshold)
		}
	}
	if _, exists := expressions["FrontierBudgetFloorBreached"]; exists {
		t.Fatal("lower-priority budget floor is still treated as an invariant")
	}
	for _, alert := range []string{
		"FrontierDriftOpen",
		"FrontierDriftPassMissing",
		"FrontierWatermarkStalled",
		"FrontierWatermarkPassMissing",
		"FrontierRequiredRoleAbsent",
	} {
		if !strings.Contains(expressions[alert], "absent(") {
			t.Fatalf("%s does not fail closed on metric absence", alert)
		}
	}
	if durations["FrontierSecondaryGateClosed"] != "5m" ||
		!strings.Contains(
			expressions["FrontierSecondaryGateClosed"],
			"frontier_c_b2_gate_closed",
		) {
		t.Fatal("secondary gate alert is not a five-minute boolean condition")
	}
	for _, role := range []string{
		"metrics", "ingress", "dispatch", "fetch", "sweep", "drift",
		"pruner", "watermarker", "deriver",
	} {
		if !strings.Contains(
			expressions["FrontierRequiredRoleAbsent"],
			`role="`+role+`"`,
		) {
			t.Fatalf("required-role alert omits %s", role)
		}
	}
	if !strings.Contains(
		expressions["FrontierCacheStalenessBoundBreached"],
		"max by (entity_class)",
	) || !strings.Contains(
		expressions["FrontierSweepOverrun"],
		"max by (sweep_kind)",
	) {
		t.Fatal("constraint comparisons are not reduced to one series per key")
	}
}

func TestAlertConditionalRatioExpressionsUseGatedWindowCounts(t *testing.T) {
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
			name: "FrontierSweepConditionalHitRateLow",
			expr: expressions["FrontierSweepConditionalHitRateLow"],
		},
		{name: "dashboard 304 ratio", expr: dashboardExpr},
	}
	numerator := `sum(increase(frontier_c_b4_conditional_304s_total{class="sweep",resource="rest"}[15m]))`
	denominator := `sum(increase(frontier_c_b4_conditional_requests_total{class="sweep",resource="rest"}[15m]))`
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

	alertExpr := expressions["FrontierSweepConditionalHitRateLow"]
	for _, series := range []string{
		"frontier_c_b4_conditional_requests_total",
		"frontier_c_b4_conditional_304s_total",
	} {
		if !strings.Contains(alertExpr, "absent("+series) {
			t.Fatalf("conditional-ratio alert lost absence check for %s", series)
		}
	}
	if 48.0/60.0 < 0.80 {
		t.Fatal("a healthy 48-of-60 sweep profile would breach the ratio")
	}

	resyncExpr := expressions["FrontierResyncStorm"]
	if strings.Contains(resyncExpr, "absent(") {
		t.Fatalf("page-severity resync alert still fires on absence: %q", resyncExpr)
	}
}

func promQLBlock(t *testing.T, markdown string) string {
	t.Helper()
	const opener = "```promql"
	start := strings.Index(markdown, opener)
	if start < 0 {
		t.Fatal("dashboard has no PromQL block")
	}
	block := markdown[start+len(opener):]
	end := strings.Index(block, "```")
	if end < 0 {
		t.Fatal("dashboard PromQL block is not closed")
	}
	return block[:end]
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
