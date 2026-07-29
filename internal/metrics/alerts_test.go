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
