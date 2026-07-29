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
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	count := 0
	expressions := make(map[string]string)
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
}
