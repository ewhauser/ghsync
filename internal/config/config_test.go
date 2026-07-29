package config

import (
	"testing"
	"time"
)

func TestFromEnvDispatchDefaultsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebhookMaxBodyBytes != defaultWebhookMaxBodyBytes ||
		cfg.DispatchBatchSize != defaultDispatchBatchSize ||
		cfg.DispatchMaxAttempts != defaultDispatchMaxAttempts ||
		cfg.DispatchDebounce != defaultDispatchDebounce ||
		cfg.DispatchPollInterval != defaultDispatchPoll {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	t.Setenv("WEBHOOK_MAX_BODY_BYTES", "4096")
	t.Setenv("DISPATCH_BATCH_SIZE", "17")
	t.Setenv("DISPATCH_MAX_ATTEMPTS", "3")
	t.Setenv("DISPATCH_DEBOUNCE", "7s")
	t.Setenv("DISPATCH_POLL_INTERVAL", "1s")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebhookMaxBodyBytes != 4096 ||
		cfg.DispatchBatchSize != 17 ||
		cfg.DispatchMaxAttempts != 3 ||
		cfg.DispatchDebounce != 7*time.Second ||
		cfg.DispatchPollInterval != time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}

func TestFromEnvRejectsInvalidDispatchValues(t *testing.T) {
	for key, value := range map[string]string{
		"WEBHOOK_MAX_BODY_BYTES": "0",
		"DISPATCH_BATCH_SIZE":    "-1",
		"DISPATCH_MAX_ATTEMPTS":  "many",
		"DISPATCH_DEBOUNCE":      "0s",
		"DISPATCH_POLL_INTERVAL": "soon",
	} {
		t.Run(key, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(key, value)
			if _, err := FromEnv(); err == nil {
				t.Fatalf("%s=%q accepted", key, value)
			}
		})
	}
}

func TestFromEnvDispatchDebounceHardCap(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("DISPATCH_DEBOUNCE", "15s")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("15s boundary rejected: %v", err)
	}
	if cfg.DispatchDebounce != 15*time.Second {
		t.Fatalf("debounce = %s, want 15s", cfg.DispatchDebounce)
	}

	clearConfigEnv(t)
	t.Setenv("DISPATCH_DEBOUNCE", "15.000000001s")
	if _, err := FromEnv(); err == nil {
		t.Fatal("15s + 1ns debounce accepted")
	}
}

func TestFromEnvM4ScheduleDefaultsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SweepOpenStackMaxStaleness != 5*time.Minute ||
		cfg.SweepOpenPRMaxStaleness != 10*time.Minute ||
		cfg.SweepRepoRulesMaxStaleness != time.Hour ||
		cfg.SweepClosedMaxStaleness != 24*time.Hour ||
		cfg.RetentionAge != 90*24*time.Hour {
		t.Fatalf("unexpected M4 defaults: %+v", cfg)
	}
	t.Setenv("SWEEP_OPEN_STACK_MAX_STALENESS", "30s")
	t.Setenv("SWEEP_PAGE_SIZE", "50")
	t.Setenv("GAP_COMPARISON_WINDOW", "2h")
	t.Setenv("DRIFT_SAMPLE_SIZE", "7")
	t.Setenv("RETENTION_AGE", "2160h")
	t.Setenv("DISPATCH_RULES_FILE", "/tmp/rules.yaml")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SweepOpenStackMaxStaleness != 30*time.Second ||
		cfg.SweepPageSize != 50 ||
		cfg.GapWindow != 2*time.Hour ||
		cfg.DriftSampleSize != 7 ||
		cfg.RetentionAge != 90*24*time.Hour ||
		cfg.DispatchRulesFile != "/tmp/rules.yaml" {
		t.Fatalf("unexpected M4 overrides: %+v", cfg)
	}
}

func TestFromEnvRejectsInvalidM4Values(t *testing.T) {
	for key, value := range map[string]string{
		"SWEEP_OPEN_PR_MAX_STALENESS": "0s",
		"SWEEP_PAGE_SIZE":             "101",
		"GAP_PAGE_SIZE":               "101",
		"GAP_MAX_PAGES":               "0",
		"DRIFT_SAMPLE_SIZE":           "none",
		"RETENTION_AGE":               "-1h",
	} {
		t.Run(key, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(key, value)
			if _, err := FromEnv(); err == nil {
				t.Fatalf("%s=%q accepted", key, value)
			}
		})
	}
}

func TestRequireWebhookSecret(t *testing.T) {
	if err := (Config{}).RequireWebhookSecret(); err == nil {
		t.Fatal("empty webhook secret accepted")
	}
	if err := (Config{GitHubWebhookSecret: "secret"}).RequireWebhookSecret(); err != nil {
		t.Fatalf("secret rejected: %v", err)
	}
}

func TestRequireFetchCredentials(t *testing.T) {
	if err := (Config{}).RequireFetchCredentials(); err == nil {
		t.Fatal("empty fetch credentials accepted")
	}
	static := Config{
		GitHubInstallationID: 1,
		GitHubOrgID:          2,
		GitHubToken:          "dev-token",
	}
	if err := static.RequireFetchCredentials(); err != nil {
		t.Fatalf("static fake-GitHub token rejected: %v", err)
	}
	app := Config{
		GitHubInstallationID: 1,
		GitHubOrgID:          2,
		GitHubAppID:          3,
		GitHubPrivateKeyPath: "/tmp/key.pem",
	}
	if err := app.RequireFetchCredentials(); err != nil {
		t.Fatalf("App credentials rejected: %v", err)
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL",
		"HTTP_ADDR",
		"GITHUB_APP_ID",
		"GITHUB_INSTALLATION_ID",
		"GITHUB_ORG_ID",
		"GITHUB_TOKEN",
		"GITHUB_PRIVATE_KEY_PATH",
		"GITHUB_WEBHOOK_SECRET",
		"GITHUB_BASE_URL",
		"WEBHOOK_MAX_BODY_BYTES",
		"DISPATCH_BATCH_SIZE",
		"DISPATCH_MAX_ATTEMPTS",
		"DISPATCH_DEBOUNCE",
		"DISPATCH_POLL_INTERVAL",
		"DISPATCH_RULES_FILE",
		"SWEEP_OPEN_STACK_MAX_STALENESS",
		"SWEEP_OPEN_PR_MAX_STALENESS",
		"SWEEP_REPO_RULES_MAX_STALENESS",
		"SWEEP_CLOSED_MAX_STALENESS",
		"SWEEP_REPOSITORY_LIST_PERIOD",
		"SWEEP_PAGE_SIZE",
		"GAP_HEAL_PERIOD",
		"GAP_COMPARISON_WINDOW",
		"GAP_PAGE_SIZE",
		"GAP_MAX_PAGES",
		"DRIFT_PERIOD",
		"DRIFT_SAMPLE_SIZE",
		"RETENTION_PERIOD",
		"RETENTION_AGE",
	} {
		t.Setenv(key, "")
	}
}
