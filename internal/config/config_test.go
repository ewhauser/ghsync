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

func TestRequireWebhookSecret(t *testing.T) {
	if err := (Config{}).RequireWebhookSecret(); err == nil {
		t.Fatal("empty webhook secret accepted")
	}
	if err := (Config{GitHubWebhookSecret: "secret"}).RequireWebhookSecret(); err != nil {
		t.Fatalf("secret rejected: %v", err)
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL",
		"HTTP_ADDR",
		"GITHUB_APP_ID",
		"GITHUB_INSTALLATION_ID",
		"GITHUB_PRIVATE_KEY_PATH",
		"GITHUB_WEBHOOK_SECRET",
		"GITHUB_BASE_URL",
		"WEBHOOK_MAX_BODY_BYTES",
		"DISPATCH_BATCH_SIZE",
		"DISPATCH_MAX_ATTEMPTS",
		"DISPATCH_DEBOUNCE",
		"DISPATCH_POLL_INTERVAL",
	} {
		t.Setenv(key, "")
	}
}
