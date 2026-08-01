package config

import (
	"strings"
	"testing"
	"time"
)

func TestFromEnvDatabaseAuthentication(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseAuth != DatabaseAuthPassword {
		t.Fatalf("database auth = %q, want %q", cfg.DatabaseAuth, DatabaseAuthPassword)
	}

	t.Setenv("DATABASE_AUTH", string(DatabaseAuthRDSIAM))
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseAuth != DatabaseAuthRDSIAM {
		t.Fatalf("database auth = %q, want %q", cfg.DatabaseAuth, DatabaseAuthRDSIAM)
	}

	t.Setenv("DATABASE_AUTH", "token")
	if _, err := FromEnv(); err == nil {
		t.Fatal("unsupported DATABASE_AUTH accepted")
	}
}

func TestParseDatabaseAuth(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]DatabaseAuth{
		"":         DatabaseAuthPassword,
		"password": DatabaseAuthPassword,
		"rds-iam":  DatabaseAuthRDSIAM,
	} {
		got, err := ParseDatabaseAuth(value)
		if err != nil {
			t.Fatalf("ParseDatabaseAuth(%q): %v", value, err)
		}
		if got != want {
			t.Fatalf("ParseDatabaseAuth(%q) = %q, want %q", value, got, want)
		}
	}
	if _, err := ParseDatabaseAuth("token"); err == nil {
		t.Fatal("unsupported database authentication mode accepted")
	}
}

func TestRequireDatabaseRDSIAMRejectsPassword(t *testing.T) {
	t.Parallel()

	passwordURL := Config{
		DatabaseURL:  "postgres://app:secret@db.example.com:5432/ghsync",
		DatabaseAuth: DatabaseAuthRDSIAM,
	}
	err := passwordURL.RequireDatabase()
	if err == nil || !strings.Contains(err.Error(), "password credentials must not be configured") {
		t.Fatalf("password-bearing RDS IAM URL error = %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("validation error disclosed DATABASE_URL password: %v", err)
	}
	queryPasswordURL := passwordURL
	queryPasswordURL.DatabaseURL = "postgres://app@db.example.com:5432/ghsync?password=secret"
	if err := queryPasswordURL.RequireDatabase(); err == nil {
		t.Fatal("RDS IAM URL with a password query parameter accepted")
	}
	keywordPassword := passwordURL
	keywordPassword.DatabaseURL = "host=db.example.com user=app dbname=ghsync password=secret"
	if err := keywordPassword.RequireDatabase(); err == nil {
		t.Fatal("RDS IAM keyword connection string with a password accepted")
	} else if strings.Contains(err.Error(), "secret") {
		t.Fatalf("keyword validation error disclosed password: %v", err)
	}
	malformed := passwordURL
	malformed.DatabaseURL = "postgres://app:secret%@db.example.com/ghsync"
	if err := malformed.RequireDatabase(); err == nil {
		t.Fatal("malformed RDS IAM URL accepted")
	} else if strings.Contains(err.Error(), "secret") {
		t.Fatalf("parse error disclosed malformed DATABASE_URL password: %v", err)
	}

	passwordlessURL := Config{
		DatabaseURL: "postgres://app@db.example.com:5432/ghsync" +
			"?sslmode=verify-full&search_path=mirror",
		DatabaseAuth: DatabaseAuthRDSIAM,
	}
	if err := passwordlessURL.RequireDatabase(); err != nil {
		t.Fatalf("passwordless RDS IAM URL rejected: %v", err)
	}

	passwordMode := Config{
		DatabaseURL:  passwordURL.DatabaseURL,
		DatabaseAuth: DatabaseAuthPassword,
	}
	if err := passwordMode.RequireDatabase(); err != nil {
		t.Fatalf("password mode rejected existing DATABASE_URL: %v", err)
	}
}

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

func TestFromEnvFetchDefaultsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FetchBatchWindow != defaultFetchBatchWindow ||
		cfg.BackfillPageSize != defaultBackfillPageSize {
		t.Fatalf("unexpected fetch defaults: %+v", cfg)
	}

	t.Setenv("FETCH_BATCH_WINDOW", "12ms")
	t.Setenv("BACKFILL_PAGE_SIZE", "75")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FetchBatchWindow != 12*time.Millisecond ||
		cfg.BackfillPageSize != 75 {
		t.Fatalf("unexpected fetch overrides: %+v", cfg)
	}
}

func TestFromEnvRejectsInvalidFetchValues(t *testing.T) {
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "FETCH_BATCH_WINDOW", value: "0s"},
		{key: "BACKFILL_PAGE_SIZE", value: "0"},
		{key: "BACKFILL_PAGE_SIZE", value: "101"},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(test.key, test.value)
			if _, err := FromEnv(); err == nil {
				t.Fatalf("%s=%q accepted", test.key, test.value)
			}
		})
	}
}

func TestFromEnvBudgetDefaultsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BudgetSweepFloor != defaultBudgetSweepFloor ||
		cfg.BudgetEventFloor != defaultBudgetEventFloor ||
		cfg.BudgetMaxConcurrent != defaultBudgetMaxConcurrent ||
		cfg.BudgetRESTLimit != defaultBudgetRESTLimit ||
		cfg.BudgetGraphQLLimit != defaultBudgetGraphQLLimit ||
		cfg.BudgetSecondaryFallback != defaultBudgetSecondaryFallback ||
		cfg.BudgetLeaseTTL != defaultBudgetLeaseTTL ||
		cfg.BudgetLeaseRenewInterval != defaultBudgetLeaseRenew {
		t.Fatalf("unexpected budget defaults: %+v", cfg)
	}

	t.Setenv("BUDGET_SWEEP_FLOOR", "0.30")
	t.Setenv("BUDGET_EVENT_FLOOR", "0.15")
	t.Setenv("BUDGET_MAX_CONCURRENT", "12")
	t.Setenv("BUDGET_REST_LIMIT", "12000")
	t.Setenv("BUDGET_GRAPHQL_LIMIT", "4000")
	t.Setenv("BUDGET_SECONDARY_FALLBACK", "90s")
	t.Setenv("BUDGET_LEASE_TTL", "12s")
	t.Setenv("BUDGET_LEASE_RENEW_INTERVAL", "3s")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BudgetSweepFloor != 0.30 ||
		cfg.BudgetEventFloor != 0.15 ||
		cfg.BudgetMaxConcurrent != 12 ||
		cfg.BudgetRESTLimit != 12000 ||
		cfg.BudgetGraphQLLimit != 4000 ||
		cfg.BudgetSecondaryFallback != 90*time.Second ||
		cfg.BudgetLeaseTTL != 12*time.Second ||
		cfg.BudgetLeaseRenewInterval != 3*time.Second {
		t.Fatalf("unexpected budget overrides: %+v", cfg)
	}
}

func TestFromEnvRejectsInvalidBudgetValues(t *testing.T) {
	for _, test := range []struct {
		key   string
		value string
	}{
		{key: "BUDGET_SWEEP_FLOOR", value: "0"},
		{key: "BUDGET_SWEEP_FLOOR", value: "1"},
		{key: "BUDGET_EVENT_FLOOR", value: "0"},
		{key: "BUDGET_EVENT_FLOOR", value: "1"},
		{key: "BUDGET_MAX_CONCURRENT", value: "0"},
		{key: "BUDGET_REST_LIMIT", value: "-1"},
		{key: "BUDGET_GRAPHQL_LIMIT", value: "many"},
		{key: "BUDGET_SECONDARY_FALLBACK", value: "0s"},
		{key: "BUDGET_LEASE_TTL", value: "0s"},
		{key: "BUDGET_LEASE_RENEW_INTERVAL", value: "0s"},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(test.key, test.value)
			if _, err := FromEnv(); err == nil {
				t.Fatalf("%s=%q accepted", test.key, test.value)
			}
		})
	}
}

func TestFromEnvRequiresBudgetRenewalShorterThanTTL(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("BUDGET_LEASE_TTL", "6s")
	t.Setenv("BUDGET_LEASE_RENEW_INTERVAL", "6s")
	if _, err := FromEnv(); err == nil {
		t.Fatal("budget renew interval equal to TTL accepted")
	}
}

func TestFromEnvRequiresEventFloorBelowSweepFloor(t *testing.T) {
	for _, test := range []struct {
		name       string
		eventFloor string
		sweepFloor string
	}{
		{name: "equal", eventFloor: "0.20", sweepFloor: "0.20"},
		{name: "event higher", eventFloor: "0.21", sweepFloor: "0.20"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BUDGET_EVENT_FLOOR", test.eventFloor)
			t.Setenv("BUDGET_SWEEP_FLOOR", test.sweepFloor)
			if _, err := FromEnv(); err == nil {
				t.Fatalf(
					"event floor %s with sweep floor %s accepted",
					test.eventFloor,
					test.sweepFloor,
				)
			}
		})
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
		cfg.DriftPageSize != defaultDriftPageSize ||
		cfg.RetentionAge != 90*24*time.Hour {
		t.Fatalf("unexpected M4 defaults: %+v", cfg)
	}
	t.Setenv("SWEEP_OPEN_STACK_MAX_STALENESS", "30s")
	t.Setenv("SWEEP_PAGE_SIZE", "50")
	t.Setenv("GAP_COMPARISON_WINDOW", "2h")
	t.Setenv("DRIFT_SAMPLE_SIZE", "7")
	t.Setenv("DRIFT_PAGE_SIZE", "50")
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
		cfg.DriftPageSize != 50 ||
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
		"DRIFT_PAGE_SIZE":             "101",
		"RETENTION_AGE":               "-1h",
		"RETENTION_BATCH_SIZE":        "0",
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

func TestRetentionAgeCannotShortenLockedPolicy(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("RETENTION_AGE", "90h")
	if _, err := FromEnv(); err == nil {
		t.Fatal("RETENTION_AGE=90h shortened the locked 90-day policy")
	}

	clearConfigEnv(t)
	t.Setenv("RETENTION_AGE", "2400h")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("explicit longer retention rejected: %v", err)
	}
	if cfg.RetentionAge != 2400*time.Hour {
		t.Fatalf("retention age = %s, want 2400h", cfg.RetentionAge)
	}
}

func TestWatermarkRefreshMustBeLessThanHalfLeaseTTL(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("STREAM_WATERMARK_LEASE_TTL", "2s")
	t.Setenv("STREAM_WATERMARK_REFRESH", "999.999999ms")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("refresh just below boundary rejected: %v", err)
	}
	if cfg.WatermarkRefresh != time.Second-time.Nanosecond {
		t.Fatalf("watermark refresh = %s", cfg.WatermarkRefresh)
	}

	clearConfigEnv(t)
	t.Setenv("STREAM_WATERMARK_LEASE_TTL", "2s")
	t.Setenv("STREAM_WATERMARK_REFRESH", "1s")
	if _, err := FromEnv(); err == nil {
		t.Fatal("refresh equal to half the lease TTL accepted")
	}
}

func TestWatermarkFenceTimeoutDefaultsOverridesAndValidates(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WatermarkFenceTimeout != defaultWatermarkFenceTimeout {
		t.Fatalf(
			"watermark fence timeout = %s, want %s",
			cfg.WatermarkFenceTimeout,
			defaultWatermarkFenceTimeout,
		)
	}

	t.Setenv("STREAM_WATERMARK_FENCE_LOCK_TIMEOUT", "750ms")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatalf("watermark fence timeout override rejected: %v", err)
	}
	if cfg.WatermarkFenceTimeout != 750*time.Millisecond {
		t.Fatalf("watermark fence timeout = %s, want 750ms", cfg.WatermarkFenceTimeout)
	}

	clearConfigEnv(t)
	t.Setenv("STREAM_WATERMARK_FENCE_LOCK_TIMEOUT", "0s")
	if _, err := FromEnv(); err == nil {
		t.Fatal("zero watermark fence timeout accepted")
	}
}

func TestStreamRetentionAgeCannotShortenSevenDayPolicy(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("STREAM_RETENTION_AGE", "167h")
	if _, err := FromEnv(); err == nil {
		t.Fatal("STREAM_RETENTION_AGE=167h shortened the seven-day policy")
	}

	clearConfigEnv(t)
	t.Setenv("STREAM_RETENTION_AGE", "168h")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("seven-day boundary rejected: %v", err)
	}
	if cfg.StreamRetentionAge != 7*24*time.Hour {
		t.Fatalf("stream retention age = %s, want 168h", cfg.StreamRetentionAge)
	}
}

func TestRequireWebhookSecret(t *testing.T) {
	t.Parallel()

	if err := (Config{}).RequireWebhookSecret(); err == nil {
		t.Fatal("empty webhook secret accepted")
	}
	if err := (Config{GitHubWebhookSecret: "secret"}).RequireWebhookSecret(); err != nil {
		t.Fatalf("secret rejected: %v", err)
	}
}

func TestRequireFetchCredentials(t *testing.T) {
	t.Parallel()

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
		"DATABASE_AUTH",
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
		"FETCH_BATCH_WINDOW",
		"BACKFILL_PAGE_SIZE",
		"BUDGET_SWEEP_FLOOR",
		"BUDGET_EVENT_FLOOR",
		"BUDGET_MAX_CONCURRENT",
		"BUDGET_REST_LIMIT",
		"BUDGET_GRAPHQL_LIMIT",
		"BUDGET_SECONDARY_FALLBACK",
		"BUDGET_LEASE_TTL",
		"BUDGET_LEASE_RENEW_INTERVAL",
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
		"DRIFT_PAGE_SIZE",
		"DRIFT_RESOLVED_RETENTION",
		"RETENTION_PERIOD",
		"RETENTION_AGE",
		"RETENTION_BATCH_SIZE",
		"STREAM_WATERMARK_REFRESH",
		"STREAM_WATERMARK_LEASE_TTL",
		"STREAM_WATERMARK_FENCE_LOCK_TIMEOUT",
		"STREAM_RETENTION_PERIOD",
		"STREAM_RETENTION_AGE",
		"STREAM_RETENTION_BATCH_SIZE",
		"DERIVER_POLL_INTERVAL",
		"DERIVER_DIRTY_CAP",
	} {
		t.Setenv(key, "")
	}
}
