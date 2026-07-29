// Package config loads frontier-syncd configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultHTTPAddr            = ":8080"
	defaultGitHubBaseURL       = "https://api.github.com"
	defaultWebhookMaxBodyBytes = int64(2 << 20)
	defaultDispatchBatchSize   = 100
	defaultDispatchMaxAttempts = 5
	defaultDispatchDebounce    = 5 * time.Second
	defaultDispatchPoll        = 250 * time.Millisecond
	maxDispatchDebounce        = 15 * time.Second

	defaultFetchBatchWindow = 5 * time.Millisecond
	defaultBackfillPageSize = 100

	defaultBudgetSweepFloor        = 0.20
	defaultBudgetEventFloor        = 0.10
	defaultBudgetMaxConcurrent     = 40
	defaultBudgetRESTLimit         = int64(15000)
	defaultBudgetGraphQLLimit      = int64(5000)
	defaultBudgetSecondaryFallback = 60 * time.Second

	defaultSweepOpenStackStaleness = 5 * time.Minute
	defaultSweepOpenPRStaleness    = 10 * time.Minute
	defaultSweepRepoRulesStaleness = time.Hour
	defaultSweepClosedStaleness    = 24 * time.Hour
	defaultSweepRepositoryPeriod   = time.Hour
	defaultSweepPageSize           = 100
	defaultGapHealPeriod           = 5 * time.Minute
	defaultGapWindow               = 6 * time.Hour
	defaultGapPageSize             = 100
	defaultGapMaxPages             = 10
	defaultDriftPeriod             = time.Hour
	defaultDriftSampleSize         = 10
	defaultDriftPageSize           = 100
	defaultDriftResolvedRetention  = 30 * 24 * time.Hour
	defaultRetentionPeriod         = 24 * time.Hour
	defaultRetentionAge            = 90 * 24 * time.Hour
	defaultRetentionBatchSize      = 1000

	defaultWatermarkRefresh      = 100 * time.Millisecond
	defaultWatermarkLeaseTTL     = 3 * time.Second
	defaultWatermarkFenceTimeout = time.Second
	defaultStreamRetentionPeriod = time.Hour
	defaultStreamRetentionAge    = 7 * 24 * time.Hour
	defaultStreamRetentionBatch  = 1000
	defaultDeriverPoll           = 500 * time.Millisecond
	defaultDeriverDirtyCap       = 500
)

type Config struct {
	// DatabaseURL is the Postgres connection string (DATABASE_URL).
	DatabaseURL string
	// HTTPAddr is the listen address for the ingress/health server (HTTP_ADDR).
	HTTPAddr string
	// GitHubAppID identifies the GitHub App installation (GITHUB_APP_ID).
	GitHubAppID int64
	// GitHubInstallationID is the single-org App installation
	// (GITHUB_INSTALLATION_ID).
	GitHubInstallationID int64
	// GitHubOrgID is the constant organization identity stored on mirror rows
	// (GITHUB_ORG_ID).
	GitHubOrgID int64
	// GitHubToken is a development/test escape hatch for fake GitHub. Production
	// uses App credentials and installation-token caching.
	GitHubToken string
	// GitHubPrivateKeyPath points at the App's PEM key (GITHUB_PRIVATE_KEY_PATH).
	GitHubPrivateKeyPath string
	// GitHubWebhookSecret verifies X-Hub-Signature-256 (GITHUB_WEBHOOK_SECRET).
	GitHubWebhookSecret string
	// GitHubBaseURL overrides the GitHub API endpoint; used to point at the
	// fake GitHub server in development and tests (GITHUB_BASE_URL).
	GitHubBaseURL string
	// WebhookMaxBodyBytes bounds ingress memory and storage per delivery
	// (WEBHOOK_MAX_BODY_BYTES).
	WebhookMaxBodyBytes int64
	// DispatchBatchSize is the maximum deliveries claimed per transaction
	// (DISPATCH_BATCH_SIZE).
	DispatchBatchSize int
	// DispatchMaxAttempts parks a poison delivery after this many
	// classification attempts (DISPATCH_MAX_ATTEMPTS).
	DispatchMaxAttempts int
	// DispatchDebounce bounds webhook burst coalescing
	// (DISPATCH_DEBOUNCE).
	DispatchDebounce time.Duration
	// DispatchPollInterval controls idle polling latency
	// (DISPATCH_POLL_INTERVAL).
	DispatchPollInterval time.Duration
	// DispatchRulesFile optionally replaces the built-in dispatcher rule table
	// with YAML/JSON data (DISPATCH_RULES_FILE).
	DispatchRulesFile string

	// FetchBatchWindow is the collection window for a pull-request GraphQL gang
	// (FETCH_BATCH_WINDOW).
	FetchBatchWindow time.Duration
	// BackfillPageSize is the GitHub page size for resumable backfill work
	// (BACKFILL_PAGE_SIZE).
	BackfillPageSize int

	// BudgetSweepFloor and BudgetEventFloor reserve installation rate-budget
	// headroom for higher-priority request classes.
	BudgetSweepFloor float64
	BudgetEventFloor float64
	// BudgetMaxConcurrent is the installation-wide GitHub request ceiling.
	BudgetMaxConcurrent int
	// BudgetRESTLimit and BudgetGraphQLLimit are pessimistic denominators until
	// GitHub supplies authoritative rate-limit observations.
	BudgetRESTLimit    int64
	BudgetGraphQLLimit int64
	// BudgetSecondaryFallback is used when a secondary-limit response supplies
	// no valid Retry-After value.
	BudgetSecondaryFallback time.Duration

	// M4 C-R1 bounds and periodic schedules are runtime configuration rather
	// than constants in the sweeper.
	SweepOpenStackMaxStaleness time.Duration
	SweepOpenPRMaxStaleness    time.Duration
	SweepRepoRulesMaxStaleness time.Duration
	SweepClosedMaxStaleness    time.Duration
	SweepRepositoryListPeriod  time.Duration
	SweepPageSize              int

	GapHealPeriod time.Duration
	GapWindow     time.Duration
	GapPageSize   int
	GapMaxPages   int

	DriftPeriod     time.Duration
	DriftSampleSize int
	// DriftPageSize independently bounds drift pagination
	// (DRIFT_PAGE_SIZE).
	DriftPageSize          int
	DriftResolvedRetention time.Duration

	RetentionPeriod    time.Duration
	RetentionAge       time.Duration
	RetentionBatchSize int

	// M5 C-S/C-D maintenance settings.
	WatermarkRefresh  time.Duration
	WatermarkLeaseTTL time.Duration
	// WatermarkFenceTimeout bounds the exclusive writer-fence wait
	// (STREAM_WATERMARK_FENCE_LOCK_TIMEOUT).
	WatermarkFenceTimeout time.Duration
	StreamRetentionPeriod time.Duration
	StreamRetentionAge    time.Duration
	StreamRetentionBatch  int
	DeriverPollInterval   time.Duration
	DeriverDirtyCap       int
}

func FromEnv() (Config, error) {
	cfg := Config{
		DatabaseURL:                os.Getenv("DATABASE_URL"),
		HTTPAddr:                   envOr("HTTP_ADDR", defaultHTTPAddr),
		GitHubPrivateKeyPath:       os.Getenv("GITHUB_PRIVATE_KEY_PATH"),
		GitHubWebhookSecret:        os.Getenv("GITHUB_WEBHOOK_SECRET"),
		GitHubToken:                os.Getenv("GITHUB_TOKEN"),
		GitHubBaseURL:              envOr("GITHUB_BASE_URL", defaultGitHubBaseURL),
		WebhookMaxBodyBytes:        defaultWebhookMaxBodyBytes,
		DispatchBatchSize:          defaultDispatchBatchSize,
		DispatchMaxAttempts:        defaultDispatchMaxAttempts,
		DispatchDebounce:           defaultDispatchDebounce,
		DispatchPollInterval:       defaultDispatchPoll,
		DispatchRulesFile:          os.Getenv("DISPATCH_RULES_FILE"),
		FetchBatchWindow:           defaultFetchBatchWindow,
		BackfillPageSize:           defaultBackfillPageSize,
		BudgetSweepFloor:           defaultBudgetSweepFloor,
		BudgetEventFloor:           defaultBudgetEventFloor,
		BudgetMaxConcurrent:        defaultBudgetMaxConcurrent,
		BudgetRESTLimit:            defaultBudgetRESTLimit,
		BudgetGraphQLLimit:         defaultBudgetGraphQLLimit,
		BudgetSecondaryFallback:    defaultBudgetSecondaryFallback,
		SweepOpenStackMaxStaleness: defaultSweepOpenStackStaleness,
		SweepOpenPRMaxStaleness:    defaultSweepOpenPRStaleness,
		SweepRepoRulesMaxStaleness: defaultSweepRepoRulesStaleness,
		SweepClosedMaxStaleness:    defaultSweepClosedStaleness,
		SweepRepositoryListPeriod:  defaultSweepRepositoryPeriod,
		SweepPageSize:              defaultSweepPageSize,
		GapHealPeriod:              defaultGapHealPeriod,
		GapWindow:                  defaultGapWindow,
		GapPageSize:                defaultGapPageSize,
		GapMaxPages:                defaultGapMaxPages,
		DriftPeriod:                defaultDriftPeriod,
		DriftSampleSize:            defaultDriftSampleSize,
		DriftPageSize:              defaultDriftPageSize,
		DriftResolvedRetention:     defaultDriftResolvedRetention,
		RetentionPeriod:            defaultRetentionPeriod,
		RetentionAge:               defaultRetentionAge,
		RetentionBatchSize:         defaultRetentionBatchSize,
		WatermarkRefresh:           defaultWatermarkRefresh,
		WatermarkLeaseTTL:          defaultWatermarkLeaseTTL,
		WatermarkFenceTimeout:      defaultWatermarkFenceTimeout,
		StreamRetentionPeriod:      defaultStreamRetentionPeriod,
		StreamRetentionAge:         defaultStreamRetentionAge,
		StreamRetentionBatch:       defaultStreamRetentionBatch,
		DeriverPollInterval:        defaultDeriverPoll,
		DeriverDirtyCap:            defaultDeriverDirtyCap,
	}
	if raw := os.Getenv("GITHUB_APP_ID"); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("GITHUB_APP_ID: %w", err)
		}
		cfg.GitHubAppID = id
	}
	if raw := os.Getenv("GITHUB_INSTALLATION_ID"); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("GITHUB_INSTALLATION_ID: %w", err)
		}
		cfg.GitHubInstallationID = id
	}
	if raw := os.Getenv("GITHUB_ORG_ID"); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("GITHUB_ORG_ID: %w", err)
		}
		cfg.GitHubOrgID = id
	}
	if raw := os.Getenv("WEBHOOK_MAX_BODY_BYTES"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value <= 0 {
			return Config{}, fmt.Errorf("WEBHOOK_MAX_BODY_BYTES must be a positive integer")
		}
		cfg.WebhookMaxBodyBytes = value
	}
	if raw := os.Getenv("DISPATCH_BATCH_SIZE"); raw != "" {
		value, err := parsePositiveInt("DISPATCH_BATCH_SIZE", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.DispatchBatchSize = value
	}
	if raw := os.Getenv("DISPATCH_MAX_ATTEMPTS"); raw != "" {
		value, err := parsePositiveInt("DISPATCH_MAX_ATTEMPTS", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.DispatchMaxAttempts = value
	}
	if raw := os.Getenv("DISPATCH_DEBOUNCE"); raw != "" {
		value, err := parsePositiveDuration("DISPATCH_DEBOUNCE", raw)
		if err != nil {
			return Config{}, err
		}
		if value > maxDispatchDebounce {
			return Config{}, fmt.Errorf(
				"DISPATCH_DEBOUNCE must not exceed %s",
				maxDispatchDebounce,
			)
		}
		cfg.DispatchDebounce = value
	}
	if raw := os.Getenv("DISPATCH_POLL_INTERVAL"); raw != "" {
		value, err := parsePositiveDuration("DISPATCH_POLL_INTERVAL", raw)
		if err != nil {
			return Config{}, err
		}
		cfg.DispatchPollInterval = value
	}
	durationValues := []struct {
		key    string
		target *time.Duration
	}{
		{"SWEEP_OPEN_STACK_MAX_STALENESS", &cfg.SweepOpenStackMaxStaleness},
		{"SWEEP_OPEN_PR_MAX_STALENESS", &cfg.SweepOpenPRMaxStaleness},
		{"SWEEP_REPO_RULES_MAX_STALENESS", &cfg.SweepRepoRulesMaxStaleness},
		{"SWEEP_CLOSED_MAX_STALENESS", &cfg.SweepClosedMaxStaleness},
		{"SWEEP_REPOSITORY_LIST_PERIOD", &cfg.SweepRepositoryListPeriod},
		{"GAP_HEAL_PERIOD", &cfg.GapHealPeriod},
		{"GAP_COMPARISON_WINDOW", &cfg.GapWindow},
		{"DRIFT_PERIOD", &cfg.DriftPeriod},
		{"DRIFT_RESOLVED_RETENTION", &cfg.DriftResolvedRetention},
		{"RETENTION_PERIOD", &cfg.RetentionPeriod},
		{"FETCH_BATCH_WINDOW", &cfg.FetchBatchWindow},
		{"BUDGET_SECONDARY_FALLBACK", &cfg.BudgetSecondaryFallback},
		{"STREAM_WATERMARK_REFRESH", &cfg.WatermarkRefresh},
		{"STREAM_WATERMARK_LEASE_TTL", &cfg.WatermarkLeaseTTL},
		{"STREAM_WATERMARK_FENCE_LOCK_TIMEOUT", &cfg.WatermarkFenceTimeout},
		{"STREAM_RETENTION_PERIOD", &cfg.StreamRetentionPeriod},
		{"DERIVER_POLL_INTERVAL", &cfg.DeriverPollInterval},
	}
	for _, item := range durationValues {
		if raw := os.Getenv(item.key); raw != "" {
			value, err := parsePositiveDuration(item.key, raw)
			if err != nil {
				return Config{}, err
			}
			*item.target = value
		}
	}
	intValues := []struct {
		key    string
		target *int
	}{
		{"SWEEP_PAGE_SIZE", &cfg.SweepPageSize},
		{"GAP_PAGE_SIZE", &cfg.GapPageSize},
		{"GAP_MAX_PAGES", &cfg.GapMaxPages},
		{"DRIFT_SAMPLE_SIZE", &cfg.DriftSampleSize},
		{"DRIFT_PAGE_SIZE", &cfg.DriftPageSize},
		{"RETENTION_BATCH_SIZE", &cfg.RetentionBatchSize},
		{"BACKFILL_PAGE_SIZE", &cfg.BackfillPageSize},
		{"BUDGET_MAX_CONCURRENT", &cfg.BudgetMaxConcurrent},
		{"STREAM_RETENTION_BATCH_SIZE", &cfg.StreamRetentionBatch},
		{"DERIVER_DIRTY_CAP", &cfg.DeriverDirtyCap},
	}
	for _, item := range intValues {
		if raw := os.Getenv(item.key); raw != "" {
			value, err := parsePositiveInt(item.key, raw)
			if err != nil {
				return Config{}, err
			}
			*item.target = value
		}
	}
	int64Values := []struct {
		key    string
		target *int64
	}{
		{"BUDGET_REST_LIMIT", &cfg.BudgetRESTLimit},
		{"BUDGET_GRAPHQL_LIMIT", &cfg.BudgetGraphQLLimit},
	}
	for _, item := range int64Values {
		if raw := os.Getenv(item.key); raw != "" {
			value, err := parsePositiveInt64(item.key, raw)
			if err != nil {
				return Config{}, err
			}
			*item.target = value
		}
	}
	floatValues := []struct {
		key    string
		target *float64
	}{
		{"BUDGET_SWEEP_FLOOR", &cfg.BudgetSweepFloor},
		{"BUDGET_EVENT_FLOOR", &cfg.BudgetEventFloor},
	}
	for _, item := range floatValues {
		if raw := os.Getenv(item.key); raw != "" {
			value, err := parseUnitFraction(item.key, raw)
			if err != nil {
				return Config{}, err
			}
			*item.target = value
		}
	}
	if raw := os.Getenv("RETENTION_AGE"); raw != "" {
		value, err := parsePositiveDuration("RETENTION_AGE", raw)
		if err != nil {
			return Config{}, err
		}
		if value < defaultRetentionAge {
			return Config{}, fmt.Errorf(
				"RETENTION_AGE must be at least the locked 90-day policy (%s)",
				defaultRetentionAge,
			)
		}
		cfg.RetentionAge = value
	}
	if raw := os.Getenv("STREAM_RETENTION_AGE"); raw != "" {
		value, err := parsePositiveDuration("STREAM_RETENTION_AGE", raw)
		if err != nil {
			return Config{}, err
		}
		if value < defaultStreamRetentionAge {
			return Config{}, fmt.Errorf(
				"STREAM_RETENTION_AGE must be at least the locked 7-day policy (%s)",
				defaultStreamRetentionAge,
			)
		}
		cfg.StreamRetentionAge = value
	}
	if cfg.WatermarkRefresh >= cfg.WatermarkLeaseTTL/2 {
		return Config{}, fmt.Errorf(
			"STREAM_WATERMARK_REFRESH must be less than half STREAM_WATERMARK_LEASE_TTL",
		)
	}
	if cfg.BudgetEventFloor >= cfg.BudgetSweepFloor {
		return Config{}, fmt.Errorf(
			"BUDGET_EVENT_FLOOR must be less than BUDGET_SWEEP_FLOOR",
		)
	}
	if cfg.BackfillPageSize > 100 {
		return Config{}, fmt.Errorf("BACKFILL_PAGE_SIZE must not exceed 100")
	}
	if cfg.SweepPageSize > 100 || cfg.GapPageSize > 100 ||
		cfg.DriftPageSize > 100 {
		return Config{}, fmt.Errorf(
			"SWEEP_PAGE_SIZE, GAP_PAGE_SIZE, and DRIFT_PAGE_SIZE must not exceed 100",
		)
	}
	return cfg, nil
}

// RequireDatabase returns an error when the configuration lacks a database
// URL; commands that touch Postgres call this up front for a clear message.
func (c Config) RequireDatabase() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	return nil
}

// RequireWebhookSecret fails closed for an ingress role with no HMAC secret.
func (c Config) RequireWebhookSecret() error {
	if c.GitHubWebhookSecret == "" {
		return fmt.Errorf("GITHUB_WEBHOOK_SECRET is required for the ingress role")
	}
	return nil
}

func (c Config) RequireFetchCredentials() error {
	if c.GitHubInstallationID <= 0 || c.GitHubOrgID <= 0 {
		return fmt.Errorf(
			"GITHUB_INSTALLATION_ID and GITHUB_ORG_ID are required for fetch",
		)
	}
	if c.GitHubToken != "" {
		return nil
	}
	if c.GitHubAppID <= 0 || c.GitHubPrivateKeyPath == "" {
		return fmt.Errorf(
			"GITHUB_APP_ID and GITHUB_PRIVATE_KEY_PATH are required for fetch",
		)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parsePositiveInt(key, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return value, nil
}

func parsePositiveInt64(key, raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return value, nil
}

func parseUnitFraction(key, raw string) (float64, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || !(value > 0 && value < 1) {
		return 0, fmt.Errorf("%s must be greater than 0 and less than 1", key)
	}
	return value, nil
}

func parsePositiveDuration(key, raw string) (time.Duration, error) {
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return value, nil
}
