// Package config loads frontier-syncd configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
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
	// GitHubPrivateKeyPath points at the App's PEM key (GITHUB_PRIVATE_KEY_PATH).
	GitHubPrivateKeyPath string
	// GitHubWebhookSecret verifies X-Hub-Signature-256 (GITHUB_WEBHOOK_SECRET).
	GitHubWebhookSecret string
	// GitHubBaseURL overrides the GitHub API endpoint; used to point at the
	// fake GitHub server in development and tests (GITHUB_BASE_URL).
	GitHubBaseURL string
}

func FromEnv() (Config, error) {
	cfg := Config{
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		HTTPAddr:             envOr("HTTP_ADDR", ":8080"),
		GitHubPrivateKeyPath: os.Getenv("GITHUB_PRIVATE_KEY_PATH"),
		GitHubWebhookSecret:  os.Getenv("GITHUB_WEBHOOK_SECRET"),
		GitHubBaseURL:        envOr("GITHUB_BASE_URL", "https://api.github.com"),
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
