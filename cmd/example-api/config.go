package main

import (
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ewhauser/ghsync/internal/config"
)

const (
	defaultAPIAddr          = ":8081"
	defaultConsumerName     = "example-api"
	defaultRingSize         = 512
	defaultSubscriberBuffer = 64
	defaultReplayLimit      = 1000
	defaultListLimit        = 100
	maximumListLimit        = 500
	maximumRingSize         = 100_000
	maximumSubscriberBuffer = 10_000
	maximumReplayLimit      = 100_000
)

type apiConfig struct {
	databaseURL      string
	databaseAuth     config.DatabaseAuth
	addr             string
	consumerName     string
	ringSize         int
	subscriberBuffer int
	replayLimit      int
}

func parseConfig(
	args []string,
	getenv func(string) string,
	output io.Writer,
) (apiConfig, error) {
	fs := flag.NewFlagSet("example-api", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		_, _ = fmt.Fprintln(
			output,
			"example-api is a reference example of ghsync's zero-duplication consumer pattern; it is not a production service.",
		)
		_, _ = fmt.Fprintln(output)
		_, _ = fmt.Fprintln(output, "Usage: example-api")
		_, _ = fmt.Fprintln(output)
		_, _ = fmt.Fprintf(output, `Environment:
  DATABASE_URL           required; use a ghsync_consumer-grade role
  DATABASE_AUTH          password (default) or rds-iam
  API_ADDR               listen address (default %s)
  API_CONSUMER_NAME      durable entities consumer (default %s)
  API_RING_SIZE          recent materialized events retained (default %d; max %d)
  API_SUBSCRIBER_BUFFER  queued events per subscriber (default %d; max %d)
  API_REPLAY_LIMIT       maximum direct-DB replay before resnapshot (default %d; max %d)
`, defaultAPIAddr, defaultConsumerName, defaultRingSize, maximumRingSize, defaultSubscriberBuffer, maximumSubscriberBuffer, defaultReplayLimit, maximumReplayLimit)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return apiConfig{}, err
	}
	if fs.NArg() != 0 {
		return apiConfig{}, fmt.Errorf("example-api does not accept positional arguments")
	}
	databaseURL := strings.TrimSpace(getenv("DATABASE_URL"))
	if databaseURL == "" {
		return apiConfig{}, fmt.Errorf("DATABASE_URL is required")
	}
	databaseAuth, err := config.ParseDatabaseAuth(getenv("DATABASE_AUTH"))
	if err != nil {
		return apiConfig{}, err
	}
	addr := strings.TrimSpace(getenv("API_ADDR"))
	if addr == "" {
		addr = defaultAPIAddr
	}
	consumerName := strings.TrimSpace(getenv("API_CONSUMER_NAME"))
	if consumerName == "" {
		consumerName = defaultConsumerName
	}
	ringSize, err := boundedEnvInt(
		getenv,
		"API_RING_SIZE",
		defaultRingSize,
		maximumRingSize,
	)
	if err != nil {
		return apiConfig{}, err
	}
	subscriberBuffer, err := boundedEnvInt(
		getenv,
		"API_SUBSCRIBER_BUFFER",
		defaultSubscriberBuffer,
		maximumSubscriberBuffer,
	)
	if err != nil {
		return apiConfig{}, err
	}
	replayLimit, err := boundedEnvInt(
		getenv,
		"API_REPLAY_LIMIT",
		defaultReplayLimit,
		maximumReplayLimit,
	)
	if err != nil {
		return apiConfig{}, err
	}
	return apiConfig{
		databaseURL:      databaseURL,
		databaseAuth:     databaseAuth,
		addr:             addr,
		consumerName:     consumerName,
		ringSize:         ringSize,
		subscriberBuffer: subscriberBuffer,
		replayLimit:      replayLimit,
	}, nil
}

func boundedEnvInt(
	getenv func(string) string,
	name string,
	fallback int,
	maximum int,
) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("%s must be between 1 and %d", name, maximum)
	}
	return parsed, nil
}
