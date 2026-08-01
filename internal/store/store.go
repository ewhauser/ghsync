// Package store owns Postgres connectivity and schema migration.
package store

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"

	"github.com/ewhauser/ghsync/internal/store/dbgen"
	"github.com/ewhauser/ghsync/internal/store/rdsiam"
)

const defaultIdleInTransactionSessionTimeout = 30 * time.Second

type connectOptions struct {
	tracerProvider      trace.TracerProvider
	rdsIAM              bool
	rdsIAMTokenProvider rdsiam.TokenProvider
}

// ConnectOption customizes Postgres connectivity.
type ConnectOption func(*connectOptions)

// WithTracerProvider instruments pgx operations with the supplied provider.
func WithTracerProvider(provider trace.TracerProvider) ConnectOption {
	return func(options *connectOptions) {
		options.tracerProvider = provider
	}
}

// WithRDSIAMAuthentication generates a fresh Amazon RDS IAM authentication
// token for every new physical pool connection. DATABASE_URL must not contain
// a password when this option is used.
func WithRDSIAMAuthentication() ConnectOption {
	return func(options *connectOptions) {
		options.rdsIAM = true
	}
}

// withRDSIAMTokenProvider substitutes a token provider for offline tests.
func withRDSIAMTokenProvider(provider rdsiam.TokenProvider) ConnectOption {
	return func(options *connectOptions) {
		options.rdsIAM = true
		options.rdsIAMTokenProvider = provider
	}
}

// Connect opens and verifies a Postgres pool with the cache durability
// invariants enabled.
func Connect(
	ctx context.Context,
	databaseURL string,
	options ...ConnectOption,
) (*pgxpool.Pool, error) {
	var configured connectOptions
	for _, option := range options {
		option(&configured)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if configured.tracerProvider != nil {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer(
			otelpgx.WithTracerProvider(configured.tracerProvider),
			otelpgx.WithTrimSQLInSpanName(),
		)
	}
	if configured.rdsIAM {
		if err := validateRDSIAMConfig(cfg); err != nil {
			return nil, err
		}
		provider := configured.rdsIAMTokenProvider
		if provider == nil {
			provider, err = rdsiam.New(ctx)
			if err != nil {
				return nil, fmt.Errorf("configure RDS IAM database authentication: %w", err)
			}
		}
		if err := configureRDSIAM(cfg, provider); err != nil {
			return nil, err
		}
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	// C-I1 requires the commit acknowledged to ingress to have reached durable
	// WAL. Override role/database defaults as a startup parameter for every
	// pooled connection.
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	// Bound abandoned writer transactions at the pool boundary. DATABASE_URL
	// may override the default with PostgreSQL's
	// idle_in_transaction_session_timeout runtime parameter.
	if _, configured := cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"]; !configured {
		cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = defaultIdleInTransactionSessionTimeout.String()
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	synchronousCommit, err := dbgen.New(pool).ShowSynchronousCommit(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("verify synchronous_commit: %w", err)
	}
	if synchronousCommit != "on" {
		pool.Close()
		return nil, fmt.Errorf(
			"verify synchronous_commit: got %q, require %q",
			synchronousCommit,
			"on",
		)
	}
	return pool, nil
}

func configureRDSIAM(
	cfg *pgxpool.Config,
	provider rdsiam.TokenProvider,
) error {
	return configureRDSIAMWithTokenInstalledHook(cfg, provider, nil)
}

func configureRDSIAMWithTokenInstalledHook(
	cfg *pgxpool.Config,
	provider rdsiam.TokenProvider,
	tokenInstalled func(*pgx.ConnConfig) error,
) error {
	if err := validateRDSIAMConfig(cfg); err != nil {
		return err
	}

	previousBeforeConnect := cfg.BeforeConnect
	cfg.BeforeConnect = func(
		ctx context.Context,
		connConfig *pgx.ConnConfig,
	) (hookErr error) {
		token := ""
		defer func() {
			if recovered := recover(); recovered != nil {
				connConfig.Password = ""
				panic(redactedPanicValue(recovered, token))
			}
			if hookErr != nil {
				connConfig.Password = ""
				hookErr = redactTokenError(hookErr, token)
			}
		}()
		if previousBeforeConnect != nil {
			if err := previousBeforeConnect(ctx, connConfig); err != nil {
				return err
			}
		}
		endpoint := net.JoinHostPort(
			connConfig.Host,
			strconv.FormatUint(uint64(connConfig.Port), 10),
		)
		var err error
		token, err = requestRDSIAMToken(
			ctx,
			provider,
			endpoint,
			connConfig.User,
		)
		if err != nil {
			return err
		}
		if token == "" {
			return fmt.Errorf("generate RDS IAM database authentication token: provider returned an empty token")
		}
		// BeforeConnect receives a per-connection copy, so the token never enters
		// the pool's durable base configuration.
		connConfig.Password = token
		if tokenInstalled != nil {
			return tokenInstalled(connConfig)
		}
		return nil
	}
	// pgx retains the connection config in both successful connections and
	// pgconn.ConnectError values. Scrub the per-connect password before either
	// can escape pgx, and give any configured tracer a passwordless copy.
	cfg.ConnConfig.Tracer = newPasswordScrubbingTracer(cfg.ConnConfig.Tracer)
	return nil
}

func requestRDSIAMToken(
	ctx context.Context,
	provider rdsiam.TokenProvider,
	endpoint string,
	user string,
) (token string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			token = ""
			err = fmt.Errorf(
				"generate RDS IAM database authentication token: provider panicked",
			)
		}
	}()
	token, err = provider.Token(ctx, endpoint, user)
	if err == nil {
		return token, nil
	}
	if token != "" {
		return "", fmt.Errorf(
			"generate RDS IAM database authentication token: provider returned a token with an error; details redacted",
		)
	}
	return "", fmt.Errorf(
		"generate RDS IAM database authentication token: %w",
		err,
	)
}

func redactTokenError(err error, token string) error {
	if err == nil || token == "" || !strings.Contains(err.Error(), token) {
		return err
	}
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), token, "[REDACTED]"))
}

func validateRDSIAMConfig(cfg *pgxpool.Config) error {
	if cfg.ConnConfig.Password != "" {
		return fmt.Errorf(
			"configure RDS IAM database authentication: database password credentials must not be configured",
		)
	}
	if cfg.ConnConfig.Host == "" || strings.HasPrefix(cfg.ConnConfig.Host, "/") {
		return fmt.Errorf(
			"configure RDS IAM database authentication: DATABASE_URL must specify a TCP host",
		)
	}
	if cfg.ConnConfig.User == "" {
		return fmt.Errorf(
			"configure RDS IAM database authentication: DATABASE_URL must specify a user",
		)
	}
	for _, fallback := range cfg.ConnConfig.Fallbacks {
		if fallback.Host != cfg.ConnConfig.Host ||
			fallback.Port != cfg.ConnConfig.Port {
			return fmt.Errorf(
				"configure RDS IAM database authentication: DATABASE_URL must specify one database endpoint",
			)
		}
	}
	return nil
}
