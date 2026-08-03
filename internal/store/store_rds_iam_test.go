package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/peterldowns/pgtestdb"

	"github.com/ewhauser/ghsync/internal/store/rdsiam"
)

type tokenRequest struct {
	endpoint string
	user     string
}

type rotatingTokenProvider struct {
	mu       sync.Mutex
	tokens   []string
	requests []tokenRequest
}

type tokenProviderFunc func(
	context.Context,
	string,
	string,
) (string, error)

func (f tokenProviderFunc) Token(
	ctx context.Context,
	endpoint string,
	user string,
) (string, error) {
	return f(ctx, endpoint, user)
}

type formattingPanicTracer struct{}

func (formattingPanicTracer) TraceConnectStart(
	ctx context.Context,
	data pgx.TraceConnectStartData,
) context.Context {
	panic(fmt.Sprintf("connect config: %v", data.ConnConfig))
}

func (formattingPanicTracer) TraceConnectEnd(
	context.Context,
	pgx.TraceConnectEndData,
) {
}

func (formattingPanicTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	return ctx
}

func (formattingPanicTracer) TraceQueryEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceQueryEndData,
) {
}

func (p *rotatingTokenProvider) Token(
	_ context.Context,
	endpoint string,
	user string,
) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, tokenRequest{endpoint: endpoint, user: user})
	index := len(p.requests) - 1
	if index >= len(p.tokens) {
		return "", fmt.Errorf("unexpected token request %d", index+1)
	}
	return p.tokens[index], nil
}

func (p *rotatingTokenProvider) Requests() []tokenRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]tokenRequest(nil), p.requests...)
}

func TestConfigureRDSIAMPreservesConnectionShape(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig(
		"postgres://mirror@db.example.com:5439/ghsync" +
			"?sslmode=verify-full&search_path=tenant&application_name=ghsync",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.TLSConfig == nil {
		t.Fatal("verify-full URL did not produce a TLS configuration")
	}
	runtimeParams := maps.Clone(cfg.ConnConfig.RuntimeParams)
	serverName := cfg.ConnConfig.TLSConfig.ServerName
	insecureSkipVerify := cfg.ConnConfig.TLSConfig.InsecureSkipVerify
	provider := &rotatingTokenProvider{tokens: []string{"fresh-token"}}

	if err := configureRDSIAM(cfg, provider); err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Password != "" {
		t.Fatal("token entered the pool's base connection config")
	}
	connectionConfig := cfg.ConnConfig.Copy()
	if err := cfg.BeforeConnect(context.Background(), connectionConfig); err != nil {
		t.Fatal(err)
	}
	if connectionConfig.Password != "fresh-token" {
		t.Fatalf("connection password = %q, want generated token", connectionConfig.Password)
	}
	if !maps.Equal(connectionConfig.RuntimeParams, runtimeParams) {
		t.Fatalf(
			"runtime params = %#v, want %#v",
			connectionConfig.RuntimeParams,
			runtimeParams,
		)
	}
	if connectionConfig.TLSConfig == nil ||
		connectionConfig.TLSConfig.ServerName != serverName ||
		connectionConfig.TLSConfig.InsecureSkipVerify != insecureSkipVerify {
		t.Fatalf("TLS verification configuration changed: %#v", connectionConfig.TLSConfig)
	}
	requests := provider.Requests()
	if len(requests) != 1 ||
		requests[0].endpoint != "db.example.com:5439" ||
		requests[0].user != "mirror" {
		t.Fatalf("token requests = %#v", requests)
	}
}

func TestRDSIAMPoolReconnectUsesFreshToken(t *testing.T) {
	t.Parallel()
	const (
		staleToken = "stale-token"
		freshToken = "fresh-token"
	)
	address, serverDone := acceptingPostgresServer(
		t,
		[]string{staleToken, freshToken},
	)
	cfg, err := pgxpool.ParseConfig(
		"postgres://mirror@" + address + "/ghsync?sslmode=disable&pool_max_conns=1",
	)
	if err != nil {
		t.Fatal(err)
	}
	provider := &rotatingTokenProvider{tokens: []string{staleToken, freshToken}}
	if err := configureRDSIAM(cfg, provider); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	first, err := pool.Acquire(t.Context())
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	first.Release()
	if requests := provider.Requests(); len(requests) != 1 {
		pool.Close()
		t.Fatalf("initial physical connection made %d token requests, want 1", len(requests))
	}

	pool.Reset()
	second, err := pool.Acquire(t.Context())
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	second.Release()
	pool.Close()
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
	requests := provider.Requests()
	if len(requests) != 2 {
		t.Fatalf("physical reconnect made %d token requests, want 2", len(requests))
	}
}

func TestRDSIAMRejectsPasswordBeforeGeneratingToken(t *testing.T) {
	t.Parallel()
	provider := &rotatingTokenProvider{tokens: []string{"unused-token"}}
	pool, err := Connect(
		context.Background(),
		"postgres://mirror:static-secret@db.example.com:5432/ghsync",
		withRDSIAMTokenProvider(provider),
	)
	if pool != nil {
		pool.Close()
		t.Fatal("password-bearing RDS IAM URL unexpectedly connected")
	}
	if err == nil || !strings.Contains(err.Error(), "password credentials must not be configured") {
		t.Fatalf("password-bearing RDS IAM URL error = %v", err)
	}
	if strings.Contains(err.Error(), "static-secret") {
		t.Fatalf("password-bearing RDS IAM error disclosed password: %v", err)
	}
	multiHostPool, multiHostErr := Connect(
		context.Background(),
		"host=db-one.example.com,db-two.example.com user=mirror dbname=ghsync sslmode=disable",
		withRDSIAMTokenProvider(provider),
	)
	if multiHostPool != nil {
		multiHostPool.Close()
		t.Fatal("multi-endpoint RDS IAM URL unexpectedly connected")
	}
	if multiHostErr == nil || !strings.Contains(multiHostErr.Error(), "one database endpoint") {
		t.Fatalf("multi-endpoint RDS IAM error = %v", multiHostErr)
	}
	if requests := provider.Requests(); len(requests) != 0 {
		t.Fatalf("provider called before URL rejection: %#v", requests)
	}
}

func TestRDSIAMRefreshesTokenAndAuthenticatesMigrationLock(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := testDatabaseConfig(t)
	adminPool, err := Connect(ctx, testDatabaseAdminURL(t, conf))
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	role := fmt.Sprintf("ghsync_iam_%d", time.Now().UnixNano())
	tokens := []string{"iam-token-one", "iam-token-two", "iam-token-three"}
	createTestLoginRole(t, ctx, adminPool, role, tokens[0])
	provider := &rotatingTokenProvider{tokens: tokens}
	poolURL := passwordlessRoleURL(t, conf.URL(), role)
	pool, err := Connect(ctx, poolURL, withRDSIAMTokenProvider(provider))
	if err != nil {
		t.Fatalf("connect IAM pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if pool.Config().ConnConfig.Password != "" {
		t.Fatal("generated token persisted in pool configuration")
	}

	var applicationName, searchPath, synchronousCommit, idleTimeout string
	if err := pool.QueryRow(ctx, "SHOW application_name").Scan(&applicationName); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SHOW search_path").Scan(&searchPath); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SHOW synchronous_commit").
		Scan(&synchronousCommit); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SHOW idle_in_transaction_session_timeout").
		Scan(&idleTimeout); err != nil {
		t.Fatal(err)
	}
	if applicationName != "ghsync-rds-iam-test" ||
		searchPath != "public" ||
		synchronousCommit != "on" ||
		idleTimeout != "30s" {
		t.Fatalf(
			"runtime parameters = application_name %q, search_path %q, synchronous_commit %q, idle timeout %q",
			applicationName,
			searchPath,
			synchronousCommit,
			idleTimeout,
		)
	}
	if requests := provider.Requests(); len(requests) != 1 {
		t.Fatalf(
			"reusing one physical connection made %d token requests, want 1",
			len(requests),
		)
	}
	pooledConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if password := pooledConn.Conn().Config().Password; password != "" {
		pooledConn.Release()
		t.Fatalf("successful connection retained generated token %q", password)
	}
	pooledConn.Release()

	alterTestLoginRolePassword(t, ctx, adminPool, role, tokens[1])
	pool.Reset()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("replace connection with refreshed token: %v", err)
	}

	alterTestLoginRolePassword(t, ctx, adminPool, role, tokens[2])
	pool.Reset()
	lockConn, err := acquireMigrationLock(ctx, pool)
	if err != nil {
		t.Fatalf("acquire IAM-authenticated migration lock: %v", err)
	}
	checker, err := adminPool.Acquire(ctx)
	if err != nil {
		_ = lockConn.Close(context.WithoutCancel(ctx))
		t.Fatal(err)
	}
	defer checker.Release()
	var acquired bool
	if err := checker.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtextextended('ghsync_schema_migrations', 0))`).
		Scan(&acquired); err != nil {
		_ = lockConn.Close(context.WithoutCancel(ctx))
		t.Fatal(err)
	}
	if acquired {
		_ = lockConn.Close(context.WithoutCancel(ctx))
		t.Fatal("migration advisory lock was not held by hijacked connection")
	}
	if err := lockConn.Close(ctx); err != nil {
		t.Fatalf("close migration lock connection: %v", err)
	}
	// Session-lock release is asynchronous relative to Close returning;
	// poll on the pinned checker session with a generous bound.
	assertMigrationLockAvailableOn(t, ctx, checker)

	requests := provider.Requests()
	if len(requests) != len(tokens) {
		t.Fatalf("token requests = %d, want %d", len(requests), len(tokens))
	}
	for _, request := range requests {
		if request.endpoint != conf.Host+":"+conf.Port || request.user != role {
			t.Fatalf("token request = %#v", request)
		}
	}
}

func TestRDSIAMMigrateAuthenticatesLockAndPoolConnections(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conf := testDatabaseConfig(t)
	adminPool, err := Connect(ctx, testDatabaseAdminURL(t, conf))
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	suffix := time.Now().UnixNano()
	role := fmt.Sprintf("ghsync_iam_migrate_role_%d", suffix)
	database := fmt.Sprintf("ghsync_iam_migrate_db_%d", suffix)
	createTestLoginRole(t, ctx, adminPool, role, "initial-token")
	createTestDatabaseOwnedByRole(t, ctx, adminPool, database, role)

	provider := &rotatingTokenProvider{
		tokens: []string{"initial-token", "rotated-token", "rotated-token"},
	}
	parsed, err := url.Parse(conf.URL())
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	parsed.User = url.User(role)
	query := parsed.Query()
	query.Set("pool_max_conns", "1")
	parsed.RawQuery = query.Encode()
	pool, err := Connect(
		ctx,
		parsed.String(),
		withRDSIAMTokenProvider(provider),
	)
	if err != nil {
		t.Fatalf("connect IAM migration pool: %v", err)
	}
	t.Cleanup(pool.Close)

	alterTestLoginRolePassword(t, ctx, adminPool, role, "rotated-token")
	pool.Reset()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate through IAM pool: %v", err)
	}
	var migrationCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").
		Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount == 0 {
		t.Fatal("IAM migration path applied no ghsync migrations")
	}
	requests := provider.Requests()
	if len(requests) != 3 {
		t.Fatalf(
			"token requests = %d, want initial, lock, and replacement pool connections",
			len(requests),
		)
	}
}

func TestRDSIAMBadTokenDoesNotDiscloseToken(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const generatedToken = "generated-token-MUST-NOT-LEAK"
	address, serverDone := rejectingPostgresServer(t, generatedToken)
	provider := &rotatingTokenProvider{tokens: []string{generatedToken}}
	pool, err := Connect(
		ctx,
		"postgres://mirror@"+address+"/ghsync?sslmode=disable",
		withRDSIAMTokenProvider(provider),
	)
	if pool != nil {
		pool.Close()
		t.Fatal("connection with a rejected generated token unexpectedly succeeded")
	}
	if err == nil {
		t.Fatal("connection with a bad generated token returned no error")
	}
	if serverErr := <-serverDone; serverErr != nil {
		t.Fatal(serverErr)
	}
	assertTokenRedacted(t, generatedToken, err)
}

func TestRDSIAMConnectionFailuresDoNotDiscloseToken(t *testing.T) {
	t.Parallel()

	tests := map[string]func(t *testing.T) (context.Context, string, rdsiam.TokenProvider){
		"unreachable host": func(t *testing.T) (context.Context, string, rdsiam.TokenProvider) {
			t.Helper()
			listener, err := (&net.ListenConfig{}).Listen(
				t.Context(),
				"tcp",
				"127.0.0.1:0",
			)
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			return context.Background(),
				"postgres://mirror@" + address + "/ghsync?sslmode=disable&connect_timeout=1",
				&rotatingTokenProvider{tokens: []string{"unreachable-token-MUST-NOT-LEAK"}}
		},
		"canceled context": func(t *testing.T) (context.Context, string, rdsiam.TokenProvider) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			provider := tokenProviderFunc(func(
				context.Context,
				string,
				string,
			) (string, error) {
				cancel()
				return "canceled-token-MUST-NOT-LEAK", nil
			})
			return ctx,
				"postgres://mirror@127.0.0.1:1/ghsync?sslmode=disable",
				provider
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, databaseURL, provider := setup(t)
			pool, err := Connect(ctx, databaseURL, withRDSIAMTokenProvider(provider))
			if pool != nil {
				pool.Close()
				t.Fatal("failing RDS IAM connection returned a pool")
			}
			if err == nil {
				t.Fatal("failing RDS IAM connection returned no error")
			}
			token := strings.Split(name, " ")[0] + "-token-MUST-NOT-LEAK"
			assertTokenRedacted(t, token, err)
		})
	}
}

func TestRDSIAMProviderFailuresAreContained(t *testing.T) {
	t.Parallel()
	const generatedToken = "provider-token-MUST-NOT-LEAK"
	tests := map[string]rdsiam.TokenProvider{
		"error": tokenProviderFunc(func(
			context.Context,
			string,
			string,
		) (string, error) {
			return generatedToken, errors.New("provider rejected " + generatedToken)
		}),
		"panic": tokenProviderFunc(func(
			context.Context,
			string,
			string,
		) (string, error) {
			panic("provider panic " + generatedToken)
		}),
	}
	for name, provider := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, err := Connect(
				context.Background(),
				"postgres://mirror@127.0.0.1:1/ghsync?sslmode=disable",
				withRDSIAMTokenProvider(provider),
			)
			if pool != nil {
				pool.Close()
				t.Fatal("failed provider returned a pool")
			}
			if err == nil {
				t.Fatal("failed provider returned no error")
			}
			assertTokenRedacted(t, generatedToken, err)
		})
	}
}

func TestRDSIAMBeforeConnectConfigFormattingIsRedacted(t *testing.T) {
	t.Parallel()
	const generatedToken = "before-connect-token-MUST-NOT-LEAK"
	cfg, err := pgxpool.ParseConfig(
		"postgres://mirror@db.example.com:5432/ghsync?sslmode=verify-full",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureRDSIAMWithTokenInstalledHook(
		cfg,
		&rotatingTokenProvider{tokens: []string{generatedToken}},
		func(connConfig *pgx.ConnConfig) error {
			return fmt.Errorf("connect setup failed: %v", connConfig)
		},
	); err != nil {
		t.Fatal(err)
	}
	connConfig := cfg.ConnConfig.Copy()
	err = cfg.BeforeConnect(context.Background(), connConfig)
	if err == nil {
		t.Fatal("naively formatted connection config returned no error")
	}
	assertTokenRedacted(t, generatedToken, err)
	if connConfig.Password != "" {
		t.Fatal("failed BeforeConnect retained generated token in connection config")
	}
}

func TestRDSIAMBeforeConnectPanicIsRedacted(t *testing.T) {
	t.Parallel()
	const generatedToken = "before-connect-panic-token-MUST-NOT-LEAK"
	cfg, err := pgxpool.ParseConfig(
		"postgres://mirror@db.example.com:5432/ghsync?sslmode=verify-full",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureRDSIAMWithTokenInstalledHook(
		cfg,
		&rotatingTokenProvider{tokens: []string{generatedToken}},
		func(connConfig *pgx.ConnConfig) error {
			panic(fmt.Sprintf("connect setup panic: %v", connConfig))
		},
	); err != nil {
		t.Fatal(err)
	}
	connConfig := cfg.ConnConfig.Copy()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = cfg.BeforeConnect(context.Background(), connConfig)
	}()
	if recovered == nil {
		t.Fatal("naively formatted connection config did not panic")
	}
	if text := fmt.Sprint(recovered); strings.Contains(text, generatedToken) {
		t.Fatalf("BeforeConnect panic disclosed generated token: %s", text)
	}
	if connConfig.Password != "" {
		t.Fatal("panicked BeforeConnect retained generated token in connection config")
	}
}

func TestRDSIAMTracerPanicCannotDiscloseToken(t *testing.T) {
	t.Parallel()
	const generatedToken = "tracer-token-MUST-NOT-LEAK"
	cfg, err := pgxpool.ParseConfig(
		"postgres://mirror@127.0.0.1:1/ghsync?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = formattingPanicTracer{}
	if err := configureRDSIAM(
		cfg,
		&rotatingTokenProvider{tokens: []string{generatedToken}},
	); err != nil {
		t.Fatal(err)
	}
	connConfig := cfg.ConnConfig.Copy()
	if err := cfg.BeforeConnect(context.Background(), connConfig); err != nil {
		t.Fatal(err)
	}
	tracer, ok := cfg.ConnConfig.Tracer.(pgx.ConnectTracer)
	if !ok {
		t.Fatal("RDS IAM connection tracer does not implement pgx.ConnectTracer")
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = tracer.TraceConnectStart(
			context.Background(),
			pgx.TraceConnectStartData{ConnConfig: connConfig},
		)
	}()
	if recovered == nil {
		t.Fatal("formatting tracer did not panic")
	}
	if text := fmt.Sprint(recovered); strings.Contains(text, generatedToken) {
		t.Fatalf("panic disclosed generated token: %s", text)
	}
	if connConfig.Password != "" {
		t.Fatal("tracer panic retained generated token in connection config")
	}
}

func assertTokenRedacted(t *testing.T, generatedToken string, err error) {
	t.Helper()

	var logOutput bytes.Buffer
	slog.New(slog.NewTextHandler(&logOutput, nil)).Error(
		"database connection failed",
		"error",
		err,
	)
	for name, text := range map[string]string{
		"error":      err.Error(),
		"formatted":  fmt.Sprintf("%+v", err),
		"go-syntax":  fmt.Sprintf("%#v", err),
		"structured": logOutput.String(),
	} {
		if strings.Contains(text, generatedToken) {
			t.Fatalf("%s disclosed generated token: %s", name, text)
		}
	}
	var connectError *pgconn.ConnectError
	if errors.As(err, &connectError) {
		if connectError.Config == nil {
			t.Fatal("pgconn.ConnectError had no config")
		}
		if connectError.Config.Password != "" {
			t.Fatalf(
				"pgconn.ConnectError retained generated token in config: %v",
				connectError.Config,
			)
		}
		if text := fmt.Sprintf("%v", connectError.Config); strings.Contains(text, generatedToken) {
			t.Fatalf("formatted pgconn config disclosed generated token: %s", text)
		}
	}
}

func acceptingPostgresServer(
	t *testing.T,
	expectedTokens []string,
) (string, <-chan error) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(
		t.Context(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		for _, expectedToken := range expectedTokens {
			conn, err := listener.Accept()
			if err != nil {
				done <- fmt.Errorf("accept fake Postgres connection: %w", err)
				return
			}
			backend := pgproto3.NewBackend(conn, conn)
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				_ = conn.Close()
				done <- fmt.Errorf("receive fake Postgres startup: %w", err)
				return
			}
			backend.Send(&pgproto3.AuthenticationCleartextPassword{})
			if err := backend.Flush(); err != nil {
				_ = conn.Close()
				done <- fmt.Errorf("request fake Postgres password: %w", err)
				return
			}
			message, err := backend.Receive()
			if err != nil {
				_ = conn.Close()
				done <- fmt.Errorf("receive fake Postgres password: %w", err)
				return
			}
			password, ok := message.(*pgproto3.PasswordMessage)
			if !ok || password.Password != expectedToken {
				_ = conn.Close()
				done <- fmt.Errorf("fake Postgres received a stale or unexpected token")
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ParameterStatus{
				Name:  "server_version",
				Value: "18.0",
			})
			backend.Send(&pgproto3.BackendKeyData{
				ProcessID: 1,
				SecretKey: []byte{0, 0, 0, 1},
			})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				_ = conn.Close()
				done <- fmt.Errorf("complete fake Postgres authentication: %w", err)
				return
			}
			if _, err := backend.Receive(); err != nil {
				_ = conn.Close()
				done <- fmt.Errorf("receive fake Postgres termination: %w", err)
				return
			}
			if err := conn.Close(); err != nil {
				done <- fmt.Errorf("close fake Postgres connection: %w", err)
				return
			}
		}
		done <- nil
	}()
	return listener.Addr().String(), done
}

func rejectingPostgresServer(
	t *testing.T,
	expectedToken string,
) (string, <-chan error) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(
		t.Context(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- fmt.Errorf("accept fake Postgres connection: %w", err)
			return
		}
		defer conn.Close() //nolint:errcheck // fake server connection cleanup
		backend := pgproto3.NewBackend(conn, conn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			done <- fmt.Errorf("receive fake Postgres startup: %w", err)
			return
		}
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := backend.Flush(); err != nil {
			done <- fmt.Errorf("request fake Postgres password: %w", err)
			return
		}
		message, err := backend.Receive()
		if err != nil {
			done <- fmt.Errorf("receive fake Postgres password: %w", err)
			return
		}
		password, ok := message.(*pgproto3.PasswordMessage)
		if !ok || password.Password != expectedToken {
			done <- fmt.Errorf("fake Postgres did not receive the generated token")
			return
		}
		backend.Send(&pgproto3.ErrorResponse{
			Severity: "FATAL",
			Code:     "28P01",
			Message:  "password authentication failed",
		})
		if err := backend.Flush(); err != nil {
			done <- fmt.Errorf("reject fake Postgres password: %w", err)
			return
		}
		done <- nil
	}()
	return listener.Addr().String(), done
}

func createTestLoginRole(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	role string,
	password string,
) {
	t.Helper()
	var statement string
	if err := pool.QueryRow(ctx,
		`SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', $1::text, $2::text)`,
		role,
		password,
	).Scan(&statement); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		var dropStatement string
		if err := pool.QueryRow(cleanupCtx,
			`SELECT format('DROP ROLE IF EXISTS %I', $1::text)`, role,
		).Scan(&dropStatement); err != nil {
			t.Errorf("format test role cleanup: %v", err)
			return
		}
		if _, err := pool.Exec(cleanupCtx, dropStatement); err != nil {
			t.Errorf("drop test role: %v", err)
		}
	})
}

func createTestDatabaseOwnedByRole(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	database string,
	role string,
) {
	t.Helper()
	var statement string
	if err := pool.QueryRow(ctx,
		`SELECT format('CREATE DATABASE %I OWNER %I', $1::text, $2::text)`,
		database,
		role,
	).Scan(&statement); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		if _, err := pool.Exec(cleanupCtx,
			`SELECT pg_terminate_backend(pid)
			 FROM pg_stat_activity
			 WHERE datname = $1
			   AND pid <> pg_backend_pid()`,
			database,
		); err != nil {
			t.Errorf("terminate test database connections: %v", err)
			return
		}
		var dropStatement string
		if err := pool.QueryRow(cleanupCtx,
			`SELECT format('DROP DATABASE IF EXISTS %I', $1::text)`,
			database,
		).Scan(&dropStatement); err != nil {
			t.Errorf("format test database cleanup: %v", err)
			return
		}
		if _, err := pool.Exec(cleanupCtx, dropStatement); err != nil {
			t.Errorf("drop test database: %v", err)
		}
	})
}

func alterTestLoginRolePassword(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	role string,
	password string,
) {
	t.Helper()
	var statement string
	if err := pool.QueryRow(ctx,
		`SELECT format('ALTER ROLE %I PASSWORD %L', $1::text, $2::text)`,
		role,
		password,
	).Scan(&statement); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatal(err)
	}
}

func passwordlessRoleURL(t *testing.T, databaseURL, role string) string {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.User(role)
	query := parsed.Query()
	query.Set("application_name", "ghsync-rds-iam-test")
	query.Set("search_path", "public")
	query.Set("pool_max_conns", "1")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func testDatabaseAdminURL(t *testing.T, conf *pgtestdb.Config) string {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + conf.Database
	query := parsed.Query()
	query.Del("pool_max_conns")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
