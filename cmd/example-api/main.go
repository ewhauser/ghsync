package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ewhauser/ghsync/internal/config"
	"github.com/ewhauser/ghsync/internal/store"
	"github.com/ewhauser/ghsync/pkg/streamclient"
)

const shutdownTimeout = 10 * time.Second

func main() {
	err := run(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "example-api:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args, os.Getenv, os.Stderr)
	if err != nil {
		return fmt.Errorf("parse configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()
	return runContext(ctx, &cfg)
}

func runContext(ctx context.Context, cfg *apiConfig) error {
	var connectOptions []store.ConnectOption
	if cfg.databaseAuth == config.DatabaseAuthRDSIAM {
		connectOptions = append(
			connectOptions,
			store.WithRDSIAMAuthentication(),
		)
	}
	pool, err := store.Connect(ctx, cfg.databaseURL, connectOptions...)
	if err != nil {
		return fmt.Errorf("connect to Postgres: %w", err)
	}
	defer pool.Close()
	stream, err := streamclient.New(pool, streamclient.Config{})
	if err != nil {
		return fmt.Errorf("create stream client: %w", err)
	}
	hub := newEventHub(cfg.ringSize, cfg.subscriberBuffer)
	tailer := newEntityTailer(stream, hub, cfg.consumerName)
	tailerCtx, stopTailer := context.WithCancel(ctx)
	defer stopTailer()
	if err := tailer.start(tailerCtx); err != nil {
		// A shutdown can race the Bootstrap COMMIT acknowledgement after
		// PostgreSQL has made the cursor visible. Startup cancellation is a
		// requested stop, not an operational tailer failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("start entity tailer: %w", err)
	}

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", cfg.addr)
	if err != nil {
		stopTailer()
		waitForTailer(tailer.done(), shutdownTimeout)
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("listen on %s: %w", cfg.addr, err)
	}
	api := newAPIServer(pool, hub, tailer, cfg.replayLimit)
	serverCtx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	httpServer := &http.Server{
		Addr:              cfg.addr,
		Handler:           api.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return serverCtx
		},
	}
	httpErr := make(chan error, 1)
	go func() {
		httpErr <- httpServer.Serve(listener)
	}()
	slog.Info(
		"example API listening",
		"addr", cfg.addr,
		"consumer", cfg.consumerName,
	)

	var result error
	tailerStopped := false
	select {
	case <-ctx.Done():
	case err := <-tailer.done():
		tailerStopped = true
		if err != nil {
			result = fmt.Errorf("entity tailer: %w", err)
		}
	case err := <-httpErr:
		if !errors.Is(err, http.ErrServerClosed) {
			result = fmt.Errorf("serve HTTP: %w", err)
		}
	}

	if err := listener.Close(); err != nil &&
		!errors.Is(err, net.ErrClosed) && result == nil {
		result = fmt.Errorf("stop accepting HTTP connections: %w", err)
	}
	stopServer()
	hub.close()
	stopTailer()
	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		shutdownTimeout,
	)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && result == nil {
		result = fmt.Errorf("shut down HTTP server: %w", err)
	}
	if !tailerStopped {
		select {
		case err := <-tailer.done():
			if err != nil && result == nil {
				result = fmt.Errorf("stop entity tailer: %w", err)
			}
		case <-shutdownCtx.Done():
			if result == nil {
				result = fmt.Errorf("stop entity tailer: %w", shutdownCtx.Err())
			}
		}
	}
	return result
}

func waitForTailer(done <-chan error, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
