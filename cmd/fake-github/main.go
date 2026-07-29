// fake-github runs the scriptable GitHub stand-in as a standalone server for
// local development (docker-compose points ghsyncd at it).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ewhauser/ghsync/internal/fakegithub"
)

func main() {
	addr := flag.String("addr", ":9797", "listen address")
	secret := flag.String("webhook-secret", "dev-secret", "HMAC secret for emitted webhooks")
	healthcheckURL := flag.String(
		"healthcheck-url",
		"",
		"check a running fake-github health endpoint and exit",
	)
	flag.Parse()

	if *healthcheckURL != "" {
		client := &http.Client{Timeout: time.Second}
		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			*healthcheckURL,
			http.NoBody,
		)
		if err != nil {
			slog.Error("invalid fake-github healthcheck URL", "error", err)
			os.Exit(1)
		}
		resp, err := client.Do(req)
		if err != nil {
			slog.Error("fake-github healthcheck failed", "error", err)
			os.Exit(1)
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			slog.Error("fake-github healthcheck failed", "status", resp.StatusCode)
			os.Exit(1)
		}
		return
	}

	srv := fakegithub.New(fakegithub.DefaultFixture(), *secret)
	slog.Info("fake-github listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, srv); err != nil {
		slog.Error("fake-github failed", "error", err)
		os.Exit(1)
	}
}
