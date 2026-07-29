// fake-github runs the scriptable GitHub stand-in as a standalone server for
// local development (docker-compose points ghsyncd at it).
package main

import (
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
		resp, err := client.Get(*healthcheckURL)
		if err != nil {
			slog.Error("fake-github healthcheck failed", "error", err)
			os.Exit(1)
		}
		resp.Body.Close()
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
