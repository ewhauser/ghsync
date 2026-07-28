// fake-github runs the scriptable GitHub stand-in as a standalone server for
// local development (docker-compose points frontier-syncd at it).
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"

	"github.com/acme/frontier/internal/fakegithub"
)

func main() {
	addr := flag.String("addr", ":9797", "listen address")
	secret := flag.String("webhook-secret", "dev-secret", "HMAC secret for emitted webhooks")
	flag.Parse()

	srv := fakegithub.New(fakegithub.DefaultFixture(), *secret)
	slog.Info("fake-github listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, srv); err != nil {
		slog.Error("fake-github failed", "error", err)
		os.Exit(1)
	}
}
