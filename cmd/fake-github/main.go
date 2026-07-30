// fake-github runs the scriptable GitHub stand-in as a standalone server for
// local development (docker-compose points ghsyncd at it).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ewhauser/ghsync/internal/fakegithub"
	"github.com/ewhauser/ghsync/internal/replay"
)

func main() {
	addr := flag.String("addr", ":9797", "listen address")
	secret := flag.String("webhook-secret", "dev-secret", "HMAC secret for emitted webhooks")
	recordingPath := flag.String(
		"recording",
		"",
		"initialize repository identities from a replay recording",
	)
	copies := flag.Int(
		"copies",
		1,
		"recording repository namespaces to expose before backfill",
	)
	appToken := flag.String(
		"app-token",
		"",
		"static bearer accepted by the development deliveries API",
	)
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

	fixtures, err := recordingFixtures(*recordingPath, *copies)
	if err != nil {
		slog.Error("fake-github recording setup failed", "error", err)
		os.Exit(1)
	}
	options := make([]fakegithub.Option, 0, 2)
	if len(fixtures) > 1 {
		options = append(
			options,
			fakegithub.WithAdditionalFixtures(fixtures[1:]...),
		)
	}
	if *appToken != "" {
		options = append(options, fakegithub.WithAppBearerToken(*appToken))
	}
	fixture := fakegithub.DefaultFixture()
	if len(fixtures) > 0 {
		fixture = fixtures[0]
	}
	srv := fakegithub.New(fixture, *secret, options...)
	slog.Info("fake-github listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, srv); err != nil {
		slog.Error("fake-github failed", "error", err)
		os.Exit(1)
	}
}

func recordingFixtures(path string, copies int) ([]fakegithub.Fixture, error) {
	if path == "" {
		if copies != 1 {
			return nil, fmt.Errorf("--copies requires --recording")
		}
		return nil, nil
	}
	if copies <= 0 {
		return nil, fmt.Errorf("--copies must be positive")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open recording: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	recording, err := replay.Read(file)
	if err != nil {
		return nil, err
	}
	steps, err := replay.FirstLap(recording, replay.CompileOptions{
		Copies: copies,
	})
	if err != nil {
		return nil, err
	}
	byName := make(map[string]fakegithub.Fixture, copies)
	for index := range steps {
		step := &steps[index]
		repository := step.Mutation.Repository
		fullName := repository.FullName()
		if _, exists := byName[fullName]; exists {
			continue
		}
		wire := fakegithub.Repository{
			ID:               repository.ID,
			NodeID:           repository.NodeID,
			Owner:            repository.Owner,
			Name:             repository.Name,
			FullName:         fullName,
			DefaultBranch:    repository.DefaultBranch,
			DefaultBranchSHA: repository.DefaultBranchSHA,
			UpdatedAt:        repository.UpdatedAt,
			PushedAt:         repository.UpdatedAt,
		}
		byName[fullName] = fakegithub.EmptyFixture(wire)
	}
	fixtures := make([]fakegithub.Fixture, 0, len(byName))
	for index := range steps {
		step := &steps[index]
		fullName := step.Mutation.Repository.FullName()
		fixture, exists := byName[fullName]
		if !exists {
			continue
		}
		fixtures = append(fixtures, fixture)
		delete(byName, fullName)
	}
	return fixtures, nil
}
