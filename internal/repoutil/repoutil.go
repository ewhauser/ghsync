// Package repoutil contains small repository-sync helpers shared by fetch,
// sweep, and drift.
package repoutil

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/ewhauser/ghsync/internal/gh"
)

// Split separates an owner/name repository identifier.
func Split(fullName string) (string, string, error) {
	owner, name, ok := strings.Cut(fullName, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("invalid repository name %q", fullName)
	}
	return owner, name, nil
}

// IsNotFound reports whether err is GitHub's HTTP 404 response.
func IsNotFound(err error) bool {
	var httpError *gh.HTTPError
	return errors.As(err, &httpError) &&
		httpError.StatusCode == http.StatusNotFound
}

// Timestamptz converts a required timestamp to sqlc's pgtype representation.
func Timestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}
