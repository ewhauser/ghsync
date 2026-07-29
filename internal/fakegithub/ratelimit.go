package fakegithub

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type rateState struct {
	limit     int64
	remaining int64
	resetAt   time.Time
	resource  string
}

func (s *Server) consume(resource string, cost int64) (rateState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.resetIfExpired(time.Now())
	if budget.remaining < cost {
		return budget.snapshot(), false
	}
	budget.remaining -= cost
	return budget.snapshot(), true
}

type rateOutcome struct {
	snapshot   rateState
	scripted   bool
	allowed    bool
	status     int
	retryAfter time.Duration
	secondary  bool
	cost       int64
}

func (s *Server) nextRate(resource string, defaultCost int64) rateOutcome {
	s.mu.Lock()
	var steps []RateLimitStep
	var index *int
	if resource == "graphql" {
		steps = s.graphQLSteps
		index = &s.graphQLStep
	} else {
		steps = s.restSteps
		index = &s.restStep
	}
	if *index < len(steps) {
		step := steps[*index]
		*index++
		cost := step.Cost
		if cost <= 0 {
			cost = defaultCost
		}
		budget := s.budget(resource)
		budget.resetIfExpired(time.Now())
		if step.Limit <= 0 {
			step.Limit = budget.limit
		}
		if step.ResetAt.IsZero() {
			step.ResetAt = budget.resetAt
		}
		budget.limit = step.Limit
		budget.remaining = step.Remaining
		budget.resetAt = step.ResetAt
		snapshot := budget.snapshot()
		s.mu.Unlock()
		return rateOutcome{
			snapshot:   snapshot,
			scripted:   true,
			allowed:    true,
			status:     step.StatusCode,
			retryAfter: step.RetryAfter,
			secondary:  step.Secondary,
			cost:       cost,
		}
	}
	s.mu.Unlock()
	snapshot, allowed := s.consume(resource, defaultCost)
	return rateOutcome{
		snapshot: snapshot,
		allowed:  allowed,
		cost:     defaultCost,
	}
}

func (s *Server) snapshot(resource string) rateState {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.resetIfExpired(time.Now())
	return budget.snapshot()
}

func (s *Server) budget(resource string) *rateState {
	if resource == "graphql" {
		return &s.graphQLBudget
	}
	return &s.restBudget
}

func (b *rateState) resetIfExpired(now time.Time) {
	if now.Before(b.resetAt) {
		return
	}
	b.remaining = b.limit
	b.resetAt = nextRateReset(now)
}

func (b *rateState) snapshot() rateState {
	return *b
}

func nextRateReset(now time.Time) time.Time {
	return now.UTC().Truncate(time.Hour).Add(time.Hour)
}

func (s *Server) beginRequest(path string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	if strings.HasPrefix(path, "/app/installations/") {
		s.tokenMaxActive = max(s.tokenMaxActive, s.active)
	}
	return s.responseDelay
}

func (s *Server) endRequest() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
}

type scriptedRateKey struct{}
type rateCostKey struct{}

func (s *Server) writeConditionalJSON(
	w http.ResponseWriter,
	r *http.Request,
	value any,
) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sum := sha256.Sum256(body)
	etag := fmt.Sprintf(`"%x"`, sum[:16])
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		s.mu.Lock()
		s.notModified[r.Method+" "+r.URL.Path]++
		s.mu.Unlock()
		if scripted, _ := r.Context().Value(scriptedRateKey{}).(bool); !scripted {
			rate := s.refund("core", 1)
			setRateHeaders(w.Header(), rate)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	body = append(body, '\n')
	_, _ = w.Write(body)
}

func (s *Server) refund(resource string, cost int64) rateState {
	s.mu.Lock()
	defer s.mu.Unlock()
	budget := s.budget(resource)
	budget.remaining = min(budget.remaining+cost, budget.limit)
	return budget.snapshot()
}

func setRateHeaders(header http.Header, budget rateState) {
	header.Set("X-RateLimit-Limit", strconv.FormatInt(budget.limit, 10))
	header.Set("X-RateLimit-Remaining", strconv.FormatInt(budget.remaining, 10))
	header.Set("X-RateLimit-Reset", strconv.FormatInt(budget.resetAt.Unix(), 10))
	header.Set("X-RateLimit-Resource", budget.resource)
	header.Set("X-RateLimit-Used", strconv.FormatInt(budget.limit-budget.remaining, 10))
}

func writeRESTRateLimitExceeded(
	w http.ResponseWriter,
	status int,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"message": "API rate limit exceeded for installation.",
		"documentation_url": "https://docs.github.com/rest/using-the-rest-api/" +
			"rate-limits-for-the-rest-api",
		"status": strconv.Itoa(status),
	}); err != nil {
		return
	}
}

func writeGraphQLRateLimitExceeded(
	w http.ResponseWriter,
	budget rateState,
	status int,
	cost int64,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": map[string]any{
			"rateLimit": map[string]any{
				"cost":      cost,
				"limit":     budget.limit,
				"remaining": budget.remaining,
				"resetAt":   budget.resetAt.Format(time.RFC3339),
				"used":      budget.limit - budget.remaining,
			},
		},
		"errors": []map[string]any{
			{
				"type":    "RATE_LIMITED",
				"message": "API rate limit exceeded for this GraphQL resource.",
			},
		},
	})
}

func writeSecondaryRateLimitExceeded(
	w http.ResponseWriter,
	status int,
	resource string,
	budget rateState,
	cost int64,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if resource == "graphql" {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"rateLimit": map[string]any{
					"cost":      cost,
					"limit":     budget.limit,
					"remaining": budget.remaining,
					"resetAt":   budget.resetAt.Format(time.RFC3339),
				},
			},
			"errors": []map[string]any{{
				"type":    "RATE_LIMITED",
				"message": "You have exceeded a secondary rate limit.",
			}},
		})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message": "You have exceeded a secondary rate limit. Please wait " +
			"a few minutes before you try again.",
		"documentation_url": "https://docs.github.com/rest/using-the-rest-api/" +
			"rate-limits-for-the-rest-api#about-secondary-rate-limits",
		"status": strconv.Itoa(status),
	})
}
