package budget

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultConcurrency = 40
	sweepFloor         = 0.20
	eventFloor         = 0.10
)

// Options configures a Gate. Limits are initial floor denominators only; an
// observed server limit always replaces them. Remaining values are never
// locally decremented (C-B2).
type Options struct {
	MaxConcurrent int
	RESTLimit     int64
	GraphQLLimit  int64
	OnStarvation  StarvationHook
}

// Gate is the C-B1 per-installation choke point. It owns admission,
// server-authoritative REST and GraphQL observations, global secondary-limit
// backoff, and the C-B6 concurrency ceiling.
type Gate struct {
	client *http.Client

	mu            sync.Mutex
	changed       chan struct{}
	inFlight      int
	maxConcurrent int
	rest          ResourceBudget
	graphql       ResourceBudget
	backoffUntil  time.Time
	unavailable   error
	onStarvation  StarvationHook

	lease *leaseRuntime
}

// New constructs an in-process gate. Production callers should use
// NewLeased; New exists for conformance tests and single-process tooling.
func New(client *http.Client, options Options) *Gate {
	if client == nil {
		client = http.DefaultClient
	}
	ownedClient := *client
	// Redirect follow-ups would be unadmitted HTTP calls hidden inside one
	// Client.Do, so C-B1 rejects redirects and lets callers handle them.
	ownedClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = defaultConcurrency
	}
	if options.RESTLimit <= 0 {
		options.RESTLimit = 15000
	}
	if options.GraphQLLimit <= 0 {
		options.GraphQLLimit = 5000
	}
	return &Gate{
		client:        &ownedClient,
		changed:       make(chan struct{}),
		maxConcurrent: options.MaxConcurrent,
		rest:          ResourceBudget{Limit: options.RESTLimit},
		graphql:       ResourceBudget{Limit: options.GraphQLLimit},
		onStarvation:  options.OnStarvation,
	}
}

// Do admits and performs exactly one GitHub request (C-B1). REST state comes
// only from x-ratelimit-* headers; GraphQL state comes only from the supplied
// rateLimit observer (C-B2/C-B5).
func (g *Gate) Do(ctx context.Context, class Class, req *Request) (*Response, error) {
	if ctx == nil {
		return nil, fmt.Errorf("budget gate: nil context")
	}
	if !class.valid() {
		return nil, fmt.Errorf("budget gate: invalid class %q", class)
	}
	if req == nil || req.httpRequest == nil {
		return nil, fmt.Errorf("budget gate: nil request")
	}
	if !req.resource.valid() {
		return nil, fmt.Errorf("budget gate: invalid resource %q", req.resource)
	}
	if req.resource == GraphQL && req.observeRate == nil {
		return nil, fmt.Errorf("budget gate: GraphQL request has no rate observer")
	}

	reportedStarvation := false
	for {
		wait, waitUntil, changed, starvation, err := g.tryAdmit(class, req.resource)
		if err != nil {
			return nil, err
		}
		if !wait {
			break
		}
		if starvation != nil && !reportedStarvation {
			reportedStarvation = true
			if g.onStarvation != nil {
				g.onStarvation(*starvation)
			}
		}
		if err := waitForChange(ctx, changed, waitUntil); err != nil {
			return nil, err
		}
	}

	httpReq := req.httpRequest.Clone(ctx)
	resp, requestErr := g.client.Do(httpReq)
	var graphQLRate *GraphQLRate
	var observeErr error
	if resp != nil {
		g.observeSecondaryLimit(resp)
		switch req.resource {
		case REST:
			observeErr = g.observeREST(resp.Header)
		case GraphQL:
			var rate GraphQLRate
			var ok bool
			rate, ok, observeErr = req.observeRate(resp)
			if ok {
				graphQLRate = &rate
				g.observeGraphQL(rate)
			}
		}
	}
	g.release()

	result := &Response{HTTP: resp, GraphQLRate: graphQLRate}
	if requestErr != nil {
		return result, requestErr
	}
	if observeErr != nil {
		return result, observeErr
	}
	return result, nil
}

func (g *Gate) tryAdmit(
	class Class,
	resource Resource,
) (bool, time.Time, <-chan struct{}, *Starvation, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unavailable != nil {
		return false, time.Time{}, nil, nil, g.unavailable
	}
	now := time.Now()
	if now.Before(g.backoffUntil) {
		return true, g.backoffUntil, g.changed, nil, nil
	}
	if g.inFlight >= g.maxConcurrent {
		// A zero deadline means "wait for a state-change notification".
		return true, time.Time{}, g.changed, nil, nil
	}
	if resource != Auth {
		state := g.resourceLocked(resource)
		if state.Known && now.Before(state.ResetAt) {
			if state.Remaining <= 0 {
				return true, state.ResetAt, g.changed, nil, nil
			}
			floor := floorFor(class)
			if floor > 0 && float64(state.Remaining) <= float64(state.Limit)*floor {
				return true, state.ResetAt, g.changed, &Starvation{
					Class:     class,
					Resource:  resource,
					Remaining: state.Remaining,
					Limit:     state.Limit,
					ResetAt:   state.ResetAt,
				}, nil
			}
		}
	}
	g.inFlight++
	return false, time.Time{}, nil, nil, nil
}

func waitForChange(ctx context.Context, changed <-chan struct{}, until time.Time) error {
	if until.IsZero() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			return nil
		}
	}
	delay := time.Until(until)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer.C:
		return nil
	}
}

func (g *Gate) release() {
	g.mu.Lock()
	g.inFlight--
	g.signalLocked()
	g.mu.Unlock()
}

func (g *Gate) observeREST(header http.Header) error {
	rawLimit := header.Get("X-RateLimit-Limit")
	rawRemaining := header.Get("X-RateLimit-Remaining")
	rawReset := header.Get("X-RateLimit-Reset")
	if rawLimit == "" && rawRemaining == "" && rawReset == "" {
		return nil
	}
	if rawLimit == "" || rawRemaining == "" || rawReset == "" {
		return fmt.Errorf("budget gate: incomplete REST rate-limit headers")
	}
	limit, err := strconv.ParseInt(rawLimit, 10, 64)
	if err != nil || limit <= 0 {
		return fmt.Errorf("budget gate: invalid X-RateLimit-Limit %q", rawLimit)
	}
	remaining, err := strconv.ParseInt(rawRemaining, 10, 64)
	if err != nil || remaining < 0 {
		return fmt.Errorf("budget gate: invalid X-RateLimit-Remaining %q", rawRemaining)
	}
	resetUnix, err := strconv.ParseInt(rawReset, 10, 64)
	if err != nil || resetUnix <= 0 {
		return fmt.Errorf("budget gate: invalid X-RateLimit-Reset %q", rawReset)
	}

	g.mu.Lock()
	g.rest = ResourceBudget{
		Known:     true,
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   time.Unix(resetUnix, 0),
	}
	g.signalLocked()
	g.mu.Unlock()
	return nil
}

func (g *Gate) observeGraphQL(rate GraphQLRate) {
	if rate.Limit <= 0 || rate.Remaining < 0 || rate.ResetAt.IsZero() {
		return
	}
	g.mu.Lock()
	g.graphql = ResourceBudget{
		Known:     true,
		Limit:     rate.Limit,
		Remaining: rate.Remaining,
		ResetAt:   rate.ResetAt,
	}
	g.signalLocked()
	g.mu.Unlock()
}

func (g *Gate) observeSecondaryLimit(resp *http.Response) {
	if resp.StatusCode != http.StatusForbidden &&
		resp.StatusCode != http.StatusTooManyRequests {
		return
	}
	raw := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if raw == "" {
		return
	}
	now := time.Now()
	until, err := retryAfterDeadline(now, raw)
	if err != nil {
		return
	}
	g.mu.Lock()
	// A shorter overlapping response must not reopen an already-closed gate.
	if until.After(g.backoffUntil) {
		g.backoffUntil = until
		g.signalLocked()
	}
	g.mu.Unlock()
}

func retryAfterDeadline(now time.Time, raw string) (time.Time, error) {
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds < 0 {
			return time.Time{}, fmt.Errorf("negative Retry-After")
		}
		return now.Add(time.Duration(seconds) * time.Second), nil
	}
	when, err := http.ParseTime(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid Retry-After %q: %w", raw, err)
	}
	return when, nil
}

func floorFor(class Class) float64 {
	switch class {
	case Sweep:
		return sweepFloor
	case Event:
		return eventFloor
	default:
		return 0
	}
}

func (g *Gate) resourceLocked(resource Resource) *ResourceBudget {
	if resource == GraphQL {
		return &g.graphql
	}
	return &g.rest
}

func (g *Gate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

// Snapshot returns the latest server observations without mutating them.
func (g *Gate) Snapshot() Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Snapshot{
		REST:         g.rest,
		GraphQL:      g.graphql,
		BackoffUntil: g.backoffUntil,
		InFlight:     g.inFlight,
	}
}

func (g *Gate) restore(snapshot Snapshot) {
	g.mu.Lock()
	if snapshot.REST.Known {
		g.rest = snapshot.REST
	}
	if snapshot.GraphQL.Known {
		g.graphql = snapshot.GraphQL
	}
	g.mu.Unlock()
}

func (g *Gate) makeUnavailable(err error) {
	g.mu.Lock()
	if g.unavailable == nil {
		if err == nil {
			err = ErrLeaseLost
		}
		g.unavailable = err
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *Gate) waitIdle(ctx context.Context) error {
	for {
		g.mu.Lock()
		if g.inFlight == 0 {
			g.mu.Unlock()
			return nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func joinErrors(errs ...error) error {
	var nonNil []error
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	return errors.Join(nonNil...)
}
