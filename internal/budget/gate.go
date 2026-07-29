package budget

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultConcurrency       = 40
	defaultRESTEstimate      = 1
	defaultGraphQLEstimate   = 100
	defaultSecondaryBackoff  = 60 * time.Second
	secondaryBodyInspectSize = 64 << 10
	sweepFloor               = 0.20
	eventFloor               = 0.10
)

// Options configures a Gate. Limits are initial floor denominators only; an
// observed server limit always replaces them. Reservations are admission-side
// pessimism and never replace or persist server-authoritative remaining values
// (C-B2/C-B3).
type Options struct {
	MaxConcurrent          int
	RESTLimit              int64
	GraphQLLimit           int64
	RESTRequestEstimate    int64
	GraphQLPointEstimate   int64
	SecondaryLimitFallback time.Duration
	OnStarvation           StarvationHook
	OnRequest              RequestHook
	Clock                  Clock
}

// Gate is the C-B1 per-installation choke point. It owns admission,
// server-authoritative REST and GraphQL observations, global secondary-limit
// backoff, and the C-B6 concurrency ceiling.
type Gate struct {
	client *http.Client
	clock  Clock

	mu                     sync.Mutex
	changed                chan struct{}
	inFlight               int
	maxConcurrent          int
	rest                   ResourceBudget
	graphql                ResourceBudget
	restReserved           int64
	graphqlReserved        int64
	restEstimate           int64
	graphqlEstimate        int64
	backoffUntil           time.Time
	secondaryLimitFallback time.Duration
	unavailable            error
	leaseUntil             time.Time
	onStarvation           StarvationHook
	onRequest              RequestHook
	nextAdmissionID        uint64
	admissions             map[uint64]*admission

	lease *leaseRuntime
}

type admission struct {
	gate     *Gate
	id       uint64
	resource Resource
	cost     int64
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once

	mu      sync.Mutex
	body    io.Closer
	aborted bool
}

type admissionContextKey struct{}

// InsideAdmission reports whether ctx is executing a Gate before-send hook.
// Token providers use it only to avoid joining a non-admitted singleflight
// renewal that could be queued behind the caller's own concurrency slot.
func InsideAdmission(ctx context.Context) bool {
	_, ok := ctx.Value(admissionContextKey{}).(*admission)
	return ok
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
	if options.RESTRequestEstimate <= 0 {
		options.RESTRequestEstimate = defaultRESTEstimate
	}
	if options.GraphQLPointEstimate <= 0 {
		options.GraphQLPointEstimate = defaultGraphQLEstimate
	}
	if options.SecondaryLimitFallback <= 0 {
		options.SecondaryLimitFallback = defaultSecondaryBackoff
	}
	if options.Clock == nil {
		options.Clock = realClock{}
	}
	return &Gate{
		client:                 &ownedClient,
		clock:                  options.Clock,
		changed:                make(chan struct{}),
		maxConcurrent:          options.MaxConcurrent,
		rest:                   ResourceBudget{Limit: options.RESTLimit},
		graphql:                ResourceBudget{Limit: options.GraphQLLimit},
		restEstimate:           options.RESTRequestEstimate,
		graphqlEstimate:        options.GraphQLPointEstimate,
		secondaryLimitFallback: options.SecondaryLimitFallback,
		onStarvation:           options.OnStarvation,
		onRequest:              options.OnRequest,
		admissions:             make(map[uint64]*admission),
	}
}

// Do admits and performs exactly one GitHub request (C-B1). REST state comes
// only from x-ratelimit-* headers; GraphQL state comes only from the supplied
// rateLimit observer (C-B2/C-B5).
func (g *Gate) Do(ctx context.Context, class Class, req *Request) (*Response, error) {
	if err := validateDo(ctx, class, req); err != nil {
		return nil, err
	}

	// Installation-token renewal reached from a before-send hook is already
	// inside this gate's admitted slot. It executes sequentially in that slot,
	// avoiding a MaxConcurrent=1 nested-admission deadlock while remaining
	// covered by the outer C-B6 accounting.
	if parent, ok := ctx.Value(admissionContextKey{}).(*admission); ok &&
		parent.gate == g && req.resource == Auth {
		return g.doAdmitted(ctx, class, req, nil)
	}

	reportedStarvation := false
	var admitted *admission
	for {
		wait, waitUntil, changed, starvation, next, err := g.tryAdmit(
			ctx,
			class,
			req.resource,
		)
		if err != nil {
			return nil, err
		}
		if !wait {
			admitted = next
			break
		}
		if starvation != nil && !reportedStarvation {
			reportedStarvation = true
			if g.onStarvation != nil {
				g.onStarvation(*starvation)
			}
		}
		if err := waitForChange(ctx, g.clock, changed, waitUntil); err != nil {
			return nil, err
		}
	}

	return g.doAdmitted(admitted.ctx, class, req, admitted)
}

func validateDo(ctx context.Context, class Class, req *Request) error {
	if ctx == nil {
		return fmt.Errorf("budget gate: nil context")
	}
	if !class.valid() {
		return fmt.Errorf("budget gate: invalid class %q", class)
	}
	if req == nil || req.httpRequest == nil {
		return fmt.Errorf("budget gate: nil request")
	}
	if !req.resource.valid() {
		return fmt.Errorf("budget gate: invalid resource %q", req.resource)
	}
	if req.resource == GraphQL && req.observeRate == nil {
		return fmt.Errorf("budget gate: GraphQL request has no rate observer")
	}
	return nil
}

func (g *Gate) doAdmitted(
	ctx context.Context,
	class Class,
	req *Request,
	admitted *admission,
) (*Response, error) {
	sendCtx := ctx
	if admitted != nil {
		sendCtx = context.WithValue(ctx, admissionContextKey{}, admitted)
	}
	httpReq := req.httpRequest.Clone(sendCtx)
	if req.beforeSend != nil {
		if err := req.beforeSend(sendCtx, httpReq); err != nil {
			if admitted != nil {
				admitted.finish()
			}
			return nil, fmt.Errorf("budget gate before send: %w", err)
		}
	}

	resp, requestErr := g.client.Do(httpReq)
	var network *networkBody
	if admitted != nil && usableBody(resp) {
		network = &networkBody{ReadCloser: resp.Body}
		resp.Body = network
		admitted.attachBody(resp.Body)
	}

	var graphQLRate *GraphQLRate
	var observeErr error
	if resp != nil {
		secondaryErr := g.observeSecondaryLimit(sendCtx, resp)
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
		observeErr = joinErrors(secondaryErr, observeErr)
	}

	if admitted != nil {
		switch {
		case !usableBody(resp):
			admitted.finish()
		case network.Done():
			// Observers consumed the network body. A restored in-memory body no
			// longer counts against C-B6.
			admitted.finish()
		default:
			resp.Body = &admittedBody{
				ReadCloser: resp.Body,
				admission:  admitted,
			}
			admitted.attachBody(resp.Body)
		}
	}

	result := &Response{HTTP: resp, GraphQLRate: graphQLRate}
	var statusCode int
	if resp != nil {
		statusCode = resp.StatusCode
	}
	if g.onRequest != nil {
		g.onRequest(RequestObservation{
			Class:       class,
			Resource:    req.resource,
			StatusCode:  statusCode,
			Conditional: httpReq.Header.Get("If-None-Match") != "",
			NotModified: statusCode == http.StatusNotModified,
			Err:         joinErrors(requestErr, observeErr),
		})
	}
	if requestErr != nil {
		return result, requestErr
	}
	if observeErr != nil {
		return result, observeErr
	}
	return result, nil
}

func usableBody(resp *http.Response) bool {
	return resp != nil && resp.Body != nil && resp.Body != http.NoBody
}

func (g *Gate) tryAdmit(
	ctx context.Context,
	class Class,
	resource Resource,
) (bool, time.Time, <-chan struct{}, *Starvation, *admission, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.Now()
	if g.unavailable == nil && !g.leaseUntil.IsZero() &&
		!now.Before(g.leaseUntil) {
		g.unavailable = ErrLeaseLost
		if g.lease != nil {
			g.lease.cancel()
		}
		g.signalLocked()
	}
	if g.unavailable != nil {
		return false, time.Time{}, nil, nil, nil, g.unavailable
	}
	if now.Before(g.backoffUntil) {
		return true, g.backoffUntil, g.changed, nil, nil, nil
	}
	if g.inFlight >= g.maxConcurrent {
		// A zero deadline means "wait for a state-change notification".
		return true, time.Time{}, g.changed, nil, nil, nil
	}

	cost := g.estimateLocked(resource)
	if resource != Auth {
		state := g.resourceLocked(resource)
		if state.Known && now.Before(state.ResetAt) {
			afterReservation := state.Remaining - g.reservedLocked(resource) - cost
			floor := floorFor(class)
			floorRemaining := int64(math.Ceil(float64(state.Limit) * floor))
			if afterReservation < 0 || (floor > 0 && afterReservation < floorRemaining) {
				return true, state.ResetAt, g.changed, &Starvation{
					Class:     class,
					Resource:  resource,
					Remaining: state.Remaining - g.reservedLocked(resource),
					Limit:     state.Limit,
					ResetAt:   state.ResetAt,
				}, nil, nil
			}
		}
	}

	admittedCtx, cancel := context.WithCancel(ctx)
	g.nextAdmissionID++
	entry := &admission{
		gate:     g,
		id:       g.nextAdmissionID,
		resource: resource,
		cost:     cost,
		ctx:      admittedCtx,
		cancel:   cancel,
	}
	g.inFlight++
	g.addReservationLocked(resource, cost)
	g.admissions[entry.id] = entry
	return false, time.Time{}, nil, nil, entry, nil
}

func waitForChange(
	ctx context.Context,
	clock Clock,
	changed <-chan struct{},
	until time.Time,
) error {
	if until.IsZero() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			return nil
		}
	}
	delay := until.Sub(clock.Now())
	if delay <= 0 {
		return nil
	}
	timer := clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer.C():
		return nil
	}
}

func (a *admission) attachBody(body io.Closer) {
	a.mu.Lock()
	if a.aborted {
		a.mu.Unlock()
		_ = body.Close()
		return
	}
	a.body = body
	a.mu.Unlock()
}

func (a *admission) finish() {
	a.once.Do(func() {
		a.cancel()
		a.gate.finishAdmission(a)
	})
}

func (a *admission) abort() {
	a.mu.Lock()
	a.aborted = true
	body := a.body
	a.mu.Unlock()
	a.cancel()
	if body != nil {
		_ = body.Close()
	}
	a.finish()
}

func (g *Gate) finishAdmission(a *admission) {
	g.mu.Lock()
	if _, ok := g.admissions[a.id]; ok {
		delete(g.admissions, a.id)
		g.inFlight--
		g.addReservationLocked(a.resource, -a.cost)
		g.signalLocked()
	}
	g.mu.Unlock()
}

type admittedBody struct {
	io.ReadCloser
	admission *admission
}

func (b *admittedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.admission.finish()
	}
	return n, err
}

func (b *admittedBody) Close() error {
	err := b.ReadCloser.Close()
	b.admission.finish()
	return err
}

type networkBody struct {
	io.ReadCloser
	mu   sync.Mutex
	done bool
}

func (b *networkBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.mu.Lock()
		b.done = true
		b.mu.Unlock()
	}
	return n, err
}

func (b *networkBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	b.done = true
	b.mu.Unlock()
	return err
}

func (b *networkBody) Done() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.done
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
	g.rest = mergeObservation(g.rest, ResourceBudget{
		Known:     true,
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   time.Unix(resetUnix, 0),
	})
	g.signalLocked()
	g.mu.Unlock()
	return nil
}

func (g *Gate) observeGraphQL(rate GraphQLRate) {
	if rate.Limit <= 0 || rate.Remaining < 0 || rate.ResetAt.IsZero() {
		return
	}
	g.mu.Lock()
	g.graphql = mergeObservation(g.graphql, ResourceBudget{
		Known:     true,
		Limit:     rate.Limit,
		Remaining: rate.Remaining,
		ResetAt:   rate.ResetAt,
	})
	g.signalLocked()
	g.mu.Unlock()
}

func mergeObservation(current, observed ResourceBudget) ResourceBudget {
	if !observed.Known {
		return current
	}
	if !current.Known || observed.ResetAt.After(current.ResetAt) {
		return observed
	}
	if observed.ResetAt.Before(current.ResetAt) {
		return current
	}
	if observed.Remaining < current.Remaining {
		current.Remaining = observed.Remaining
	}
	// A limit increase in the same window raises reserved floors, so retaining
	// the maximum is the conservative interpretation of conflicting headers.
	current.Limit = max(current.Limit, observed.Limit)
	return current
}

func (g *Gate) observeSecondaryLimit(
	ctx context.Context,
	resp *http.Response,
) error {
	if resp.StatusCode != http.StatusForbidden &&
		resp.StatusCode != http.StatusTooManyRequests {
		return nil
	}

	now := g.clock.Now()
	raw := strings.TrimSpace(resp.Header.Get("Retry-After"))
	var (
		until       time.Time
		isSecondary bool
		err         error
	)
	if raw != "" {
		until, err = retryAfterDeadline(now, raw)
		isSecondary = err == nil
	} else {
		isSecondary, err = responseHasSecondaryLimitShape(resp)
		if isSecondary {
			until = now.Add(g.secondaryLimitFallback)
		}
	}
	if err != nil || !isSecondary {
		return err
	}

	g.mu.Lock()
	changed := until.After(g.backoffUntil)
	if changed {
		g.backoffUntil = until
		g.signalLocked()
	}
	g.mu.Unlock()
	if !changed || g.lease == nil {
		return nil
	}

	// Secondary-limit coordination is the deliberate exception to C-P6's
	// periodic snapshots: losing this installation-wide closure on handoff is
	// unsafe, so persist it synchronously under the lease token.
	ok, err := g.persistBackoff(ctx, until)
	if err != nil {
		// The in-memory closure remains authoritative and the periodic snapshot
		// loop will retry persistence. A transport error does not prove that
		// this runtime lost the lease.
		return err
	}
	if !ok {
		err := fmt.Errorf(
			"%w: persist secondary-limit backoff: ownership lost",
			ErrLeaseLost,
		)
		g.loseLease(err)
		return err
	}
	return nil
}

func responseHasSecondaryLimitShape(resp *http.Response) (bool, error) {
	if !usableBody(resp) {
		return false, nil
	}
	original := resp.Body
	prefix, err := io.ReadAll(io.LimitReader(original, secondaryBodyInspectSize+1))
	if err != nil {
		return false, fmt.Errorf("inspect GitHub rate-limit response: %w", err)
	}
	resp.Body = &replayReadCloser{
		Reader: io.MultiReader(bytes.NewReader(prefix), original),
		closer: original,
	}
	inspect := prefix
	if len(inspect) > secondaryBodyInspectSize {
		inspect = inspect[:secondaryBodyInspectSize]
	}
	lower := strings.ToLower(string(inspect))
	return strings.Contains(lower, "secondary rate limit") ||
		strings.Contains(lower, "abuse detection"), nil
}

type replayReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *replayReadCloser) Close() error {
	return r.closer.Close()
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

func (g *Gate) estimateLocked(resource Resource) int64 {
	switch resource {
	case REST:
		return g.restEstimate
	case GraphQL:
		return g.graphqlEstimate
	default:
		return 0
	}
}

func (g *Gate) reservedLocked(resource Resource) int64 {
	if resource == GraphQL {
		return g.graphqlReserved
	}
	return g.restReserved
}

func (g *Gate) addReservationLocked(resource Resource, delta int64) {
	switch resource {
	case REST:
		g.restReserved += delta
	case GraphQL:
		g.graphqlReserved += delta
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
// In-flight reservations are intentionally omitted from persisted state.
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
	if snapshot.BackoffUntil.After(g.backoffUntil) {
		g.backoffUntil = snapshot.BackoffUntil
	}
	g.signalLocked()
	g.mu.Unlock()
}

func (g *Gate) setLeaseUntil(until time.Time) {
	g.mu.Lock()
	if g.unavailable == nil {
		g.leaseUntil = until
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *Gate) stopAdmission(err error) {
	g.mu.Lock()
	if g.unavailable == nil {
		if err == nil {
			err = ErrClosed
		}
		g.unavailable = err
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *Gate) loseLease(err error) {
	if err == nil {
		err = ErrLeaseLost
	}
	if g.lease != nil {
		// Refusing admission and renewing the lease must be one state
		// transition. Otherwise no replacement process can acquire the gate.
		g.lease.cancel()
	}
	g.stopAdmission(err)
	g.cancelAdmissions()
}

func (g *Gate) cancelAdmissions() {
	g.mu.Lock()
	entries := make([]*admission, 0, len(g.admissions))
	for _, entry := range g.admissions {
		entries = append(entries, entry)
	}
	g.mu.Unlock()
	for _, entry := range entries {
		entry.abort()
	}
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
