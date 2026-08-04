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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	defaultConcurrency = 40
	// Public-GitHub App installation baseline; higher GHEC limits are
	// adopted from observed rate headers.
	defaultRESTLimit         = int64(5000)
	defaultGraphQLLimit      = int64(5000)
	defaultRESTEstimate      = 1
	defaultGraphQLEstimate   = 100
	defaultSecondaryBackoff  = 60 * time.Second
	defaultSweepFloor        = 0.20
	defaultEventFloor        = 0.10
	secondaryBodyInspectSize = 64 << 10
)

// Options configures a Gate. Limits are initial floor denominators only; an
// observed server limit always replaces them. Reservations are admission-side
// pessimism and never replace or persist server-authoritative remaining values
// (C-B2/C-B3).
type Options struct {
	MaxConcurrent int
	RESTLimit     int64
	GraphQLLimit  int64
	// SweepFloor and EventFloor are fractions in (0,1); EventFloor must be
	// lower because event work has priority over sweep work.
	SweepFloor             float64
	EventFloor             float64
	RESTRequestEstimate    int64
	GraphQLPointEstimate   int64
	SecondaryLimitFallback time.Duration
	OnStarvation           StarvationHook
	OnRequest              RequestHook
	Clock                  Clock
	Tracer                 trace.Tracer
}

// Gate is the C-B1 per-installation choke point. It owns admission,
// server-authoritative REST and GraphQL observations, per-auth-context
// secondary-limit backoff, and the C-B6 concurrency ceiling.
type Gate struct {
	client *http.Client
	clock  Clock

	mu                     sync.Mutex
	changed                chan struct{}
	inFlight               int
	maxConcurrent          int
	rest                   ResourceBudget
	appREST                ResourceBudget
	graphql                ResourceBudget
	restReserved           int64
	appRESTReserved        int64
	graphqlReserved        int64
	restEstimate           int64
	graphqlEstimate        int64
	sweepFloor             float64
	eventFloor             float64
	backoffUntil           time.Time
	appJWTBackoffUntil     time.Time
	secondaryLimitFallback time.Duration
	unavailable            error
	leaseUntil             time.Time
	onStarvation           StarvationHook
	onRequest              RequestHook
	tracer                 trace.Tracer
	nextAdmissionID        uint64
	admissions             map[uint64]*admission

	lease *leaseRuntime
}

type admission struct {
	gate        *Gate
	id          uint64
	resource    Resource
	authContext AuthContext
	cost        int64
	holdsSlot   bool
	cancel      context.CancelFunc
	once        sync.Once

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
func New(client *http.Client, options Options) *Gate { //nolint:gocritic // value options are normalized into owned gate state
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
		options.RESTLimit = defaultRESTLimit
	}
	if options.GraphQLLimit <= 0 {
		options.GraphQLLimit = defaultGraphQLLimit
	}
	if options.SweepFloor <= 0 || options.SweepFloor >= 1 {
		options.SweepFloor = defaultSweepFloor
	}
	if options.EventFloor <= 0 || options.EventFloor >= 1 {
		options.EventFloor = defaultEventFloor
	}
	if options.EventFloor >= options.SweepFloor {
		options.SweepFloor = defaultSweepFloor
		options.EventFloor = defaultEventFloor
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
	if options.Tracer == nil {
		options.Tracer = noop.NewTracerProvider().Tracer(
			"github.com/ewhauser/ghsync/internal/budget",
		)
	}
	return &Gate{
		client:                 &ownedClient,
		clock:                  options.Clock,
		changed:                make(chan struct{}),
		maxConcurrent:          options.MaxConcurrent,
		rest:                   ResourceBudget{Limit: options.RESTLimit},
		appREST:                ResourceBudget{Limit: defaultRESTLimit},
		graphql:                ResourceBudget{Limit: options.GraphQLLimit},
		restEstimate:           options.RESTRequestEstimate,
		graphqlEstimate:        options.GraphQLPointEstimate,
		sweepFloor:             options.SweepFloor,
		eventFloor:             options.EventFloor,
		secondaryLimitFallback: options.SecondaryLimitFallback,
		onStarvation:           options.OnStarvation,
		onRequest:              options.OnRequest,
		tracer:                 options.Tracer,
		admissions:             make(map[uint64]*admission),
	}
}

// Do admits and performs exactly one GitHub request (C-B1). REST state comes
// only from x-ratelimit-* headers; GraphQL state comes only from the supplied
// rateLimit observer (C-B2/C-B5).
//
// Admission owns a C-B6 concurrency slot until the response body reaches EOF
// or is closed. Do may return a non-nil Response alongside a non-nil error;
// callers must still close every non-nil response body. Forgetting to close
// permanently leaks that slot, and enough leaks stop all installation traffic.
// Redirect following is disabled because a redirect would otherwise hide an
// unadmitted request inside http.Client.Do.
//
// An installation-token mint issued from a BeforeSend hook reuses its outer
// request's concurrency slot so renewal cannot deadlock at MaxConcurrent=1.
// The mint still performs independent App-JWT REST admission, reservation,
// header observation, and backoff accounting.
func (g *Gate) Do(
	ctx context.Context,
	class Class,
	req *Request,
) (result *Response, err error) {
	if err := validateDo(ctx, class, req); err != nil {
		return nil, err
	}
	ctx, span := g.tracer.Start(
		ctx,
		"ghsync.github.admission",
		trace.WithAttributes(
			attribute.String("ghsync.github.class", string(class)),
			attribute.String("ghsync.github.resource", string(req.resource)),
			attribute.String("ghsync.github.auth_context", string(req.authContext)),
		),
	)
	defer func() {
		if result != nil && result.HTTP != nil {
			span.SetAttributes(attribute.Int(
				"http.response.status_code",
				result.HTTP.StatusCode,
			))
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	admissionStarted := time.Now()

	// Installation-token renewal reached from a before-send hook is already
	// inside this gate's admitted slot. It executes sequentially in that slot,
	// avoiding a MaxConcurrent=1 nested-admission deadlock while remaining
	// covered by the outer C-B6 accounting.
	var parentAdmission *admission
	if parent, ok := ctx.Value(admissionContextKey{}).(*admission); ok &&
		parent.gate == g && req.tokenMint {
		parentAdmission = parent
		span.SetAttributes(attribute.Bool("ghsync.github.nested_auth", true))
	}

	reportedStarvation := false
	var admitted *admission
	var admittedCtx context.Context
	for {
		decision, candidateCtx := g.tryAdmit(
			ctx,
			class,
			req.resource,
			req.authContext,
			parentAdmission,
		)
		if decision.err != nil {
			return nil, decision.err
		}
		if !decision.wait {
			admitted = decision.admitted
			admittedCtx = candidateCtx
			break
		}
		if decision.starvation != nil && !reportedStarvation {
			reportedStarvation = true
			if g.onStarvation != nil {
				g.onStarvation(*decision.starvation)
			}
		}
		if err := waitForChange(
			ctx,
			g.clock,
			decision.changed,
			decision.waitUntil,
		); err != nil {
			return nil, err
		}
	}
	span.AddEvent(
		"admitted",
		trace.WithAttributes(attribute.Float64(
			"ghsync.github.admission_wait_seconds",
			time.Since(admissionStarted).Seconds(),
		)),
	)

	return g.doAdmitted(admittedCtx, class, req, admitted)
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
	if !req.authContext.valid() {
		return fmt.Errorf("budget gate: invalid auth context %q", req.authContext)
	}
	if req.resource == GraphQL && req.authContext != InstallationAuth {
		return fmt.Errorf("budget gate: GraphQL requires installation auth")
	}
	if req.tokenMint &&
		(req.resource != REST || req.authContext != AppJWTAuth) {
		return fmt.Errorf("budget gate: token exchange requires App-JWT auth")
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

	resp, requestErr := g.client.Do(httpReq) //nolint:bodyclose // response body ownership is transferred to the caller
	var network *networkBody
	if admitted != nil && resp != nil && usableBody(resp) {
		network = &networkBody{ReadCloser: resp.Body}
		resp.Body = network
		admitted.attachBody(resp.Body)
	}

	var graphQLRate *GraphQLRate
	var observeErr error
	if resp != nil {
		secondaryErr := g.observeSecondaryLimit(
			sendCtx,
			req.authContext,
			resp,
		)
		switch req.resource {
		case REST:
			observeErr = g.observeREST(req.authContext, resp.Header)
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
		if resp == nil || !usableBody(resp) {
			admitted.finish()
		} else {
			switch {
			case network != nil && network.Done():
				// Observers consumed the network body. A restored in-memory
				// body no longer counts against C-B6.
				admitted.finish()
			default:
				resp.Body = &admittedBody{
					ReadCloser: resp.Body,
					admission:  admitted,
				}
				admitted.attachBody(resp.Body)
			}
		}
	}

	result := &Response{HTTP: resp, GraphQLRate: graphQLRate}
	var statusCode int
	if resp != nil {
		statusCode = resp.StatusCode
	}
	if g.onRequest != nil {
		authContext, endpoint := requestAttribution(req, httpReq)
		g.onRequest(RequestObservation{
			Class:          class,
			Resource:       req.resource,
			AuthContext:    authContext,
			EndpointFamily: endpoint,
			StatusCode:     statusCode,
			Conditional:    httpReq.Header.Get("If-None-Match") != "",
			NotModified:    statusCode == http.StatusNotModified,
			Err:            joinErrors(requestErr, observeErr),
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

type admissionDecision struct {
	wait       bool
	waitUntil  time.Time
	changed    <-chan struct{}
	starvation *Starvation
	admitted   *admission
	err        error
}

func (g *Gate) tryAdmit(
	ctx context.Context,
	class Class,
	resource Resource,
	authContext AuthContext,
	parent *admission,
) (admissionDecision, context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	reuseSlot := parent != nil && parent.holdsSlot &&
		g.admissions[parent.id] == parent

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
		return admissionDecision{err: g.unavailable}, nil
	}
	backoffUntil := g.backoffLocked(authContext)
	if now.Before(backoffUntil) {
		return admissionDecision{
			wait:      true,
			waitUntil: backoffUntil,
			changed:   g.changed,
		}, nil
	}
	if !reuseSlot && g.inFlight >= g.maxConcurrent {
		// A zero deadline means "wait for a state-change notification".
		return admissionDecision{wait: true, changed: g.changed}, nil
	}

	cost := g.estimateLocked(resource)
	state := g.resourceLocked(resource, authContext)
	if state.Known && now.Before(state.ResetAt) {
		afterReservation := state.Remaining -
			g.reservedLocked(resource, authContext) - cost
		floor := g.floorFor(class)
		floorRemaining := int64(math.Ceil(float64(state.Limit) * floor))
		if afterReservation < 0 || (floor > 0 && afterReservation < floorRemaining) {
			return admissionDecision{
				wait:      true,
				waitUntil: state.ResetAt,
				changed:   g.changed,
				starvation: &Starvation{
					Class:       class,
					Resource:    resource,
					AuthContext: authContext,
					Remaining: state.Remaining -
						g.reservedLocked(resource, authContext),
					Limit:   state.Limit,
					ResetAt: state.ResetAt,
				},
			}, nil
		}
	}

	admittedCtx, cancel := context.WithCancel(ctx)
	g.nextAdmissionID++
	entry := &admission{
		gate:        g,
		id:          g.nextAdmissionID,
		resource:    resource,
		authContext: authContext,
		cost:        cost,
		holdsSlot:   !reuseSlot,
		cancel:      cancel,
	}
	if entry.holdsSlot {
		g.inFlight++
	}
	g.addReservationLocked(resource, authContext, cost)
	g.admissions[entry.id] = entry
	return admissionDecision{admitted: entry}, admittedCtx
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
	timer, stop := clock.NewTimerAt(until)
	defer stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer:
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
		if a.holdsSlot {
			g.inFlight--
		}
		g.addReservationLocked(a.resource, a.authContext, -a.cost)
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

func (g *Gate) observeREST(
	authContext AuthContext,
	header http.Header,
) error {
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
	state := g.resourceLocked(REST, authContext)
	*state = mergeObservation(*state, ResourceBudget{
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
	authContext AuthContext,
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
	backoffUntil := g.backoffLocked(authContext)
	changed := until.After(backoffUntil)
	if changed {
		g.setBackoffLocked(authContext, until)
		g.signalLocked()
	}
	g.mu.Unlock()
	if !changed || g.lease == nil {
		return nil
	}

	// Secondary-limit coordination is the deliberate exception to C-P6's
	// periodic snapshots: losing this credential-context closure on handoff is
	// unsafe, so persist it synchronously under the lease token.
	ok, err := g.persistBackoff(ctx, authContext, until)
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

func (g *Gate) floorFor(class Class) float64 {
	switch class {
	case Sweep:
		return g.sweepFloor
	case Event:
		return g.eventFloor
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

func (g *Gate) reservedLocked(
	resource Resource,
	authContext AuthContext,
) int64 {
	if resource == GraphQL {
		return g.graphqlReserved
	}
	if resource == REST && authContext == AppJWTAuth {
		return g.appRESTReserved
	}
	return g.restReserved
}

func (g *Gate) addReservationLocked(
	resource Resource,
	authContext AuthContext,
	delta int64,
) {
	switch resource {
	case REST:
		if authContext == AppJWTAuth {
			g.appRESTReserved += delta
		} else {
			g.restReserved += delta
		}
	case GraphQL:
		g.graphqlReserved += delta
	}
}

func (g *Gate) resourceLocked(
	resource Resource,
	authContext AuthContext,
) *ResourceBudget {
	if resource == GraphQL {
		return &g.graphql
	}
	if resource == REST && authContext == AppJWTAuth {
		return &g.appREST
	}
	return &g.rest
}

func (g *Gate) backoffLocked(authContext AuthContext) time.Time {
	if authContext == AppJWTAuth {
		return g.appJWTBackoffUntil
	}
	return g.backoffUntil
}

func (g *Gate) setBackoffLocked(
	authContext AuthContext,
	until time.Time,
) {
	if authContext == AppJWTAuth {
		g.appJWTBackoffUntil = until
		return
	}
	g.backoffUntil = until
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
		REST:               g.rest,
		AppREST:            g.appREST,
		GraphQL:            g.graphql,
		BackoffUntil:       g.backoffUntil,
		AppJWTBackoffUntil: g.appJWTBackoffUntil,
		InFlight:           g.inFlight,
	}
}

func (g *Gate) restore(snapshot *Snapshot) {
	g.mu.Lock()
	if snapshot.REST.Known {
		g.rest = snapshot.REST
	}
	if snapshot.AppREST.Known {
		g.appREST = snapshot.AppREST
	}
	if snapshot.GraphQL.Known {
		g.graphql = snapshot.GraphQL
	}
	if snapshot.BackoffUntil.After(g.backoffUntil) {
		g.backoffUntil = snapshot.BackoffUntil
	}
	if snapshot.AppJWTBackoffUntil.After(g.appJWTBackoffUntil) {
		g.appJWTBackoffUntil = snapshot.AppJWTBackoffUntil
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
