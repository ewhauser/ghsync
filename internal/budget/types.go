// Package budget owns the single admission and accounting choke point for
// GitHub requests (SYNC_ENGINE C-B1..C-B6).
package budget

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Class is the request priority used by the C-B3 reserved floors.
type Class string

const (
	// Interactive is user-requested work and has no reserved-floor deduction.
	Interactive Class = "interactive"
	// Event is webhook-originated work protected from sweep exhaustion.
	Event Class = "event"
	// Sweep is background reconciliation work.
	Sweep Class = "sweep"
)

func (c Class) valid() bool {
	return c == Interactive || c == Event || c == Sweep
}

// AuthContext identifies the credential pool GitHub uses to account a
// request. Installation tokens and App JWTs have independent REST budgets.
type AuthContext string

const (
	// InstallationAuth is an installation access token.
	InstallationAuth AuthContext = "installation"
	// AppJWTAuth is a GitHub App JWT.
	AppJWTAuth AuthContext = "app_jwt"
)

func (c AuthContext) valid() bool {
	return c == InstallationAuth || c == AppJWTAuth
}

// Resource identifies GitHub's independently-accounted API resources. REST
// accounting is further partitioned by AuthContext.
type Resource string

const (
	// REST is GitHub's REST request budget.
	REST Resource = "rest"
	// GraphQL is GitHub's installation GraphQL point budget.
	GraphQL Resource = "graphql"
)

func (r Resource) valid() bool {
	return r == REST || r == GraphQL
}

// GraphQLRate is extracted from a response's top-level data.rateLimit block.
// It is deliberately distinct from REST response-header accounting (C-B5).
type GraphQLRate struct {
	Cost      int64
	Limit     int64
	Remaining int64
	ResetAt   time.Time
}

// GraphQLRateObserver reads and restores a GraphQL response body, returning
// the authoritative rateLimit block when one is present.
type GraphQLRateObserver func(*http.Response) (GraphQLRate, bool, error)

// Request wraps the only HTTP request shape accepted by Gate.Do. Callers use
// the constructors below so REST, GraphQL, and App-auth accounting cannot be
// confused.
type Request struct {
	httpRequest *http.Request
	resource    Resource
	authContext AuthContext
	observeRate GraphQLRateObserver
	beforeSend  func(context.Context, *http.Request) error
	tokenMint   bool
}

// NewRESTRequest wraps one installation-authenticated REST request for
// admission. New call sites should use NewInstallationRESTRequest so the
// credential context remains explicit.
func NewRESTRequest(req *http.Request) *Request {
	return NewInstallationRESTRequest(req)
}

// NewInstallationRESTRequest wraps one installation-token REST request.
func NewInstallationRESTRequest(req *http.Request) *Request {
	return &Request{
		httpRequest: req,
		resource:    REST,
		authContext: InstallationAuth,
	}
}

// NewAppRESTRequest wraps one App-JWT REST request.
func NewAppRESTRequest(req *http.Request) *Request {
	return &Request{
		httpRequest: req,
		resource:    REST,
		authContext: AppJWTAuth,
	}
}

// NewGraphQLRequest wraps one GraphQL request and its rate observer.
func NewGraphQLRequest(req *http.Request, observer GraphQLRateObserver) *Request {
	return &Request{
		httpRequest: req,
		resource:    GraphQL,
		authContext: InstallationAuth,
		observeRate: observer,
	}
}

// NewAuthRequest wraps one App-JWT installation-token exchange.
func NewAuthRequest(req *http.Request) *Request {
	return &Request{
		httpRequest: req,
		resource:    REST,
		authContext: AppJWTAuth,
		tokenMint:   true,
	}
}

// BeforeSend installs work that must run after admission and immediately
// before the transport. GitHub clients use it to refresh and inject an
// installation token without letting a queued request carry a stale token.
func (r *Request) BeforeSend(
	fn func(context.Context, *http.Request) error,
) *Request {
	r.beforeSend = fn
	return r
}

// Response preserves the HTTP response and, for GraphQL, the extracted point
// accounting observed before the concurrency slot is released.
type Response struct {
	HTTP        *http.Response
	GraphQLRate *GraphQLRate
}

// Doer is the narrow dependency used by internal/gh. Implementations must
// preserve Gate.Do's C-B invariants.
type Doer interface {
	Do(context.Context, Class, *Request) (*Response, error)
}

// ResourceBudget is the most recently observed server-authoritative budget.
// Known is false until a complete REST header set or GraphQL rateLimit block
// has been observed.
type ResourceBudget struct {
	Known     bool
	Limit     int64
	Remaining int64
	ResetAt   time.Time
}

// Snapshot is safe to expose to persistence and observability code. It never
// contains credentials or request data.
type Snapshot struct {
	REST               ResourceBudget
	AppREST            ResourceBudget
	GraphQL            ResourceBudget
	BackoffUntil       time.Time
	AppJWTBackoffUntil time.Time
	InFlight           int
}

// Starvation is emitted once for each request that queues behind a C-B3
// reserved floor.
type Starvation struct {
	Class       Class
	Resource    Resource
	AuthContext AuthContext
	Remaining   int64
	Limit       int64
	ResetAt     time.Time
}

// StarvationHook is the M1 observability seam; M6 will attach metrics.
type StarvationHook func(Starvation)

// RequestObservation is emitted after one admitted network call. It contains
// only cardinality-bounded accounting data and never request URLs or headers.
type RequestObservation struct {
	Class       Class
	Resource    Resource
	AuthContext AuthContext
	StatusCode  int
	Conditional bool
	NotModified bool
	Err         error
}

// RequestHook is M6's C-B1/C-B4 request-rate and conditional-hit seam.
type RequestHook func(RequestObservation)

var (
	// ErrClosed reports admission attempted after Gate.Close.
	ErrClosed = fmt.Errorf("GitHub budget gate is closed")
	// ErrLeaseLost reports proven loss or expiry of the budget lease.
	ErrLeaseLost = fmt.Errorf("GitHub budget gate lease lost")
)
