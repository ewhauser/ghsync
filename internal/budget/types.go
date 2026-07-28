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
	Interactive Class = "interactive"
	Event       Class = "event"
	Sweep       Class = "sweep"
)

func (c Class) valid() bool {
	return c == Interactive || c == Event || c == Sweep
}

// Resource identifies GitHub's independently-accounted API budgets.
type Resource string

const (
	REST    Resource = "rest"
	GraphQL Resource = "graphql"

	// Auth is the App-JWT installation-token exchange. It passes through the
	// C-B1 gate for concurrency and global backoff, but GitHub does not account
	// it against an installation's REST or GraphQL quota.
	Auth Resource = "auth"
)

func (r Resource) valid() bool {
	return r == REST || r == GraphQL || r == Auth
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
	observeRate GraphQLRateObserver
}

func NewRESTRequest(req *http.Request) *Request {
	return &Request{httpRequest: req, resource: REST}
}

func NewGraphQLRequest(req *http.Request, observer GraphQLRateObserver) *Request {
	return &Request{
		httpRequest: req,
		resource:    GraphQL,
		observeRate: observer,
	}
}

func NewAuthRequest(req *http.Request) *Request {
	return &Request{httpRequest: req, resource: Auth}
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
	REST         ResourceBudget
	GraphQL      ResourceBudget
	BackoffUntil time.Time
	InFlight     int
}

// Starvation is emitted once for each request that queues behind a C-B3
// reserved floor.
type Starvation struct {
	Class     Class
	Resource  Resource
	Remaining int64
	Limit     int64
	ResetAt   time.Time
}

// StarvationHook is the M1 observability seam; M6 will attach metrics.
type StarvationHook func(Starvation)

var (
	ErrClosed    = fmt.Errorf("GitHub budget gate is closed")
	ErrLeaseLost = fmt.Errorf("GitHub budget gate lease lost")
)
