package gh

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/acme/frontier/internal/budget"
)

// AppHookDelivery is the compact delivery-list representation used by C-R4.
// The list endpoint intentionally omits request payloads.
type AppHookDelivery struct {
	ID             int64     `json:"id"`
	GUID           string    `json:"guid"`
	DeliveredAt    time.Time `json:"delivered_at"`
	Redelivery     bool      `json:"redelivery"`
	Status         string    `json:"status"`
	StatusCode     int       `json:"status_code"`
	Event          string    `json:"event"`
	Action         string    `json:"action"`
	InstallationID int64     `json:"installation_id"`
	RepositoryID   int64     `json:"repository_id"`
}

// ListAppHookDeliveriesOptions controls newest-first delivery pagination.
type ListAppHookDeliveriesOptions struct {
	PerPage int
	Cursor  string
}

// DeliveriesClient is separate from RESTClient because GitHub requires an App
// JWT for /app/hook/deliveries and rejects installation access tokens.
type DeliveriesClient struct {
	client client
}

// NewDeliveriesClient constructs an App-JWT-authenticated deliveries client.
func NewDeliveriesClient(
	baseURL string,
	gate budget.Doer,
	appTokens TokenProvider,
) (*DeliveriesClient, error) {
	common, err := newClient(baseURL, gate, appTokens)
	if err != nil {
		return nil, err
	}
	return &DeliveriesClient{client: common}, nil
}

// ListAppHookDeliveries fetches deliveries newest first.
func (c *DeliveriesClient) ListAppHookDeliveries(
	ctx context.Context,
	options ListAppHookDeliveriesOptions,
	etag string,
) ([]AppHookDelivery, *RESTResponse, error) {
	query := make(url.Values)
	if options.PerPage > 0 {
		query.Set("per_page", strconv.Itoa(options.PerPage))
	}
	if options.Cursor != "" {
		query.Set("cursor", options.Cursor)
	}
	var deliveries []AppHookDelivery
	response, err := c.client.getJSON(
		ctx,
		budget.Sweep,
		"app/hook/deliveries",
		query,
		etag,
		&deliveries,
	)
	return deliveries, response, err
}

// RedeliverAppHookDelivery requests a new attempt for one delivery.
func (c *DeliveriesClient) RedeliverAppHookDelivery(
	ctx context.Context,
	deliveryID int64,
) error {
	if deliveryID <= 0 {
		return fmt.Errorf("delivery ID must be positive")
	}
	path := fmt.Sprintf(
		"app/hook/deliveries/%d/attempts",
		deliveryID,
	)
	req, err := c.client.request(ctx, http.MethodPost, path, nil, nil)
	if err != nil {
		return err
	}
	gated, err := c.client.gate.Do(
		ctx,
		budget.Sweep,
		budget.NewRESTRequest(req).BeforeSend(c.client.authorize),
	)
	if err != nil {
		if gated != nil {
			_ = closeResponseBody(gated.HTTP)
		}
		return err
	}
	resp := gated.HTTP
	if resp.StatusCode != http.StatusAccepted {
		return decodeHTTPError(resp)
	}
	return resp.Body.Close()
}
