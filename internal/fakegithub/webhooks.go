package fakegithub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ewhauser/ghsync/internal/gh"
	jwt "github.com/golang-jwt/jwt/v4"
)

func (s *Server) installationToken(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept") != "application/vnd.github+json" ||
		r.Header.Get("X-GitHub-Api-Version") != "2022-11-28" ||
		!strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "invalid GitHub API headers", http.StatusBadRequest)
		return
	}
	if !s.validateAppAuthorization(w, r) {
		return
	}
	installationID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || installationID <= 0 {
		http.Error(w, "bad installation ID", http.StatusBadRequest)
		return
	}
	now := s.now()
	s.mu.Lock()
	s.tokenRequests++
	requestNumber := s.tokenRequests
	ttl := s.tokenTTL
	s.mu.Unlock()
	writeJSONStatus(w, http.StatusCreated, map[string]any{
		"token": fmt.Sprintf(
			"fake-installation-%d-token-%d",
			installationID,
			requestNumber,
		),
		"expires_at": now.Add(ttl).UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) validateAppAuthorization(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	const bearer = "Bearer "
	rawAuthorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(rawAuthorization, bearer) ||
		s.appPublicKey == nil || s.appID <= 0 {
		http.Error(w, "valid App JWT required", http.StatusUnauthorized)
		return false
	}
	auth := strings.TrimPrefix(rawAuthorization, bearer)
	claims := &fakeAppClaims{}
	token, err := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
	).ParseWithClaims(auth, claims, func(token *jwt.Token) (any, error) {
		if token.Header["typ"] != "JWT" {
			return nil, fmt.Errorf("JWT typ header is required")
		}
		return s.appPublicKey, nil
	})
	now := s.now()
	expectedIssuer := strconv.FormatInt(s.appID, 10)
	if err != nil || !token.Valid ||
		claims.Issuer != expectedIssuer ||
		claims.IssuedAt == nil ||
		claims.ExpiresAt == nil ||
		claims.IssuedAt.After(now) ||
		claims.IssuedAt.Before(now.Add(-time.Minute)) ||
		!claims.ExpiresAt.After(now) ||
		claims.ExpiresAt.Sub(claims.IssuedAt.Time) > 10*time.Minute {
		http.Error(w, "valid App JWT required", http.StatusUnauthorized)
		return false
	}
	return true
}

func validateInstallationAuthorization(
	w http.ResponseWriter,
	r *http.Request,
) bool {
	if !strings.HasPrefix(
		r.Header.Get("Authorization"),
		"Bearer fake-installation-",
	) {
		http.Error(
			w,
			"valid installation bearer required",
			http.StatusUnauthorized,
		)
		return false
	}
	return true
}

func (s *Server) listAppHookDeliveries(
	w http.ResponseWriter,
	r *http.Request,
) {
	perPage := 30
	if raw := r.URL.Query().Get("per_page"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || value > 100 {
			http.Error(w, "bad per_page", http.StatusUnprocessableEntity)
			return
		}
		perPage = value
	}
	var beforeID int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		value, err := decodeDeliveryCursor(raw)
		if err != nil {
			http.Error(w, "bad cursor", http.StatusUnprocessableEntity)
			return
		}
		beforeID = value
	}
	deliveries := s.Deliveries()
	if beforeID > 0 {
		filtered := make([]HookDelivery, 0, len(deliveries))
		for index := range deliveries {
			delivery := &deliveries[index]
			if delivery.ID < beforeID {
				filtered = append(filtered, *delivery)
			}
		}
		deliveries = filtered
	}
	end := min(perPage, len(deliveries))
	page := deliveries[:end]
	if end < len(deliveries) {
		nextURL := *r.URL
		query := nextURL.Query()
		query.Set(
			"cursor",
			encodeDeliveryCursor(page[len(page)-1].ID),
		)
		nextURL.RawQuery = query.Encode()
		w.Header().Set(
			"Link",
			fmt.Sprintf("<%s>; rel=\"next\"", nextURL.String()),
		)
	}
	s.writeConditionalJSON(w, r, page)
}

func (s *Server) redeliverAppHookDelivery(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "bad delivery ID", http.StatusUnprocessableEntity)
		return
	}
	s.mu.Lock()
	var original *storedHookDelivery
	for index := range s.deliveries {
		if s.deliveries[index].ID == id {
			deliveryCopy := s.deliveries[index]
			original = &deliveryCopy
			break
		}
	}
	if original != nil {
		s.redeliveries = append(s.redeliveries, id)
	}
	s.mu.Unlock()
	if original == nil {
		http.Error(w, "delivery not found", http.StatusUnprocessableEntity)
		return
	}
	// GitHub acknowledges a redelivery request before attempting the webhook.
	// Use a background context because the request context is canceled as
	// soon as this 202 response completes.
	w.WriteHeader(http.StatusAccepted)
	go func(delivery storedHookDelivery) { //nolint:contextcheck // redelivery must outlive the accepted control request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		statusCode, _ := s.sendWebhook(
			ctx,
			delivery.targetURL,
			delivery.Event,
			delivery.GUID,
			delivery.body,
		)
		s.recordHookDelivery(
			delivery.targetURL,
			delivery.Event,
			delivery.GUID,
			delivery.body,
			true,
			statusCode,
		)
	}(*original)
}

func encodeDeliveryCursor(beforeID int64) string {
	return base64.RawURLEncoding.EncodeToString(
		fmt.Appendf(nil, "before:%d", beforeID),
	)
}

func decodeDeliveryCursor(cursor string) (int64, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimPrefix(string(decoded), "before:")
	if raw == string(decoded) {
		return 0, fmt.Errorf("cursor prefix is invalid")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("cursor boundary is invalid")
	}
	return value, nil
}

type fakeAppClaims struct {
	jwt.RegisteredClaims
}

// Valid deliberately leaves time validation to installationToken's injected
// clock while jwt.Parser still verifies the signature and signing algorithm.
func (*fakeAppClaims) Valid() error {
	return nil
}

// EmitWebhook signs and POSTs a webhook delivery to targetURL, returning the
// globally unique delivery GUID it generated. Non-2xx responses are errors, mirroring
// GitHub's delivery-failure semantics.
func (s *Server) EmitWebhook(ctx context.Context, targetURL, event string, payload any) (string, error) {
	return s.EmitWebhookWithGUID(ctx, targetURL, event, "", payload)
}

// EmitWebhookWithGUID emits a delivery with an explicit GUID. Tests use it to
// model GitHub retries and verify GUID deduplication; an empty GUID generates a
// new UUID just like EmitWebhook.
func (s *Server) EmitWebhookWithGUID(
	ctx context.Context,
	targetURL string,
	event string,
	guid string,
	payload any,
) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	if guid == "" {
		guid, err = newDeliveryGUID()
		if err != nil {
			return "", fmt.Errorf("generate delivery GUID: %w", err)
		}
	}
	statusCode, deliveryErr := s.sendWebhook(
		ctx,
		targetURL,
		event,
		guid,
		body,
	)
	s.recordHookDelivery(
		targetURL,
		event,
		guid,
		body,
		false,
		statusCode,
	)
	return guid, deliveryErr
}

// DropWebhook records a GitHub-side delivery without sending it to ingress.
// C-R4 tests use this to model an outage/drop and then verify redelivery.
func (s *Server) DropWebhook(
	targetURL string,
	event string,
	payload any,
) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	guid, err := newDeliveryGUID()
	if err != nil {
		return "", fmt.Errorf("generate delivery GUID: %w", err)
	}
	s.recordHookDelivery(targetURL, event, guid, body, false, 0)
	return guid, nil
}

func (s *Server) sendWebhook(
	ctx context.Context,
	targetURL string,
	event string,
	guid string,
	body []byte,
) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		targetURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", guid)
	req.Header.Set("X-Hub-Signature-256", gh.SignBody(s.webhookSecret, body))
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf(
			"webhook target returned %d",
			resp.StatusCode,
		)
	}
	return resp.StatusCode, nil
}

func (s *Server) recordHookDelivery(
	targetURL string,
	event string,
	guid string,
	body []byte,
	redelivery bool,
	statusCode int,
) {
	status := "DROPPED"
	if statusCode >= 200 && statusCode <= 399 {
		status = "OK"
	} else if statusCode > 0 {
		status = "FAILED"
	}
	var action string
	var envelope struct {
		Action string `json:"action"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		action = envelope.Action
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextDeliveryID
	s.nextDeliveryID++
	s.deliveries = append(s.deliveries, storedHookDelivery{
		HookDelivery: HookDelivery{
			ID:           id,
			GUID:         guid,
			DeliveredAt:  s.now().UTC(),
			Redelivery:   redelivery,
			Status:       status,
			StatusCode:   statusCode,
			Event:        event,
			Action:       action,
			RepositoryID: s.fixture.Repository.ID,
		},
		targetURL: targetURL,
		body:      append([]byte(nil), body...),
	})
}
