package gh

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	ghinstallation "github.com/bradleyfalzon/ghinstallation/v2"
	jwt "github.com/golang-jwt/jwt/v4"
	"golang.org/x/sync/singleflight"

	"github.com/acme/frontier/internal/budget"
)

const defaultTokenRefreshBefore = time.Minute

type InstallationTokenOptions struct {
	BaseURL        string
	AppID          int64
	InstallationID int64
	PrivateKey     *rsa.PrivateKey
	PrivateKeyPEM  []byte
	RefreshBefore  time.Duration
	Clock          interface{ Now() time.Time }
}

// InstallationTokens exchanges an App JWT for installation access tokens,
// caches them until shortly before expiry, and collapses concurrent renewals.
// The hand-rolled exchange keeps the token endpoint inside Gate.Do; it uses
// ghinstallation's signer rather than its RoundTripper, whose internal refresh
// request would otherwise bypass C-B1.
type InstallationTokens struct {
	baseURL        *url.URL
	appID          int64
	installationID int64
	signer         ghinstallation.Signer
	gate           budget.Doer
	refreshBefore  time.Duration
	clock          interface{ Now() time.Time }

	mu        sync.Mutex
	token     string
	expiresAt time.Time
	renewals  singleflight.Group
}

func NewInstallationTokens(
	gate budget.Doer,
	options InstallationTokenOptions,
) (*InstallationTokens, error) {
	if gate == nil {
		return nil, fmt.Errorf("GitHub budget gate is required")
	}
	if options.AppID <= 0 || options.InstallationID <= 0 {
		return nil, fmt.Errorf("GitHub App and installation IDs must be positive")
	}
	if options.PrivateKey == nil && len(options.PrivateKeyPEM) > 0 {
		parsed, err := jwt.ParseRSAPrivateKeyFromPEM(options.PrivateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("parse GitHub App private key: %w", err)
		}
		options.PrivateKey = parsed
	}
	if options.PrivateKey == nil {
		return nil, fmt.Errorf("GitHub App private key or PEM is required")
	}
	baseURL, err := url.Parse(strings.TrimRight(options.BaseURL, "/") + "/")
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("GitHub base URL must be absolute")
	}
	if options.RefreshBefore <= 0 {
		options.RefreshBefore = defaultTokenRefreshBefore
	}
	if options.Clock == nil {
		options.Clock = tokenRealClock{}
	}
	return &InstallationTokens{
		baseURL:        baseURL,
		appID:          options.AppID,
		installationID: options.InstallationID,
		signer: ghinstallation.NewRSASigner(
			jwt.SigningMethodRS256,
			options.PrivateKey,
		),
		gate:          gate,
		refreshBefore: options.RefreshBefore,
		clock:         options.Clock,
	}, nil
}

type tokenRealClock struct{}

func (tokenRealClock) Now() time.Time {
	return time.Now()
}

// AppTokens signs short-lived GitHub App JWTs for App-only endpoints such as
// the webhook deliveries API used by C-R4.
type AppTokens struct {
	appID  int64
	signer ghinstallation.Signer
	clock  interface{ Now() time.Time }
}

func NewAppTokens(
	appID int64,
	privateKeyPEM []byte,
) (*AppTokens, error) {
	if appID <= 0 {
		return nil, fmt.Errorf("GitHub App ID must be positive")
	}
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse GitHub App private key: %w", err)
	}
	return &AppTokens{
		appID: appID,
		signer: ghinstallation.NewRSASigner(
			jwt.SigningMethodRS256,
			privateKey,
		),
		clock: tokenRealClock{},
	}, nil
}

func (m *AppTokens) Token(_ context.Context) (string, error) {
	now := m.clock.Now()
	claims := &jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second).Truncate(time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(90 * time.Second).Truncate(time.Second)),
		Issuer:    strconv.FormatInt(m.appID, 10),
	}
	token, err := m.signer.Sign(claims)
	if err != nil {
		return "", fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	return token, nil
}

func (m *InstallationTokens) Token(ctx context.Context) (string, error) {
	if token, ok := m.cached(); ok {
		return token, nil
	}
	renewalKey := "installation-token"
	if budget.InsideAdmission(ctx) {
		// An ordinary renewal may itself be queued behind this request's slot.
		// Keep admitted renewals in a separate singleflight group so they can
		// reuse the outer admission instead of waiting in a cycle.
		renewalKey += "-admitted"
	}
	result := m.renewals.DoChan(renewalKey, func() (any, error) {
		if token, ok := m.cached(); ok {
			return token, nil
		}
		// One canceled waiter must not cancel the shared renewal for every
		// other caller. Bound the detached exchange independently.
		renewCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return m.renew(renewCtx)
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case outcome := <-result:
		if outcome.Err != nil {
			return "", outcome.Err
		}
		return outcome.Val.(string), nil
	}
}

func (m *InstallationTokens) cached() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token, m.token != "" &&
		m.clock.Now().Before(m.expiresAt.Add(-m.refreshBefore))
}

func (m *InstallationTokens) renew(ctx context.Context) (string, error) {
	path := fmt.Sprintf(
		"app/installations/%d/access_tokens",
		m.installationID,
	)
	endpoint := m.baseURL.ResolveReference(&url.URL{Path: path})
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint.String(),
		bytes.NewReader([]byte("{}")),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)

	gated, err := m.gate.Do(
		ctx,
		budget.Interactive,
		budget.NewAuthRequest(req).BeforeSend(m.authorizeApp),
	)
	if err != nil {
		if gated != nil {
			_ = closeResponseBody(gated.HTTP)
		}
		return "", fmt.Errorf("exchange GitHub installation token: %w", err)
	}
	resp := gated.HTTP
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", decodeHTTPError(resp)
	}
	defer resp.Body.Close()
	var tokenResponse struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResponse); err != nil {
		return "", fmt.Errorf("decode GitHub installation token: %w", err)
	}
	if tokenResponse.Token == "" || !tokenResponse.ExpiresAt.After(m.clock.Now()) {
		return "", fmt.Errorf("GitHub returned an invalid installation token")
	}
	m.mu.Lock()
	m.token = tokenResponse.Token
	m.expiresAt = tokenResponse.ExpiresAt
	m.mu.Unlock()
	return tokenResponse.Token, nil
}

func (m *InstallationTokens) authorizeApp(
	_ context.Context,
	req *http.Request,
) error {
	now := m.clock.Now()
	claims := &jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second).Truncate(time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(90 * time.Second).Truncate(time.Second)),
		Issuer:    strconv.FormatInt(m.appID, 10),
	}
	appJWT, err := m.signer.Sign(claims)
	if err != nil {
		return fmt.Errorf("sign GitHub App JWT: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+appJWT)
	return nil
}

func (m *InstallationTokens) Expiry() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.expiresAt
}
