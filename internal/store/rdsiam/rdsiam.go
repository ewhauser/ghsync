// Package rdsiam generates short-lived Amazon RDS IAM database auth tokens.
package rdsiam

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
)

// TokenProvider generates an authentication token for one database
// connection. Implementations must not cache tokens.
type TokenProvider interface {
	Token(ctx context.Context, endpoint, user string) (string, error)
}

// Provider generates tokens from one resolved AWS SDK configuration.
type Provider struct {
	config aws.Config
}

// New loads the default AWS SDK region and credential chain and verifies that
// credentials are resolvable. The resolved configuration is reused, but every
// call to Token generates a new database authentication token.
func New(ctx context.Context) (*Provider, error) {
	config, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load default AWS configuration: %w", err)
	}
	return newProvider(ctx, &config)
}

func newProvider(ctx context.Context, config *aws.Config) (*Provider, error) {
	if config.Region == "" {
		return nil, fmt.Errorf(
			"load default AWS configuration: region is required (set AWS_REGION or configure a profile)",
		)
	}
	if config.Credentials == nil {
		return nil, fmt.Errorf("load default AWS configuration: credentials provider is missing")
	}
	if _, err := config.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("resolve default AWS credentials: %w", err)
	}
	return &Provider{config: *config}, nil
}

// Token generates a fresh RDS IAM authentication token for endpoint and user.
func (p *Provider) Token(
	ctx context.Context,
	endpoint string,
	user string,
) (string, error) {
	token, err := auth.BuildAuthToken(
		ctx,
		endpoint,
		p.config.Region,
		user,
		p.config.Credentials,
	)
	if err != nil {
		return "", fmt.Errorf("build RDS IAM authentication token: %w", err)
	}
	return token, nil
}
