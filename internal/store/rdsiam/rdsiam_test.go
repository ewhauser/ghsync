package rdsiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type credentialProviderFunc func(context.Context) (aws.Credentials, error)

func (f credentialProviderFunc) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return f(ctx)
}

func TestNewProviderValidatesRegionAndCredentialsBeforeConnect(t *testing.T) {
	t.Parallel()

	credentialsCalled := false
	credentials := credentialProviderFunc(func(context.Context) (aws.Credentials, error) {
		credentialsCalled = true
		return aws.Credentials{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		}, nil
	})
	if _, err := newProvider(t.Context(), &aws.Config{
		Credentials: credentials,
	}); err == nil || !strings.Contains(err.Error(), "region is required") {
		t.Fatalf("missing region error = %v", err)
	}
	if credentialsCalled {
		t.Fatal("credential chain ran before missing-region validation")
	}

	sentinel := errors.New("offline credential failure")
	if _, err := newProvider(t.Context(), &aws.Config{
		Region: "us-east-1",
		Credentials: credentialProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{}, sentinel
		}),
	}); !errors.Is(err, sentinel) {
		t.Fatalf("credential resolution error = %v, want sentinel", err)
	}
}
