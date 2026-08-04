package budget

import (
	"net/http"
	"testing"
)

func TestRequestAttributionBoundsLabels(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		path        string
		request     func(*http.Request) *Request
		authContext AuthContext
		endpoint    string
	}{
		{
			name: "pull metadata", path: "/repos/acme/widgets/pulls/42",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointPullRequestMetadata,
		},
		{
			name: "pull files", path: "/api/v3/repos/acme/widgets/pulls/42/files",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointPullRequestFiles,
		},
		{
			name: "check runs", path: "/repos/acme/widgets/commits/deadbeef/check-runs",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointCheckRuns,
		},
		{
			name: "repository contents", path: "/repos/acme/widgets/contents/CODEOWNERS",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointRepositoryContents,
		},
		{
			name: "app deliveries", path: "/app/hook/deliveries/123/attempts",
			request: NewAppRESTRequest, authContext: AppJWTAuth,
			endpoint: endpointAppHookDeliveries,
		},
		{
			name: "installation token", path: "/app/installations/123/access_tokens",
			request: NewAuthRequest, authContext: AppJWTAuth,
			endpoint: endpointInstallationTokens,
		},
		{
			name: "graphql", path: "/graphql",
			request: func(req *http.Request) *Request {
				return NewGraphQLRequest(req, func(*http.Response) (GraphQLRate, bool, error) {
					return GraphQLRate{}, false, nil
				})
			},
			authContext: InstallationAuth,
			endpoint:    endpointGraphQL,
		},
		{
			name:    "repo names cannot mimic app route",
			path:    "/repos/app/hook/deliveries/123",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointOther,
		},
		{
			name: "enterprise prefix", path: "/github/api/v3/repos/acme/widgets",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointRepositoryMetadata,
		},
		{
			name:    "repo names cannot mimic enterprise prefix",
			path:    "/repos/api/v3/pulls/42",
			request: NewRESTRequest, authContext: InstallationAuth,
			endpoint: endpointPullRequestMetadata,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			httpRequest, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				"https://github.example"+test.path,
				http.NoBody,
			)
			if err != nil {
				t.Fatal(err)
			}
			authContext, endpoint := requestAttribution(
				test.request(httpRequest),
				httpRequest,
			)
			if authContext != test.authContext || endpoint != test.endpoint {
				t.Fatalf(
					"attribution = %q/%q, want %q/%q",
					authContext,
					endpoint,
					test.authContext,
					test.endpoint,
				)
			}
		})
	}
}
