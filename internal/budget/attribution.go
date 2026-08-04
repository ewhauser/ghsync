package budget

import (
	"net/http"
	"strings"
)

const (
	endpointOther                    = "other"
	endpointGraphQL                  = "graphql"
	endpointInstallationRepositories = "installation_repositories"
	endpointRepositoryMetadata       = "repository_metadata"
	endpointRepositoryRules          = "repository_rules"
	endpointStacks                   = "stacks"
	endpointPullRequestList          = "pull_request_list"
	endpointPullRequestMetadata      = "pull_request_metadata"
	endpointPullRequestFiles         = "pull_request_files"
	endpointRepositoryContents       = "repository_contents"
	endpointCheckRuns                = "check_runs"
	endpointAppHookDeliveries        = "app_hook_deliveries"
	endpointInstallationTokens       = "installation_tokens"
)

// requestAttribution reduces a request to cardinality-bounded labels before
// the public observation hook sees it. Paths, repository names, and IDs never
// leave the gate.
func requestAttribution(
	req *Request,
	httpReq *http.Request,
) (AuthContext, string) {
	authContext := req.authContext
	segments := strings.FieldsFunc(httpReq.URL.Path, func(value rune) bool {
		return value == '/'
	})
	if req.resource == GraphQL {
		return authContext, endpointGraphQL
	}
	segments = githubAPISegments(segments)
	if hasSegmentPrefix(segments, "app", "hook", "deliveries") {
		return authContext, endpointAppHookDeliveries
	}
	if len(segments) >= 4 && hasSegmentPrefix(
		segments,
		"app",
		"installations",
	) && segments[3] == "access_tokens" {
		return authContext, endpointInstallationTokens
	}
	if hasSegmentPrefix(segments, "installation", "repositories") {
		return authContext, endpointInstallationRepositories
	}
	if len(segments) < 3 || segments[0] != "repos" {
		return authContext, endpointOther
	}
	tail := segments[3:]
	if len(tail) == 0 {
		return authContext, endpointRepositoryMetadata
	}
	switch tail[0] {
	case "rulesets":
		return authContext, endpointRepositoryRules
	case "stacks":
		return authContext, endpointStacks
	case "pulls":
		if len(tail) == 1 {
			return authContext, endpointPullRequestList
		}
		if len(tail) >= 3 && tail[2] == "files" {
			return authContext, endpointPullRequestFiles
		}
		return authContext, endpointPullRequestMetadata
	case "contents":
		return authContext, endpointRepositoryContents
	case "commits":
		if len(tail) >= 3 && tail[2] == "check-runs" {
			return authContext, endpointCheckRuns
		}
	}
	return authContext, endpointOther
}

func githubAPISegments(segments []string) []string {
	for index := 0; index+2 < len(segments); index++ {
		if segments[index] == "api" && segments[index+1] == "v3" &&
			(segments[index+2] == "repos" ||
				segments[index+2] == "app" ||
				segments[index+2] == "installation") {
			return segments[index+2:]
		}
	}
	return segments
}

func hasSegmentPrefix(segments []string, want ...string) bool {
	if len(segments) < len(want) {
		return false
	}
	for index := range want {
		if segments[index] != want[index] {
			return false
		}
	}
	return true
}
