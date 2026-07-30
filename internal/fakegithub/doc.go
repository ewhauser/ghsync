// Package fakegithub provides a scriptable GitHub HTTP server for integration
// tests and local load-replay runs.
//
// Options are applied in declaration order. Options that cannot produce a
// meaningful server configuration panic immediately (for example, negative
// rate limits, non-positive token TTLs, nil hooks, and incomplete App
// authentication).
//
// RateLimitStep scripts are consumed once per response in their resource
// family: REST and GraphQL advance independently. An unscripted conditional
// 304 refunds its provisional one-unit REST charge; a scripted 304 does not
// refund because the scripted remaining value is authoritative. Installation
// token endpoints use GitHub's separate App bucket, so they are exempt from
// REST/GraphQL rate-step consumption while still participating in the shared
// concurrency ceiling.
//
// App-hook deliveries are listed newest first. Webhook emission uses the
// fake's internal HTTP client, records GitHub-side delivery state even when
// the target rejects the request, and is available only inside this internal
// module.
package fakegithub
