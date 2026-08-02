// Package main implements example-api, a reference example of the ghsync
// zero-duplication consumer pattern. It is not a production service.
//
// The mirror's public tables remain the only entity backing store. The process
// persists only its pkg/streamclient cursor and retains entity JSON only in a
// bounded recent-event ring used for SSE catch-up. Each subscriber also has a
// bounded queue. A subscriber that cannot keep up receives a resync advisory
// and is disconnected instead of backpressuring the single process tailer.
// A deployment would connect with the ghsync_consumer-grade role described in
// db/CONTRACT.md, or another role with the same public-contract grants.
package main
