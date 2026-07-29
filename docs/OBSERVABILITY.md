# OpenTelemetry tracing

`ghsyncd` exports traces over OTLP/HTTP while retaining its existing
OpenTelemetry-to-Prometheus metrics endpoint. Tracing is opt-in: when
`OTEL_TRACES_EXPORTER` is unset or `none`, the daemon uses a no-op trace
provider and makes no collector connection.

## Configuration

Set `OTEL_TRACES_EXPORTER=otlp` to enable tracing. The OTLP/HTTP exporter reads
the standard variables below.

| Variable | Default | Meaning |
|---|---|---|
| `OTEL_TRACES_EXPORTER` | `none` in `ghsyncd` | `otlp` enables export; `none` or unset disables it. |
| `OTEL_SERVICE_NAME` | `ghsyncd` | Stable service name attached to every span. |
| `OTEL_RESOURCE_ATTRIBUTES` | empty | Comma-separated resource attributes, such as `deployment.environment.name=production`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | exporter default | Base OTLP/HTTP endpoint; `/v1/traces` is appended. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | exporter default | Trace-specific OTLP/HTTP endpoint. |
| `OTEL_EXPORTER_OTLP_HEADERS` | empty | Collector authentication headers. Treat the value as a secret. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | exporter default | Export request timeout in milliseconds. |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | none | Set to `gzip` when supported by the collector. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off`, or `parentbased_traceidratio`. |
| `OTEL_TRACES_SAMPLER_ARG` | `1` | Sampling ratio from `0` through `1` for ratio samplers. |

An explicitly invalid exporter or sampler fails startup instead of silently
running with an unintended policy. Export happens in batches, and shutdown
flushes after the HTTP server, River workers, and background roles stop.

For production, start with
`OTEL_TRACES_SAMPLER=parentbased_traceidratio` and a ratio between `0.01` and
`0.05`. Prefer collector-side tail sampling when errors and slow operations
must be retained independently of the head-sampling ratio.

## Trace topology

Durable asynchronous boundaries use span links:

```text
github.webhook
  ..link..> ghsync.dispatch.batch
                -> river.insert_many
                     ..link..> river.work/<job-kind>
                                      -> ghsync.github.admission
                                           -> HTTP <method>
                                      -> query <operation>
```

The ingress handler stores only W3C `traceparent` and `tracestate` on the
private `webhook_deliveries` row. A dispatcher batch links to each distinct
delivery context. River's `otelriver` plugin then stores W3C context in
`river_job.metadata` and links each worker span to its insert span. These
relationships may appear under a backend's **Links** view rather than as one
parent/child tree.

The principal application spans are:

| Span | Purpose |
|---|---|
| `github.webhook` | Verified inbound webhook request and durable acknowledgement. |
| `ghsync.dispatch.batch` | Claimed delivery batch, classification, and transactional River inserts. |
| `river.insert_many` | River job insert or insert batch, including unique-job skips. |
| `river.work/<job-kind>` | River worker execution, attempts, snoozes, failures, and queue. |
| `ghsync.github.admission` | Time waiting for the installation-wide budget gate. |
| HTTP client spans | Network time and status for GitHub REST, GraphQL, token, and deliveries calls. |
| pgx query spans | Postgres operation time and SQLSTATE failures. |
| `ghsync.deriver.pass` | A non-empty dirty-scope derivation pass. |
| `ghsync.stream.retention` | One bounded change-event retention pass. |
| `ghsync.command.*` | `migrate`, `backfill`, and `requeue` operator commands. |

River also emits additive `river_*` metrics through the existing process-local
Prometheus registry. Generic OpenTelemetry messaging metrics remain disabled so
the constraint-named `ghsync_*` metrics continue to be the alerting contract.

## Data handling

Trace attributes may contain role, queue, job kind, attempt, webhook event,
request class, resource class, batch size, status, and timing information.

Do not add webhook bodies, stored request headers, GitHub response bodies,
authorization tokens, private keys, SQL parameters, or complete River job
arguments to spans. The pgx instrumentation deliberately does not enable query
parameter capture. Outbound propagation uses W3C Trace Context only; baggage is
not forwarded to GitHub.

## Local inspection

Start the pinned Jaeger development profile:

```sh
docker compose --profile tracing up -d postgres jaeger

export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_TRACES_SAMPLER=parentbased_always_on
```

Run `ghsyncd` normally, then open the Jaeger UI at
<http://127.0.0.1:16686>. The Jaeger service uses in-memory storage and is for
local inspection only.
