# Cache trust dashboard

The C-O4 dashboard is one screen with one question in its title: **Is the
cache trustworthy right now?** Use a five-minute default range. Red means no;
yellow means correct now but losing safety margin.

## Row 1: trust verdict

Use four large stat panels:

1. **Freshness:** C-Q2 p95/p99 from
   `ghsync_c_q2_event_to_cache_latency_seconds` beside 20s/60s limits, plus
   `ghsync_c_q2_oldest_unprocessed_delivery_age_seconds` and
   `ghsync_c_q2_oldest_outstanding_generation_age_seconds`.
2. **Bounded staleness:** table
   `ghsync_c_r1_cache_staleness_seconds` /
   `ghsync_c_r1_staleness_bound_seconds` by `entity_class`.
3. **Correctness:** open
   `ghsync_c_o3_drift_findings{state="open"}` and
   `ghsync_c_i5_parked_deliveries`. Either above zero is red.
4. **Stream safety:** `ghsync_c_s2_watermark_lag_sequences`,
   `ghsync_c_s2_watermark_age_seconds`, and maximum per-stream consumer
   outstanding-event count/age.

This row alone answers the on-call question. Link each stat to its runbook.

## Row 2: freshness path

- Event-to-cache p50/p95/p99 with fixed 20s and 60s lines.
- Queue depth by queue, oldest delivery age, and dispatch batch-size heatmap.
- Fetch rate by kind, CAS reject ratio, tombstones, and the 15-minute
  sweep-scoped ratio below, with a fixed 0.80 line. The denominator gate
  leaves quiet windows blank; the alert separately treats absent counters as
  a signal failure.

  ```promql
  (
    sum(increase(ghsync_c_b4_conditional_304s_total{class="sweep",resource="rest"}[15m]))
    /
    sum(increase(ghsync_c_b4_conditional_requests_total{class="sweep",resource="rest"}[15m]))
  )
  and
  sum(increase(ghsync_c_b4_conditional_requests_total{class="sweep",resource="rest"}[15m])) > 0
  ```

## Row 3: reconciliation and budget

- Server-authoritative budget remaining by class/resource; request and
  starvation rates. Starvation, not a post-request floor comparison, is the
  admission-safety signal.
- `ghsync_c_b2_gate_closed` as a boolean with continuous closed duration
  supplied by the alert `for` clause.
- Sweep duration and period by kind, gap-heal requests, drift state, and
  pruner deletes. Include durable sweep/drift success count, drift sample
  count, and age; `-1` or absent is red.

## Row 4: stream and derivation

- Watermark sequence lag/age, durable watermarker success age,
  retention-eligible `ghsync_c_s7_prunable_outbox_depth`, and advance rate.
- Consumer own-stream outstanding event count/age and resync rate.
- Deriver pass duration and dirty backlog.

Keep active alerts down the right edge. Do not split the view by daemon role:
`ghsync_c_o4_role_enabled` identifies each scraped process. Only the
dedicated `metrics` role reads authoritative aggregate Postgres state; other
roles expose local counters. Every trust panel has an absence rule, so a
missing collector or required role is red rather than a blank success.

## Trace links

Link freshness, queue, budget, deriver, and stream panels to the trace backend
with the same time window. Useful entry spans are `github.webhook`,
`ghsync.dispatch.batch`, `river.work/<job-kind>`,
`ghsync.github.admission`, and `ghsync.deriver.pass`. Webhook-to-dispatch and
River insert-to-worker relationships are asynchronous span links, so trace
queries and UI navigation must include linked spans rather than assuming one
parent/child tree. See [`../docs/OBSERVABILITY.md`](../docs/OBSERVABILITY.md).
