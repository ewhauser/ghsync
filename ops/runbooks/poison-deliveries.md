# Poison deliveries

## Symptoms

- `FrontierPoisonDeliveryParked`.
- `frontier_c_i5_parked_deliveries` is nonzero.
- Dispatch logs repeat a classification error through
  `DISPATCH_MAX_ATTEMPTS`.

## Diagnosis

```sql
SELECT delivery_guid, event, received_at, attempts, last_error,
       payload_pruned_at
FROM webhook_deliveries
WHERE status = 'parked'
ORDER BY received_at;

SELECT event, last_error, count(*)
FROM webhook_deliveries
WHERE status = 'parked'
GROUP BY event, last_error ORDER BY count(*) DESC;
```

If payload is pruned, use GitHub's delivery view; never reconstruct it from
`last_error`.

## Remediation

Fix `DISPATCH_RULES_FILE`, roll dispatch, then test one GUID:

```sh
frontier-syncd requeue --guid=DELIVERY_GUID
frontier-syncd serve --roles=dispatch
```

After it processes cleanly:

```sh
# Replay at most 100 exact GUIDs:
frontier-syncd requeue --guids=guid-1,guid-2

# Or replay the next bounded batch sharing one diagnosed signature:
frontier-syncd requeue \
  --event=pull_request \
  --error-contains='unsupported action reopened'
```

## Escalation

Escalate if the delivery parks again, its payload is unavailable, multiple
event families are affected, or parking threatens the C-Q2 age SLO.
