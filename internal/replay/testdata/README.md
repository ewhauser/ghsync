# CI recording

`workerd-week.ndjson` records `cloudflare/workerd` from 2026-07-21T00:00:00Z
through 2026-07-28T00:00:00Z (the inclusive date window July 21–27), with
20% deterministic stack synthesis using seed 1. It was generated from the
repository root with:

```sh
ghrecord_token="$(gh auth token)"
go run ./cmd/ghrecord \
  --repo cloudflare/workerd \
  --since 2026-07-21 \
  --until 2026-07-27 \
  --token "$ghrecord_token" \
  --out internal/replay/testdata/workerd-week.ndjson \
  --synthesize-stacks=20 \
  --seed=1
```
