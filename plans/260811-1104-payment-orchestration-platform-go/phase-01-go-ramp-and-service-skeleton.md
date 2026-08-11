# Phase 01 — Go Ramp + Service Skeleton

**Priority:** P0 · **Status:** Scaffold complete — Go ramp ongoing · **Weeks:** 1–2

Get productive in Go and stand up a running service with infra, migrations, logging, and CI. No payment logic yet.

## Key insights

- Go ramp for a fluent TS developer is ~2 weeks to competent, longer to idiomatic. Concurrency (`context`, goroutine lifecycle, channel ownership) is where TS habits mislead.
- Running DSA problems in Go during this phase is the fastest ramp — it also feeds the interview track.
- CloudWeGo Hertz/Kitex are ByteDance's own frameworks. Adopt them now; retrofitting the HTTP layer later is wasted work.

## Requirements

**Functional**
- Service boots, serves `/healthz` and `/readyz`, connects to Postgres/Redis/Kafka.
- Migrations run up/down deterministically.
- Structured JSON logs with request ID propagation.

**Non-functional**
- `docker compose up` brings the entire stack live from a clean clone.
- CI runs build, `go vet`, lint, and tests on every push.

## Architecture

```
cmd/orchestrator/        service entrypoint
internal/
  config/                env + file config loading
  transport/http/        Hertz handlers, middleware
  platform/postgres/     pgx pool, tx helpers
  platform/redis/
  platform/kafka/
  platform/telemetry/    logger, tracer, metrics bootstrap
migrations/              golang-migrate SQL files
deploy/docker-compose.yml
```

Single binary, module boundaries enforced by package layout. No microservices.

## Related code files

**Create:** everything above, plus `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml`

## Implementation steps

1. Go fundamentals: interfaces, error wrapping (`errors.Is/As`), `context` propagation, goroutine + channel patterns, `sync` primitives, generics basics, table-driven tests.
2. Init module, adopt `golangci-lint`, write `Makefile` (`run`, `test`, `lint`, `migrate`, `up`, `down`).
3. Docker Compose: Postgres 16, Redis 7, Kafka (KRaft mode, single broker), Jaeger, Prometheus, Grafana.
4. Config loader — env-first, typed struct, fail-fast validation on boot.
5. Hertz server + middleware chain: request ID, structured logging, panic recovery, timeout.
6. pgx pool + a `WithTx(ctx, fn)` helper — every later phase depends on this for outbox atomicity.
7. golang-migrate wiring + first migration (schema bootstrap only).
8. `slog` JSON logger, request ID in every line.
9. GitHub Actions CI.
10. Port one small existing project to Go as a ramp exercise.

## Todo

- [ ] Go fundamentals pass (concurrency, errors, context, testing) — **the remaining work in this phase**
- [x] Module + lint + Makefile
- [x] Docker Compose stack green
- [x] Config loader with fail-fast validation
- [x] Hertz server + middleware chain
- [x] pgx pool + `WithTx` helper
- [x] Migration tooling + bootstrap migration
- [x] Structured logging with request ID
- [x] CI pipeline written (unverified — no remote yet)
- [ ] Ramp exercise complete

## Verified on 2026-08-11

Toolchain: Go 1.26.5, Colima 0.10.3 + Docker 29.7.2, golangci-lint 2.12.2.

- `docker compose up -d --wait` → postgres, redis, kafka all healthy in 39s
- `make migrate-up` → version=1; `down` then `up` round-trips cleanly
- `GET /healthz` → 200; `GET /readyz` → 200 with all three dependencies ok
- Redis stopped → `/readyz` 503 `{"redis":"unavailable","status":"degraded"}` while `/healthz` stays 200; recovers automatically on restart
- Inbound `X-Request-ID` echoed when well-formed; header-injection attempt (`bad id with spaces`) discarded and replaced with a fresh UUID
- SIGTERM → ordered drain, "shutdown complete", exit 0, port released
- `vet` clean, `golangci-lint` 0 issues, tests pass under `-race`

## Success criteria

- Clean clone → `make up` → healthy service in under 2 minutes.
- CI green.
- You can write a table-driven Go test without reference material.

## Risks

| Risk | Mitigation |
|------|-----------|
| Go ramp overruns and eats project time | Timebox to 2 weeks; move on with imperfect Go, refactor later |
| CloudWeGo docs thinner than mainstream Go frameworks | Fall back to `net/http` + chi if Hertz blocks >2 days; keep Kitex for the RPC layer only |
| Yak-shaving on tooling | Tooling is done when CI is green — stop there |

## Security considerations

- No secrets in the repo. `.env.example` only; real `.env` gitignored from commit one.
- Pin base images by digest in Compose.

## Next steps

Phase 02 — the spine. Do not proceed until `WithTx` is solid; the outbox pattern depends entirely on it.
