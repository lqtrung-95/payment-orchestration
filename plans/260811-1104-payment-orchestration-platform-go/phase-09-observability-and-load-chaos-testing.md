# Phase 09 — Observability + Load/Chaos Testing

**Priority:** P1 — this phase manufactures the resume numbers · **Status:** Not started · **Week:** 12

Load testing alone gives you "2k RPS" — unremarkable. Load testing **while the PSP fails 30% of calls** gives you a claim nobody else has. Run them together or the phase is wasted.

The JD asks for "excellent experience in online positioning and problem solving." That is incident debugging. The way to demonstrate it without production is to build the tooling that makes it possible and then use it on your own injected faults.

## Key insights

- The single most impressive artifact is **one trace spanning API → outbox → Kafka → PSP → webhook → ledger settle**. Most projects lose the trace at the queue boundary. Propagating context through Kafka headers is the differentiator.
- Payment-specific metrics beat generic RED metrics. `double_charges_total` — a counter that must always be zero — is a better story than p99 latency.
- Chaos runs must be reproducible from a seed, or the numbers are anecdotes.

## Requirements

**Functional**
- Distributed tracing with context propagated across HTTP, Kafka, and worker boundaries.
- RED metrics plus a payment-domain metric set.
- One Grafana dashboard that tells the whole story.
- k6 load profiles and scripted chaos scenarios.
- A continuously-running invariant checker.

**Non-functional**
- Observability overhead under 5% of p99.
- Every published number reproducible from a documented command + seed.

## Architecture

**Tracing:** OpenTelemetry → Jaeger. Trace context injected into Kafka message headers and extracted by consumers. Spans: HTTP handler → idempotency check → DB tx → outbox write → publish → consume → PSP call → state transition → ledger posting → webhook receipt → settle.

**Metrics**

| Metric | Type | Why |
|--------|------|-----|
| `payment_requests_total{status,psp}` | counter | RED baseline |
| `payment_duration_seconds` | histogram | p50/p95/p99 |
| `double_charges_total` | counter | **Must be 0.** The headline number. |
| `lost_payments_total` | counter | **Must be 0.** Charged at PSP, absent from ledger. |
| `ledger_imbalance_total` | gauge | **Must be 0.** Continuous invariant check. |
| `psp_errors_total{psp,class}` | counter | Error taxonomy in action |
| `decline_rate{code}` | gauge | Domain-specific, not generic |
| `webhook_duplicates_total` | counter | Proves dedup is firing |
| `retry_attempts_total{topic,attempt}` | counter | Retry ladder behaviour |
| `dlq_depth` | gauge | Failure accumulation |
| `circuit_breaker_state{psp}` | gauge | Failover visibility |
| `recon_breaks_total{category}` | counter | Ties Phase 07 to ops |
| `kafka_consumer_lag` | gauge | Backpressure |

**Load profiles (k6)**
- `smoke` — 10 RPS, correctness only
- `baseline` — ramp to 2,000 RPS, healthy PSP
- `chaos` — 2,000 RPS with 30% fault rate across the Phase 03 catalogue
- `outage` — PSP fully down 90s mid-run; measure recovery and failover
- `spike` — 10× step for 30s; measure shedding and recovery
- `soak` — 500 RPS for 2h; hunt leaks and unbounded growth

**Invariant checker:** background job asserting `sum(debits) == sum(credits)` and that every PSP-confirmed charge has a ledger entry. Runs during every load test — a passing load test with a broken invariant is a failed load test.

## Related code files

**Create**
- `internal/platform/telemetry/` — tracer, meter, Kafka header propagation
- `internal/invariant/` — continuous checker
- `loadtest/` — k6 scripts per profile
- `deploy/grafana/` — provisioned dashboard JSON
- `docs/benchmarks/` — results, commands, seeds

## Implementation steps

1. OTel SDK wiring; Jaeger exporter.
2. Instrument HTTP, DB, Kafka produce/consume, PSP client.
3. Trace context into Kafka headers + extraction in consumers — verify one unbroken trace end to end.
4. Prometheus metrics; register the full table above.
5. Grafana dashboard: traffic, latency, error taxonomy, the three must-be-zero counters, DLQ depth, breaker state, consumer lag.
6. Invariant checker as a background job exporting `ledger_imbalance_total`.
7. k6 scripts for all six profiles.
8. Baseline run → record numbers.
9. Chaos run at 30% fault rate → record numbers, confirm zeros hold.
10. Outage and spike runs → record recovery times.
11. Soak run → confirm no leak, no unbounded queue growth.
12. Write `docs/benchmarks/` with the exact command and seed for every published number.

## Todo

- [ ] OTel tracing across HTTP, DB, Kafka, PSP
- [ ] Trace context propagation through Kafka headers
- [ ] Full metric set registered
- [ ] Grafana dashboard provisioned
- [ ] Continuous invariant checker
- [ ] Six k6 profiles
- [ ] Baseline run recorded
- [ ] Chaos run recorded
- [ ] Outage + spike recovery recorded
- [ ] Soak run clean
- [ ] `docs/benchmarks/` with reproducible commands + seeds

## Success criteria

- **2,000 RPS at p99 <150ms** on the baseline profile.
- **Zero double-charges, zero lost payments, zero ledger imbalance** at 2,000 RPS with 30% injected faults.
- One screenshot of a single trace spanning API → Kafka → PSP → webhook → settle.
- Full PSP outage: automatic failover, recovery under 30s, no manual intervention.
- 2h soak: flat memory, no unbounded queue growth.

## Risks

| Risk | Mitigation |
|------|-----------|
| Local hardware cannot hit 2k RPS | Publish what you actually achieve with hardware specs stated. An honest 800 RPS beats a fabricated 2,000. |
| Tracing overhead distorts results | Sample at 10% under load, 100% for demos; state which was used |
| Numbers not reproducible | Every published number gets a command + seed in `docs/benchmarks/` |

## Security considerations

- No card data, tokens, or PII in spans, logs, or metric labels. Audit span attributes explicitly.
- Metrics endpoint must not be publicly exposed.
- High-cardinality labels (transaction ID) are both a cost and a leak risk — never label by them.

## Next steps

Phase 10 — packaging all of this into artifacts a hiring manager will actually read.
