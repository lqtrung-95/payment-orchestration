# Phase 09 — Observability + Load/Chaos Testing

**Priority:** P1 — this phase manufactures the resume numbers · **Status:** Partly complete · **Week:** 12 · **Worked on 2026-08-15**

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

- [ ] OTel tracing across HTTP, DB, Kafka, PSP — **not started**
- [ ] Trace context propagation through Kafka headers — **not started**
- [x] Full metric set registered, exposed at `/metrics`
- [ ] Grafana dashboard provisioned — Prometheus scrapes, no dashboard yet
- [x] Continuous invariant checker, tested against seeded violations
- [x] Five k6 profiles (smoke, baseline, chaos, spike, soak)
- [x] Baseline run executed — **result not publishable**, see below
- [ ] Chaos run recorded
- [ ] Outage + spike recovery recorded
- [ ] Soak run clean — run for minutes, not hours
- [x] `docs/benchmarks/` with method, results, and why throughput is withheld

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

## Worked on 2026-08-15

**Built and verified**

- `/metrics` exposing a payment-domain set, including the three must-be-zero
  gauges. Cardinality is bounded deliberately — nothing labelled by transaction
  id, merchant, or reference.
- A continuous invariant checker for ledger imbalance, double captures, and
  captured-with-no-entry. It is tested against **seeded** violations, which is
  the test that matters: a checker that always returns zero passes every load
  test ever run against it.
- A depth sampler for outbox backlog, per-topic consumer lag, and DLQ depth —
  the visibility whose absence meant a stalled consumer had to be found by hand
  in phase 05.
- Five k6 profiles sharing one request path, with thresholds as assertions so a
  degraded run exits non-zero.

**Measured**

164,933 payments created across the runs, with zero ledger imbalance, zero
double charges, and zero lost payments. Four 5xx, all statement timeouts on the
idempotency claim under severe host contention — the system shed work rather
than corrupting state. Median latency on a clean database at 200 rps: 7.65ms.

**Not measured, and why**

No throughput figure is published. The host sat at a load average of 46 on 10
cores throughout, dominated by unrelated desktop applications, and repeated runs
of the identical profile produced between 33 and 428 requests/second. That
spread is a measurement of a laptop's spare capacity, not of this system. The
phase's own risk table said an honest 800 beats a fabricated 2,000; this goes
further — an honest *nothing* beats a number that is really noise.

Two things the runs did establish: raising the connection pool from 20 to 50
moved throughput by 3%, so Postgres is the ceiling rather than the pool; and
disabling the invariant checker made the latency tail *worse*, ruling out the
obvious suspect for a hundredfold median-to-p99 spread.

**Deferred**

- Tracing entirely. The single end-to-end trace is the phase's most distinctive
  artifact and it does not exist.
- Grafana dashboard — Prometheus scrapes, nothing renders it.
- Chaos, outage, and soak runs to completion.
- PSP error, decline, retry, breaker, and webhook counters are registered but
  not yet incremented from their call sites; only HTTP, the invariants, and the
  depths are wired.

## Measured on 2026-08-16

Full numbers, hardware, and commands in [`docs/benchmarks/`](../../docs/benchmarks/README.md).

| Run | Result |
|---|---|
| Steady 200 req/s, healthy | 192 req/s, median 7.7ms, p99 1.73s, 0 errors |
| Ramp to 1,000 req/s, healthy | 416 req/s sustained, p95 3.28s, 0 errors |
| Chaos, ~30% fault rate | 453 req/s, 0 server errors in 55,767 requests |
| Invariants during the chaos run | double charges 0 · lost payments 0 · imbalance 0 |
| Provider charges vs transactions reaching it | 177 vs 177 |

**One trace spans HTTP → outbox → Kafka → worker → provider**, across two
processes. Crossing Kafka needs the context in record headers; crossing the
outbox needs it in the database row, because the handler commits and returns
while the relay publishes later in another goroutine.

## The finding

**Ingestion scales; processing does not.** At the end of the chaos run the API
had accepted 55,767 payments and the pipeline had authorised 175, with 42,409
outbox rows pending and 13,200 messages of consumer lag.

The cause is architectural: the worker consumes with a single goroutine walking
every assigned partition in order, each message making a synchronous provider
call. Under injected faults those calls include multi-second hangs, so the drain
rate collapses to roughly the reciprocal of provider latency however many
partitions exist.

Accepting durably and draining slowly is the correct behaviour for a queue —
it is why the outbox exists — but the drain rate is a real limit and it is not
horizontal. Concurrent per-partition consumers would fix it and are deliberately
not built: that changes the ordering the retry ladder depends on, and a delay
tier that stops being ordered by time stops working.

None of this was visible before this phase. The same backlog stalled twenty
payments during the phase 05 demo and was found by querying Kafka by hand.

## Bugs found

**The outbox relay silently stopped publishing.** Adding `traceparent` to the
claim query's RETURNING clause without adding the matching Scan destination
produced `9 fields, 8 destinations` on every sweep. Caught because the phase 04
work logs every sweep failure rather than only when parking — the failure was
loud, and the payment simply sat at `created`.

**Schema URL conflict at startup.** `resource.Merge` refuses to combine
resources built against different semconv versions, so importing v1.26.0
alongside an SDK on v1.43.0 aborted boot. Correct behaviour, and it surfaced
immediately rather than as a missing attribute later.

## Deferred

- **No Grafana dashboard.** Prometheus scrapes and the metrics are correct, but
  the provisioned dashboard JSON is not written, so the story has to be told
  from raw queries.
- **No DB spans.** Traces cover HTTP, Kafka, and the provider; individual
  queries are not instrumented, so a slow statement is invisible in a trace.
- **Outage, spike, and soak runs.** Profiles exist and were never executed, so
  nothing is claimed about failover timing, shedding, or leaks.
- **The plan's targets are not met.** 2,000 req/s at p99 < 150 ms is not close
  on this hardware, and the honest figures are published instead.
- **No PSP-level counters wired.** `psp_errors_total`, `payment_declines_total`,
  `payment_retry_attempts_total`, `webhooks_received_total`, `recon_breaks_total`
  and the breaker gauge are registered but nothing increments them yet.

## Next steps

Phase 10 — packaging all of this into artifacts a hiring manager will actually read.
