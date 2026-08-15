# Payment Orchestration Platform — Implementation Plan

**Goal:** Portfolio-grade multi-PSP payment orchestration platform in Go, engineered specifically as hiring evidence for ByteDance Global Payment (Singapore) backend roles.

## Target

| Role | Job ID | Priority |
|------|--------|----------|
| SWE (Payment Network) — Global Payment SG | A118879B | **Primary** |
| SWE (Financial Product & Solution) — Global Payment SG | A143282 | Stretch |
| MLE (Payment & Risk) | A51487 | Dropped — different career track |

Every module below maps to a noun in ByteDance's own team description: *payment acquisitions, disbursements, transaction monitoring, payment method management, foreign exchange conversion, accounting, reconciliations.*

## Stack

Go 1.23+ · CloudWeGo Hertz (HTTP) + Kitex (RPC) · PostgreSQL · Redis · Kafka · OpenTelemetry + Jaeger + Grafana · k6 · Docker Compose

CloudWeGo is ByteDance's own open-source Go stack — using it is a deliberate signal.

## Phases

| # | Phase | Weeks | Status |
|---|-------|-------|--------|
| 01 | [Go ramp + service skeleton](phase-01-go-ramp-and-service-skeleton.md) | 1–2 | Scaffold done, ramp ongoing |
| 02 | [Ledger, state machine, idempotency](phase-02-ledger-state-machine-idempotency.md) | 3–4 | **Complete** |
| 03 | [PSP abstraction + chaos simulator](phase-03-psp-abstraction-and-chaos-simulator.md) | 5 | **Complete** |
| 04 | [Outbox, Kafka, retry, DLQ](phase-04-outbox-kafka-retry-dlq.md) | 6–7 | **Complete** |
| 05 | [Webhook ingestion + dedup](phase-05-webhook-ingestion-and-dedup.md) | 8 | **Complete** |
| 06 | [Payment instrument lifecycle](phase-06-payment-instrument-lifecycle.md) | 9 | Skipped — not a dependency for 07 |
| 07 | [FX + reconciliation](phase-07-fx-and-reconciliation.md) | 10 | **Complete** |
| 08 | [Sharding + distributed transactions](phase-08-sharding-and-distributed-transactions.md) | 11 | Not started |
| 09 | [Observability + load/chaos testing](phase-09-observability-and-load-chaos-testing.md) | 12 | Not started |
| 10 | [Docs, ADRs, demo, resume assets](phase-10-docs-adrs-demo-and-resume-assets.md) | 12 | Not started |

**Shippable checkpoint: end of Phase 05.** Everything after is upside. Do not start Phase 06 if 01–05 are not complete and demoable.

## Parallel tracks (non-negotiable, run alongside all phases)

- **DSA in Go — 45–60 min/day.** Doubles as Go fluency. ByteDance runs 2–3 live algorithm rounds; a great project plus a failed coding round is still a rejection. This track outranks the project.
- **System design — 2–3 hrs/week.** Payment system design, sharding, distributed transactions, consistency models.
- **Referral hunt — ongoing.** Higher leverage than any resume line for a non-standard profile.

## Key dependencies

- Phase 02 is the spine — ledger schema, money representation, and sharding key cannot be retrofitted. Decide them there even though sharding is only *implemented* in Phase 08.
- Phase 03 (fault-injecting PSP) gates all correctness claims. Without it, retry/idempotency/webhook work is untestable and the resume numbers don't exist.
- Phase 04 depends on 02 (outbox writes in the same tx as ledger writes).
- Phase 07 reconciliation depends on 02 (ledger) and 05 (webhook-driven state).
- Phase 09 numbers depend on 03 fault injection being runtime-configurable.

## Out of scope (deliberate)

- Microservices / Kubernetes. One service, clean module boundaries. A distributed monolith is a worse signal than a well-structured single service.
- Real sandbox accounts beyond Stripe. One real PSP for credibility, three simulated for behavioural variety.
- Checkout UI beyond a thin demo surface. These JDs are backend; a polished UI buys nothing here.
- Any storage of raw PANs. Vault stores tokens and metadata only.

## Success criteria (project level)

- Ledger invariant holds under all chaos runs: `sum(debits) == sum(credits)`, always.
- Zero double-charges and zero lost payments at 2,000 req/s with 30% injected PSP fault rate.
- Reconciliation detects and classifies 100% of seeded breaks across ≥5 categories.
- One OTel trace spans API → outbox → Kafka → PSP → webhook → ledger settle.
- README communicates the above above-the-fold, in under 60 seconds of reading.
