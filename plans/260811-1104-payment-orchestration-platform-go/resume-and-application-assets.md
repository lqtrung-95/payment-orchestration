# Resume and application assets

Copy blocks for the CV, LinkedIn, and referral messages. Every figure is
measured on this repo — see [interview notes](interview-preparation-notes.md)
for the underlying numbers and the hardware they came from.

> **This file is committed.** `plans/` is tracked by git. So is the plan that
> names the target job IDs.

## Rule for all of it

Never write a number without being able to say what machine produced it. The
throughput figures here are from a laptop with Docker limited to 4 CPUs; say so
if asked, and preferably before being asked.

---

## Long form — the project block on a CV

**Payment Orchestration Platform** — Go, PostgreSQL, Kafka, Redis, OpenTelemetry
· [github.com/lqtrung-95/payment-orchestration](https://github.com/lqtrung-95/payment-orchestration)

Multi-PSP payment orchestrator built around provider failure rather than the
happy path: a double-entry ledger with derived balances, a transactional outbox
feeding an error-class-aware retry ladder, webhook ingestion tolerant of
duplicate and out-of-order delivery, FX with banker's rounding, settlement
reconciliation across an eight-category break taxonomy, and merchant-sharded
storage with try-confirm-cancel for cross-shard money movement.

- Sustained **453 req/s with ~30% injected provider faults and zero server
  errors across 55,767 requests**, with double charges, lost payments, and
  ledger imbalance all held at zero by a checker running *during* the load
  (4-CPU Docker on a laptop; full method published in the repo).
- Resolved ambiguous provider outcomes — the timeout-after-charge case — by
  querying provider status rather than retrying or assuming, verified against a
  fault-injecting simulator that charges and then hangs.
- Enforced correctness in the database rather than in application code:
  deferred constraint triggers for double-entry balance, append-only triggers on
  ledger history, composite foreign keys binding posting currency to account
  currency, and unique indexes arbitrating webhook and consumer deduplication.
- Sharded merchants across physical databases behind a fixed 64-slot logical
  map, and implemented **TCC** for transfers spanning two databases, proving
  conservation of money under 120 concurrent cross-shard transfers.
- **224 tests** across 22 packages under `-race` against real Postgres and
  Kafka, plus **10 ADRs** each recording a rejected alternative.

## Short form — three bullets

- Built a multi-PSP payment orchestrator in Go handling **453 req/s under 30%
  injected provider failure with zero double charges and zero lost payments**,
  verified continuously during load rather than after it.
- Implemented a transactional outbox, error-class-aware retry ladder, and
  duplicate/out-of-order-tolerant webhook ingestion; every correctness invariant
  is enforced by a Postgres constraint or trigger, not by application code.
- Sharded storage by merchant across physical databases and implemented
  try-confirm-cancel for cross-shard transfers, with conservation of money
  asserted under concurrency.

## One line

Go payment orchestrator built around provider failure: double-entry ledger,
transactional outbox, error-class-aware retries, sharded storage, and TCC for
cross-shard money movement — 453 req/s under 30% fault injection with zero
double charges.

## LinkedIn headline fragment

Backend engineer (Go) · payments, distributed transactions, event-driven systems

---

## Referral message

Short, specific, and asks for one thing. Do not attach the CV to the first
message.

> Hi <name> — I'm a frontend engineer of 6 years moving into backend, and I've
> spent the last three months building a payment orchestration platform in Go to
> make that switch concrete rather than aspirational: double-entry ledger,
> transactional outbox to Kafka, webhook ingestion that tolerates duplicate and
> out-of-order delivery, FX and settlement reconciliation, and merchant sharding
> with TCC for cross-shard transfers.
>
> The part I'd point at is the failure handling — it runs against a provider
> simulator that charges the card and *then* times out, and holds zero double
> charges and zero lost payments at 453 req/s with a third of requests failing.
>
> Repo: github.com/lqtrung-95/payment-orchestration — the README is a 60-second
> read.
>
> Would you be open to referring me for <role>, or pointing me at whoever owns
> it?

## Cover-letter paragraph

> I've been a frontend engineer for six years and decided that the way to move
> into payments was to build one rather than to describe wanting to. The result
> is a multi-PSP orchestrator in Go, organised entirely around the failure modes
> that cost money: a provider that charges the card and then times out, a
> webhook delivered five times out of order before the API call returned, a
> settlement file that disagrees with the ledger by three cents. It holds a
> double-entry ledger with balances derived from postings, moves work off the
> request path through a transactional outbox, retries by error class rather
> than by count, and coordinates money across sharded databases with
> try-confirm-cancel. Every claim in the README is asserted by a test in the
> repo — including the ones that record what it does *not* do.

---

## What to link, in order

1. **The README.** Written for 60 seconds: what it is, the diagram, the failure
   table, the measured numbers.
2. **[`docs/adr/`](../../docs/adr/)** — ten decisions, each naming a rejected
   alternative. This is the artefact that reads as senior.
3. **[`docs/benchmarks/`](../../docs/benchmarks/)** — the numbers with the
   hardware and the commands, including the targets that were missed.
4. **The demo GIF**, embedded at the top of the README.

## Still to do — user actions only

- [ ] **Record the 3-minute video.** Script is in
      [phase 10](phase-10-docs-adrs-demo-and-resume-assets.md); one take, rough
      edit, ship it. Check the recording for local credentials on screen.
- [ ] **Pin the repo** on the GitHub profile.
- [ ] **Decide whether `plans/` should be public.** It names the target job IDs
      and frames the project as hiring evidence; a reviewer at another company
      would read that. Either `.gitignore` it or leave it deliberately.
- [ ] **Send the referral messages.** Higher leverage than any further code.
