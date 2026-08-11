# Phase 10 — Docs, ADRs, Demo, Resume Assets

**Priority:** P0 — do not skip · **Status:** Not started · **Week:** 12 (runs alongside Phase 09)

Nobody clones your repo. A hiring manager gives it 60 seconds. This phase is the difference between three months of work landing and three months of work being invisible.

ADRs are also the deliverable most aligned with the senior framing: at 6+ years the expectation is judgment, and an ADR is judgment in written form.

## Key insights

- Write ADRs **as you go**, not reconstructed at the end. Reconstructed ADRs read as reconstructed.
- The README's first screen must carry: what it is, the architecture diagram, the failure-mode table, and the chaos numbers. Everything else is below the fold.
- A 3-minute video of you killing the PSP mid-payment and showing recovery is worth more than the entire codebase in a screening call.
- Rejected alternatives are what make an ADR credible. An ADR with no rejected options is a changelog entry.

## Requirements

- README that communicates the project in under 60 seconds.
- Architecture doc with Mermaid diagrams.
- ADR set covering every non-obvious decision.
- 3-minute demo video.
- Interview preparation notes.
- Resume bullets in long and short form.

## Deliverables

**README (above the fold, in this order)**
1. One-sentence description.
2. Architecture diagram.
3. The failure-mode table — every fault injected and how the system answers it. This is the hook.
4. The chaos numbers, with hardware stated.
5. `make demo` — one command to a running system.

**`docs/system-architecture.md`** — component diagram, payment sequence diagram, state machines (transaction + instrument), sharding topology, TCC flow. Mermaid v11 syntax.

**ADR set** (`docs/adr/`)

| ADR | Decision |
|-----|----------|
| 0001 | Money as int64 minor units + currency |
| 0002 | Double-entry ledger; balances derived, never stored authoritatively |
| 0003 | Shard key = `merchant_id`; rejected `user_id`, `transaction_id` |
| 0004 | Idempotency scope, fingerprinting, 24h TTL |
| 0005 | Transactional outbox over CDC/Debezium |
| 0006 | At-least-once delivery + idempotent consumers over "exactly-once" |
| 0007 | Kafka partitioning by merchant for per-entity ordering |
| 0008 | Error-class-aware retry policy; never retry declines |
| 0009 | TCC over Saga and 2PC for cross-shard transfers |
| 0010 | DB unique index authoritative for webhook dedup; Redis advisory only |
| 0011 | Cache as optimization, never source of truth |
| 0012 | Reconciliation break taxonomy + auto-resolve tolerances |
| 0013 | Go + CloudWeGo (Hertz/Kitex) stack selection |
| 0014 | Single service with module boundaries over microservices |

Each: context → decision → rejected alternatives → consequences. Rejected alternatives are the part that matters.

**Demo video (3 min, scripted)**
1. Normal payment, end to end. (20s)
2. Open the Jaeger trace — API through Kafka to webhook, unbroken. (30s)
3. Switch faults to `chaos`; run load; show `double_charges_total` flat at zero. (45s)
4. Kill `pspsim` mid-payment; show circuit breaker open, failover, recovery. (45s)
5. Run reconciliation on a settlement file with seeded breaks; show the classified report. (40s)

**Interview prep notes** (`docs/interview-notes.md`, private) — for each phase: what you built, what broke, what you'd do differently at 100× scale. Pre-write answers to: Why TCC not Saga? Why shard on merchant? How do you guarantee exactly-once? What happens when the PSP times out after charging? How would you scale reconciliation to 100M rows/day? What's your biggest design regret here?

That last question is the one that separates senior from mid. Have a real answer.

## Implementation steps

1. Backfill ADRs 0001–0014 (write future ones at decision time).
2. `docs/system-architecture.md` with Mermaid v11.
3. README, above-the-fold discipline enforced.
4. Failure-mode table generated from the Phase 03 catalogue + Phase 05/09 test results.
5. `make demo` — seeds data, starts everything, opens the dashboard.
6. Script, record, and edit the video.
7. `docs/benchmarks/` finalized with reproducible commands.
8. Interview notes.
9. Resume bullets, long and short.
10. Prune: delete dead code, TODOs, commented blocks. A reviewer reads the repo as a work sample.

## Todo

- [ ] ADRs 0001–0014
- [ ] `docs/system-architecture.md` with diagrams
- [ ] README with above-the-fold discipline
- [ ] Failure-mode table
- [ ] `make demo`
- [ ] Demo video recorded + edited
- [ ] Benchmarks documented + reproducible
- [ ] Interview notes
- [ ] Resume bullets
- [ ] Repo cleanup pass

## Success criteria

- A payments engineer reads the README for 60 seconds and can state what is hard about the project.
- Every ADR names at least one seriously-considered rejected alternative.
- `make demo` works from a clean clone on a machine that has never run it.
- You can answer all six prep questions without notes.

## Risks

| Risk | Mitigation |
|------|-----------|
| Docs deferred until "after the code" and never written | ADRs are written at decision time, from Phase 02 onward. This phase only backfills. |
| README becomes a feature list | Enforce the four above-the-fold items; everything else goes below |
| Video perfectionism | One take, rough edit, ship it |

## Security considerations

- Scrub the repo for secrets before it goes public — history included, not just HEAD.
- No real card numbers in test data or demo footage, including test PANs on screen.
- Verify the demo video does not expose local credentials or tokens.

## Next steps

Apply. Payment Network (A118879B) first. Chase the referral in parallel — it outweighs everything in this document.
