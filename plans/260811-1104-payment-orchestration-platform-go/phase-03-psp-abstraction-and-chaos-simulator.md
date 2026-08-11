# Phase 03 — PSP Abstraction + Fault-Injecting Simulator

**Priority:** P0 · **Status:** Complete · **Week:** 5

The centerpiece, not a test fixture. Every impressive claim this project makes comes from things going wrong on purpose.

## Key insights

- Two adapters behind one interface with identical semantics proves nothing. The PSPs must be **structurally different** — that is what forces real orchestration rather than a thin passthrough.
- The single hardest scenario in payments: **the PSP times out after successfully charging the card.** If you cannot demo that, the project is theater.
- Fault injection must be runtime-configurable, not compile-time. The demo is toggling failures live while traffic runs.

## Requirements

**Functional**
- One `PSPAdapter` interface: `Authorize`, `Capture`, `Refund`, `Void`, `GetStatus`.
- Four providers with genuinely different semantics.
- Control API to enable/disable/tune faults at runtime.

**Non-functional**
- Simulators are deterministic under a seed, so chaos runs are reproducible.
- Per-PSP timeout and connection budgets, independently configurable.

## Architecture

**Providers**

| Provider | Semantics | What it forces you to solve |
|----------|-----------|-----------------------------|
| `stripe-sandbox` | Real API, real webhooks | Credibility; real signature verification |
| `psp-sync-sim` | Synchronous auth+capture | Baseline happy path |
| `psp-async-sim` | API returns `pending`; outcome only via webhook | Transaction parked in non-terminal state indefinitely |
| `psp-redirect-sim` | Returns a redirect URL (3DS-like), resolves later | Multi-step flows, mid-flow abandonment, expiry |

**Fault catalogue** (each independently probability-tunable)

| Fault | Why it matters |
|-------|----------------|
| `timeout_after_success` | Charge succeeded, caller never learned. The flagship problem. |
| `error_5xx_after_success` | Same class, different shape |
| `duplicate_webhook` | Forces dedup (Phase 05) |
| `out_of_order_webhook` | Forces state-machine guards on stale events |
| `webhook_before_response` | Webhook lands before the API call returns |
| `outage_window` | PSP down N seconds → circuit breaker, failover |
| `slow_response` | Latency distribution → timeout tuning, budget exhaustion |
| `partial_capture_drift` | Captured amount ≠ requested → reconciliation break |
| `stale_status` | `GetStatus` lags reality → recovery logic must tolerate it |

**Status reconciliation path:** on timeout or 5xx, the orchestrator must **never** blindly retry. It calls `GetStatus` with the same PSP idempotency key, and only retries when the PSP confirms no charge exists.

## Related code files

**Create**
- `internal/psp/` — adapter interface, registry, per-PSP config
- `internal/psp/stripe/`, `internal/psp/simulator/`
- `internal/psp/simulator/faults/` — fault engine, seeded RNG
- `cmd/pspsim/` — standalone simulator service (separate process; must be killable for the demo)
- `internal/transport/http/admin/` — fault control API

## Implementation steps

1. Define `PSPAdapter` with a normalized request/response and a **normalized error taxonomy** — `Declined`, `InsufficientFunds`, `DoNotHonor`, `SuspectedFraud`, `NetworkError`, `Timeout`, `RateLimited`, `InvalidInstrument`, `Unknown`. Map every PSP's native codes into it.
2. Build `pspsim` as its own process with its own store, so it can be killed mid-flow.
3. Fault engine: per-endpoint probability config, seeded RNG for reproducibility, live-reloadable.
4. Implement the four providers, honouring their distinct semantics.
5. Simulator webhook emitter: configurable delay, duplication, reordering.
6. Per-PSP idempotency keys — your key ≠ PSP's key; store both and document why.
7. Admin control API: `GET/PUT /admin/faults`, plus a preset switcher (`healthy`, `degraded`, `chaos`).
8. Integration tests: one per fault, asserting no double-charge and no lost payment.

## Todo

- [x] `PSPAdapter` interface + normalized error taxonomy
- [x] `pspsim` standalone service
- [x] Seeded, live-reloadable fault engine
- [x] Three simulated providers with distinct semantics (Stripe deferred — see below)
- [x] Webhook emitter with delay/duplicate/reorder
- [x] Per-PSP idempotency key handling
- [x] Admin fault control API + presets
- [x] Integration test per fault mode

## Verified on 2026-08-11

ADR [0006](../../docs/adr/0006-ambiguous-provider-outcomes-are-resolved-not-guessed.md).

**Recovery, the flagship claim** — `timeout_after_success` and
`error_5xx_after_success` each forced to 100%: transaction reaches `authorized`
with **exactly one charge** at the provider. Tests assert the audit trail
contains *recovered after ambiguous failure*, so the assertion cannot pass
vacuously if the fault silently stops firing.

**Nothing terminal without evidence** — provider outage, and provider process
killed outright mid-flow: transaction stays `authorizing`, never `failed`. Only
a genuine decline reaches a terminal state.

**Error taxonomy** — partition test proves every class is exactly one of
terminal / retryable / ambiguous. 5xx and unrecognised errors both classify as
ambiguous, not failure.

**Determinism** — same seed replays identically regardless of call order or
concurrency; different seeds diverge. Fault verdicts are derived per
attempt rather than per key, so a retry can escape a fault that fired once.

**Provider shapes** — sync returns authorized; async returns pending and parks
the transaction until a webhook resolves it; redirect returns requires_action
with a URL. Duplicate and out-of-order webhook faults verified at the emitter.

**Demo, end to end** — `make pspsim` + `make run`, then `make chaos`,
`make outage`, or killing the simulator; audit trail shows
`authorizing -> authorized (recovered after ambiguous failure)`.

## Deferred

- **Stripe sandbox adapter.** Needs a real Stripe account and API key. The
  `psp.Adapter` interface and registry are built so it slots in without touching
  orchestration; simulators remain the primary test surface, per this phase's own
  risk note.
- **End-to-end webhook faults.** `duplicate_webhook`, `out_of_order_webhook`, and
  `webhook_before_response` are implemented and tested at the emitter, but there
  is no ingestion endpoint until Phase 05, where they get proven end to end.
- **Capture and refund have no HTTP surface** yet, though both paths exist on the
  adapter and aggregate.
- **Authorization runs inline** in the request. Phase 04 moves it behind the
  transactional outbox.

## Success criteria

- Every fault in the catalogue is reproducible from a seed and toggleable at runtime.
- `timeout_after_success` at 100% rate → zero double-charges, transaction lands in the correct terminal state via `GetStatus` recovery.
- Killing `pspsim` mid-flow leaves no transaction stuck in a non-recoverable state.

## Risks

| Risk | Mitigation |
|------|-----------|
| Simulator grows into its own project | Cap it: the fault catalogue above is the whole scope |
| Stripe sandbox behaviour changes | Contract-test the adapter; simulators are the primary test surface |
| Non-deterministic tests | Seed everything; no wall-clock dependence in assertions |

## Security considerations

- Admin fault API must be disabled by default and gated behind a config flag — a fault-injection endpoint in a payment system is a live weapon.
- Simulator never accepts or logs real card data.

## Next steps

Phase 04 — outbox, Kafka, retry, DLQ. The fault engine is what proves the retry strategy actually works.
