# Phase 03 — PSP Abstraction + Fault-Injecting Simulator

**Priority:** P0 · **Status:** Not started · **Week:** 5

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

- [ ] `PSPAdapter` interface + normalized error taxonomy
- [ ] `pspsim` standalone service
- [ ] Seeded, live-reloadable fault engine
- [ ] Four providers with distinct semantics
- [ ] Webhook emitter with delay/duplicate/reorder
- [ ] Per-PSP idempotency key handling
- [ ] Admin fault control API + presets
- [ ] Integration test per fault mode

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
