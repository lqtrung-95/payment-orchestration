# ADR 0006 — Ambiguous provider outcomes are resolved, never guessed

**Status:** Accepted · **Date:** 2026-08-11

## Context

A provider call can fail in a way that says nothing about whether the money
moved. The request reached the provider, the charge was recorded, and the
response was lost — a timeout, a dropped connection, a 500 emitted after the
work was done.

There are two obvious responses and both are wrong. Retrying charges the
customer twice. Marking the payment failed writes off money the customer has
actually paid. The system needs a third answer.

## Decision

### Every provider error is normalized into one of three categories

The adapter maps provider-specific codes onto a taxonomy, and each category
gets a different response. A test asserts the categories partition the set —
every class is exactly one of the three, so no error can fall through every
branch or take whichever branch is checked first.

| Category | Classes | Response |
|---|---|---|
| **Terminal** | declined, insufficient_funds, do_not_honor, suspected_fraud, invalid_instrument | Fail the transaction. Never retry. |
| **Retryable** | rate_limited, unavailable | Leave open. Nothing happened. |
| **Ambiguous** | timeout, network_error, unknown | Resolve via `GetStatus`. Never retry blind. |

A **500 is ambiguous, not a failure.** This is the single most consequential
line in the mapping: the provider may have recorded the charge and then fallen
over while replying.

An **unrecognised error defaults to ambiguous**, not terminal. Guessing "it
failed" about an outcome nobody established is how a real payment gets written
off while the customer's money is gone.

### Ambiguity is resolved by asking, with an allowance for replica lag

On an ambiguous failure the orchestrator calls `GetStatus` using the same
provider-scoped idempotency key. If the provider reports a charge, that is the
outcome. If it reports none, the query is repeated up to three times at 150ms
intervals, because a provider's status endpoint is frequently served from a
replica that lags its write path — believing an immediate "not found" would
license a retry against a charge that already exists.

### When nothing can be established, the transaction stays open

If the provider consistently reports no charge, or cannot be reached at all, the
transaction is left in a **non-terminal** state and `ErrOutcomeUnresolved` is
returned. It is not failed. Something later — a webhook, a retry, reconciliation
— has to close it, but nothing may close it by assumption.

This is the deliberate asymmetry: a false "authorized" is recoverable by
reconciliation, a false "failed" is money quietly lost.

### Provider idempotency keys are per operation, derived not supplied

`OperationKey(transactionID, operation)` — deliberately not the merchant's key.
One merchant request produces several provider operations, and reusing one key
across them would make the provider treat a capture as a replay of the
authorization. Being a pure function of transaction and operation, a retry of
the same logical operation always presents the same key, which is what makes the
provider's own idempotency protect us.

## Alternatives rejected

- **Retry on timeout.** The textbook double charge.
- **Fail on timeout.** Loses real payments, and the loss is silent.
- **Treat 5xx as a clean failure.** Same as above, wearing an HTTP status code.
- **Believe the first "not found" from GetStatus.** Ignores replica lag, and
  converts a lagging read into a duplicate charge.
- **Block until the outcome is certain.** Unbounded: a provider that is down
  stays down, and the request holds a connection for the duration.
- **A single global provider timeout.** Providers have genuinely different
  latency profiles; one budget either cuts off the slow one or lets the fast one
  hang. Timeouts are per provider.

## Consequences

- Some transactions sit in a non-terminal state indefinitely until a later phase
  builds the machinery to close them. That is correct but visible, and needs an
  alert on age rather than a cleanup that guesses.
- The recovery path costs up to three extra calls on an ambiguous failure.
- `GetStatus` must be safe to call at any time and is deliberately excluded from
  the post-success fault injection: corrupting the recovery path itself would
  leave no way out of an ambiguous state.
- Verified: with timeout-after-success and 5xx-after-success each forced to
  100%, the transaction reaches `authorized` with exactly one charge at the
  provider, and the audit trail records *recovered after ambiguous failure* —
  asserted in tests, so the guarantee cannot pass vacuously.
