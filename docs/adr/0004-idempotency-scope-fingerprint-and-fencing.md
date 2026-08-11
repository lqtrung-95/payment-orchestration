# ADR 0004 — Idempotency: scope, fingerprint, and fencing

**Status:** Accepted · **Date:** 2026-08-11

## Context

Clients retry. Networks drop responses after the work has already been done.
Without a mechanism that makes retries safe, every timeout is a potential double
charge — the single most damaging failure mode this system has.

## Decision

`POST /v1/payments` requires an `Idempotency-Key` header. A row in
`idempotency_keys` arbitrates ownership, with four decisions behind it.

### Scope is per merchant

Unique on `(merchant_id, key)`. A global namespace lets one merchant collide
with another's key by accident, and lets them probe for it deliberately: a bare
409 would reveal that some other tenant had used that value.

### The claim is committed before the handler runs

Ownership is decided by the unique constraint inside a transaction that commits
immediately. Until the in-flight row is visible to other connections, a
concurrent request carrying the same key finds nothing and concludes it may
proceed. A read-then-write, or holding the claim open inside the handler's own
transaction, both defeat this.

Four outcomes: **Acquired** (do the work), **Replay** (return the stored
response), **InFlight** (409 with `Retry-After`), **FingerprintMismatch** (409).

### Requests are fingerprinted over canonicalised JSON

SHA-256 over method, path, and body. The body is decoded and re-encoded first,
which normalises whitespace and orders keys — otherwise a client that merely
re-serialised its request on retry would be told the body changed and refused.
A body that is not valid JSON is hashed verbatim: at worst that causes a
spurious mismatch, never a false match.

### A stale claim can be taken over, under a fencing token

A claim whose owner died would otherwise block that key forever, so a claim
older than `LockTTL` (twice the HTTP request timeout) may be taken over. Every
claim issues a fresh `claim_token`, and `Complete` requires the current one.
Without it, an owner that stalled past the TTL and then woke up would overwrite
the result of the request that legitimately replaced it.

Record TTL is 24 hours, matching what providers conventionally honour.

## Alternatives rejected

- **Redis as the arbiter.** A cache cannot arbitrate a race that must be decided
  atomically with the work. Redis is added later as a fast path, never as the
  authority.
- **Blocking until the in-flight request finishes.** Holds a connection for the
  length of a provider call, and a client retrying on timeout queues behind
  itself.
- **Replaying only successes, re-executing failures.** A caller that received a
  decline and retries the same key would get a second attempt against the
  instrument, which is user-hostile and trips fraud controls.
- **Storing the response as JSONB.** JSONB normalises on write — reordering keys
  and rewriting whitespace — so replays would not be byte-identical. That breaks
  any client verifying a signature over the response, and does so invisibly.
  Stored as `BYTEA` instead; querying into stored responses is not a requirement.
- **Lock TTL without a fencing token.** Leaves a window where two processes both
  believe they hold the claim.

## Consequences

- Verified: 50 concurrent requests with one key produce exactly one transaction
  and one key row; retries are byte-identical.
- Takeover safety ultimately rests on the provider call itself being idempotent,
  via a per-provider key. Until that exists, `LockTTL` is the only guard.
- Expired records need a reaper, not yet built. Until then the table grows.
- If recording the response fails, the response is still returned correctly but
  cannot be replayed; the retry re-executes instead.
