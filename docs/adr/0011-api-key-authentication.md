# ADR 0011 — API key authentication

**Status:** Accepted · **Date:** 2026-08-17

## Context

The merchant was an unauthenticated header. `X-Merchant-Id: whoever` was taken
at face value, so any caller could act as any merchant.

That made every other guarantee in the system conditional. The ledger is
per-merchant, idempotency claims are scoped per merchant, payments are routed to
a merchant's shard — and all of it keyed off a string the caller chose. It is
also why cross-shard transfers were given a CLI and no HTTP endpoint: an
unauthenticated endpoint that moves money between merchants is worse than no
endpoint.

## Decision

### Bearer API keys, not signed requests

`Authorization: Bearer pmt_<public>_<secret>`.

Request signing — HMAC over method, path, timestamp, and body — was considered
and rejected for the merchant-facing surface. It defends against a different
threat: an attacker who can read traffic but not replay it. Under TLS that
attacker does not exist, and the scheme's real cost is borne by every integrator
who now has to reproduce a canonicalisation exactly. The inbound webhook path
*does* use HMAC, because there the provider is authenticating to us over a
public endpoint and we cannot issue them a credential.

### The secret is stored as a SHA-256 digest, and never recoverable

There is no command that reads a key back. It is displayed once at issue and
then exists only in the caller's hands.

**SHA-256 rather than bcrypt or argon2.** Password hashes are deliberately slow
because they protect secrets humans chose, which are short and drawn from a
guessable distribution. This secret is 32 bytes from `crypto/rand`: there is no
dictionary and no rainbow table, so slowness would buy nothing and would put
milliseconds on every authenticated request — the hottest path in the system.

The property that matters is that a stolen database yields digests, and a digest
cannot be presented to the API. A test asserts the secret appears in no column
of the stored row.

### A plaintext public half, for lookup

A hash is not searchable, so verifying by hash alone would mean scanning the
table and hashing as it went. The key therefore carries a public handle that is
stored in clear and uniquely indexed: one row lookup, then one constant-time
comparison of the digest.

The comparison uses `subtle.ConstantTimeCompare`. A byte-by-byte compare that
returns early leaks how much of a guess was right, and over enough attempts that
recovers the key.

### Base32, because the alphabet must not contain the delimiter

The first implementation used base64url and was wrong: its alphabet includes
`_`, which is the field separator, so roughly a third of generated keys came out
with an extra separator inside the secret and failed to parse. Caught by a test
that generated keys and parsed them back.

Lowercase base32 excludes both `_` and `-`, so the three fields can never be
ambiguous. The string is longer; the format is unambiguous.

### Keys live on shard 0

Authentication happens *before* the merchant is known, so there is no shard key
to route on. This is the same reason the webhook log and the consumer dedup
index live there, and it is now a recognisable pattern in this system: data
whose identity is not yet resolved cannot be partitioned by that identity.

### Revoked, never deleted

A key that authenticated a payment last week is part of that payment's history.
Deleting the row destroys the only record of which credential acted. Revoking
twice reports no change rather than a second success, so an operator is never
told they disabled something they did not.

### `last_used_at` is updated lazily

Answering "is anything still using this key" is worth a column; writing it on
every request is not. It is refreshed at most hourly, which is the resolution
the question actually needs, and the update is best-effort — refusing an
authenticated request because a timestamp could not be written would turn
bookkeeping into an outage.

### The merchant reaches handlers out of band

The middleware puts the authenticated merchant in the request context. Handlers
read it from there and cannot read it from a header, because a handler that
reads `X-Merchant-Id` has no way to tell whether the middleware or the caller
put it there.

### Registering the payment routes without auth is a panic

The route group is built only when the router and key store are both present,
and refuses to be built when they are not. A payment API that boots and serves
without authentication is the failure this guards; refusing to start is loud,
immediate, and happens on somebody's laptop.

## Consequences

- The demo mints a key at startup and asserts that an unauthenticated create is
  refused. The load profiles take `API_KEY`.
- `X-Merchant-Id` is gone entirely — presenting it now returns 401, verified
  against a running service.
- Tenant isolation is real: a payment created with one merchant's key answers
  404 to another's, which was previously enforceable only by convention.
- An HTTP surface for cross-shard transfers is now possible. It is still not
  built.

## What this does not do

- **No scopes or permissions.** A key can do everything its merchant can. Read-
  only keys and per-endpoint scopes are the obvious next step and are not here.
- **No rotation workflow.** Issuing a second key and revoking the first works,
  but nothing helps an integrator overlap them safely.
- **No rate limiting per key**, which is the natural place for it now that
  callers are identified.
- **No admin authentication.** `apikeyctl` authenticates to the database, not to
  the service, so anyone who can reach Postgres can mint a key. That is the same
  trust boundary the migration tooling already assumes.
