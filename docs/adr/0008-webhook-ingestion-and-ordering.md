# ADR 0008 — Webhook ingestion, deduplication, and ordering

**Status:** Accepted · **Date:** 2026-08-12

## Context

Providers report outcomes by calling back. Those callbacks are untrusted,
duplicated, unordered, and sometimes arrive before the API response that created
the state they describe — all four routinely, not as edge cases. For the async
provider shape a callback is the *only* way an outcome is ever learned, so the
receiver is not an accessory to the payment path; it is part of it.

## Decision

### Ingestion verifies, stores, and acknowledges — nothing else

`POST /webhooks/{provider}` checks the signature, writes the raw delivery, queues
a message, and returns 200. Everything that interprets the event happens
afterwards, asynchronously.

The reason is a feedback loop rather than tidiness. A provider that waits on our
processing times out and redelivers, which multiplies load at exactly the moment
the receiver is least able to absorb it. Slow ingestion does not degrade
gracefully; it amplifies.

Measured at 6ms worst case in the chaos run.

### The unique index is the deduplication point, not a cache

`webhook_events_raw` is unique on `(provider, provider_event_id)`, and a conflict
returns 200 rather than an error. A duplicate is the provider doing its job;
telling it otherwise makes it retry harder.

Redis in front of this would be a latency optimisation and may be wrong in both
directions after a failover. A unique constraint cannot be.

### The raw payload is stored as bytes, not JSONB

The signature was computed over the exact bytes received. JSONB reorders keys and
rewrites whitespace, so a normalised payload can never again be verified against
the signature that arrived with it — which destroys the log's value as evidence,
its entire reason for existing. Same reasoning as the idempotency response body
in ADR 0004.

Re-parsing the stored bytes on every read also means a mapping mistake is
recoverable: the interpretation can be corrected and the log re-read through it.

### Staleness is judged by the provider's sequence, never by arrival order

Each transaction carries `last_applied_event_seq`, a high-water mark. An event at
or below it is `superseded` — recorded, never applied, never dropped.

A sequence rather than a timestamp because timestamps come from whichever
provider host emitted the event, and two hosts a few milliseconds apart routinely
disagree about which of two events came first.

**Whatever the adapter reports is authoritative, including zero.** A shared
"treat a low value as missing and derive one from the timestamp" rule rewrites
the oldest event in a batch into the newest, silently defeating the guard for
exactly the delivery it exists to catch. This was a real bug, caught by the
out-of-order fault emitting sequence 0. Whether a provider has an ordering token
is the adapter's knowledge; nothing downstream may infer it.

### Application is guarded by the state machine, not by ad-hoc checks

A late `authorized` event arriving after a capture is refused because that edge
is absent from the transition matrix — a structural answer rather than a special
case somebody remembered to write.

An event confirming the state the transaction is already in is `ignored`, not
applied. The matrix permits a same-state move, so it would otherwise be written
into the audit trail as a transition, and a trail showing a payment authorized
twice sends an incident responder hunting for a second authorization that never
happened. This happens constantly, because the ambiguous-outcome recovery path
from ADR 0006 often establishes the outcome before the callback lands.

### A webhook never creates a transaction

If no transaction matches the reference, the event is deferred and retried. After
the ladder is exhausted it is marked `unmatched` and parked in the DLQ for a
person.

Creating a transaction from a callback would be how phantom payments enter a
ledger. A payment this service has no record of asking for is not a payment.

The retry ladder *is* the parking area. A dedicated `pending_correlation` table
with its own TTL sweeper would be a second mechanism doing what the first already
does — the same waiting, the same escalation, the same dead letter queue at the
end — and two mechanisms for one job drift apart.

### Deferred messages pause partitions; they never sleep

Delay tiers were implemented by sleeping in the handler until a message was due.
That is wrong, and it was found stalling a live run: one message on the
5-minute tier held **20 payments** at `created`.

Records from every assigned partition are handed to a single goroutine, so a
handler that waits holds all of them, not just its own. A wait longer than the
rebalance timeout also gets the consumer evicted from its group, which the 5m and
30m tiers guarantee.

The consumer now pauses the partition and rewinds to the message's offset,
resuming when it is due. Polling continues, heartbeats continue, rebalances work,
and the deferred message waits alone. Head-of-line blocking *within* that
partition is retained deliberately: a delay topic is ordered by time, so its head
message is always the one that becomes due first.

### Replay is convergent, and that is a narrower claim than it sounds

`webhookctl replay` re-evaluates every stored event against current state and
reports what would change. A clean log changes nothing.

The original goal was "replaying the log into an empty database reproduces
identical state". That is not achievable and claiming it would be dishonest:
transactions are created by API calls and moved by provider responses, neither of
which is in this log, so an empty database has nothing for these events to
correlate against. Full reconstruction would require event-sourcing the whole
aggregate.

What convergence *does* buy is the property that matters operationally — replay
is safe. A log that quietly re-applies itself is worse than no log, because it
invites precisely the recovery procedure that corrupts state.

## Consequences

- Webhook processing shares the retry ladder and DLQ with authorization, so
  messages carry an origin-topic header and a router dispatches on it.
- `webhook_events_raw` grows without bound; it needs a retention policy, and the
  payloads may contain PII.
- The endpoint is unauthenticated by design (signature *is* the authentication),
  so body size is capped and the production boot refuses the development secret.
- Correlation is by provider reference alone, which required a dedicated index.
- Capture and refund outcomes are parsed but not applied: neither operation has
  an HTTP surface yet, and applying a capture from a callback would post to the
  ledger from a path with no amount reconciliation behind it.
