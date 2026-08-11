# ADR 0007 — Transactional outbox and error-aware retries

**Status:** Accepted · **Date:** 2026-08-11

## Context

Authorization was running inside the HTTP request, so every caller waited on a
third party. Moving it to a queue raises the question every queued system has to
answer: how does work get onto the queue without being lost or invented, and
what happens when it fails.

## Decision

### The message is written in the same transaction as the domain change

`POST /v1/payments` writes the transaction, its first audit row, and an `outbox`
row in one database transaction. A relay carries the row to Kafka afterwards.

The two obvious alternatives both lose. Publishing to the broker *inside* the
transaction emits work for a payment that then rolls back — a phantom charge.
Publishing *after* the commit loses the work whenever the process dies in the
gap, and that gap is exactly where crashes are most likely, because it is the
moment of highest activity.

CDC (Debezium reading the WAL) is the other real option and is documented here
rather than built: it removes the polling latency, at the cost of an additional
piece of infrastructure to operate and a schema coupling to the replication
stream. At this size, polling at 100ms is the cheaper trade.

### Rows are claimed by lease, not by lock

The claim is an `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` that
pushes `available_at` forward.

A locking `SELECT` alone is wrong here, and subtly so: row locks last only as
long as the transaction holding them, but publishing happens *after* that
transaction commits. The locks are gone by then, so every other publisher
immediately claims the same batch and sends it again. This was caught by a test
asserting no duplicate publishes — 259 sends for 120 messages.

The lease keeps rows invisible to other publishers without holding a database
transaction open across network I/O. A publisher that dies mid-batch stops
renewing, and its rows become claimable when the lease expires, which is also
how a crashed instance's work is recovered.

### Delivery is at-least-once; the *effect* is exactly-once

A row is marked published only after the broker acknowledges it, so a crash in
between republishes. That is the correct trade: a duplicate is absorbed
downstream, whereas a lost payment event has nothing to recover it.

Exactly-once *delivery* does not exist — a broker cannot distinguish a consumer
that processed a record from one that died just before. Exactly-once *effect*
does, and here it comes from two things composed:

1. `processed_events`, keyed by `(event_id, consumer_group)`, which stops an
   event being handled twice in the ordinary case.
2. The provider-scoped idempotency key from ADR 0006, which makes the provider
   itself collapse a repeated operation into the original charge.

The second is what actually protects the money. There is a window between a
handler succeeding and the dedup row being written, and no single transaction
can close it, because the provider call is not part of any transaction we
control. Claiming the table alone is the guarantee would be a false comfort.

### Partitioned by merchant

The Kafka key is the merchant id — the same value as the shard key from ADR
0003. Ordering is guaranteed per partition, so this gives per-merchant ordering,
which is the only ordering this system needs. Global ordering across all
merchants is neither achievable at scale nor useful.

### Retries are driven by the error taxonomy, not by a uniform count

| Class | Policy |
|---|---|
| `Timeout`, `NetworkError` | Retry, but confirm the outcome via `GetStatus` first |
| `Unknown` | One confirmation attempt only, and alert |
| `RateLimited`, `Unavailable` | Retry on the slower tiers — retrying at the fast pace keeps a rate limit engaged |
| `Declined`, `InsufficientFunds`, `DoNotHonor` | **Never retry** |
| `SuspectedFraud` | Never retry, and alert |
| `InvalidInstrument` | Never retry until the instrument is re-verified |

"Retry everything a few times" is the common answer and it is wrong in both
directions: it repeats declines, which is user-hostile and escalates issuer
fraud controls, and it retries ambiguous failures blindly, which charges twice.

### Delay is modelled as topics, not sleeps

Four tiers — 5s, 30s, 5m, 30m — each a topic whose consumer waits until the
message is due. Sleeping inside a handler loses every scheduled retry when a
consumer restarts. Holding the partition while waiting is intended: a delay
topic is ordered by time, so the message at the head is always the one that
becomes due first.

Backoff uses **full jitter** — a delay drawn uniformly from `[0, ceiling]`
rather than set to it. When a provider recovers from an outage, callers that
backed off deterministically all wake at the same instant and knock it over
again; spreading the wake-ups is what lets it actually recover.

### Nothing is dropped

Exhausting the ladder parks the message in a DLQ with its reason and origin
topic. `dlqctl` lists and replays, and replay requires `--actor` and `--reason`,
because replaying a failed payment is a money-moving decision and the first
question afterwards is always who decided to.

### The circuit breaker counts provider failures, not declines

Per provider, so one failing provider cannot stop traffic to the others. A
decline is the provider working correctly; counting declines would trip the
breaker on a merchant with poor approval rates and take a healthy provider out
of service.

## Consequences

- `POST /v1/payments` returns in the `created` state. The outcome is discovered
  by polling the resource, or by a webhook in a later phase. The response no
  longer carries the final state, which is a real API change.
- Two more processes to run: the worker, and the relay (which runs inside the
  API process, safely, because of the lease).
- End-to-end latency now includes the poll interval — 100ms at the floor.
- A message stuck in a retry tier is invisible without looking at consumer lag,
  which the observability phase has to surface.
- The DLQ needs someone to look at it. A dead letter queue nobody watches is
  just a slower way of dropping messages.
