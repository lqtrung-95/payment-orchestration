# Interview preparation notes

Working notes, not documentation. Everything here is grounded in what actually
happened in this repo — the bugs are real, the numbers are measured, and the
regrets are genuine.

> **This file is committed.** `plans/` is tracked by git, so this is public if
> the repo is. Decide deliberately: `.gitignore` it, or leave it.

## The numbers, so they are never guessed at

| | |
|---|---|
| Tests | 224 across 22 packages, `-race -p 1`, against real Postgres and Kafka |
| Non-test Go + SQL | ~16,500 lines |
| Test code | ~7,500 lines |
| ADRs | 10, each naming a rejected alternative |
| Migrations | 12, verified up **and** down |
| Peak measured | 453 req/s accepted with ~30% provider fault injection, 0 server errors in 55,767 |
| Invariants during that run | double charges 0, lost payments 0, ledger imbalance 0 |
| Hardware | Apple Silicon laptop, Docker limited to 4 CPUs / 8 GB shared by Postgres + Kafka + Redis |

Always state the hardware. A throughput number without it is a decoration, and
saying so unprompted is itself a signal.

---

## The six questions

### 1. Why TCC and not Saga?

Because compensation for money that has *moved* is not an inverse.

A saga's compensation for a completed transfer is a reverse transfer. Between
the two, the funds were spendable — so a balance check in that window sees money
that is about to be taken back, and the account can be overdrawn by an operation
that was individually valid. Add fees or FX and the reverse leg does not even
have the same magnitude.

TCC reserves instead. The funds become unavailable without moving, so cancelling
is exact: nothing was posted, and there is nothing to undo. The merchant's books
show no trace of a transfer that did not happen.

The cost is real and worth naming: an explicit Try on every participant, and a
sweeper for holds nobody resolved. A reservation is unspendable funds, so the
sweeper failing is a money-safety failure, not a background-job failure.

**Why not 2PC:** it blocks. A coordinator dying during the prepared phase leaves
participants holding locks until a human intervenes. On a payment ledger that is
an outage, not a delay.

**The part I would lead with:** the commit point. `trying → confirming` is
committed before either side posts. Before it, cancel freely. After it, every
participant has agreed and the transfer is *owed* completion — a failing confirm
is retried, never converted into a cancel. That single rule is what makes crash
recovery decidable: the sweeper reads one column and knows what to do without
asking anyone.

### 2. Why shard on merchant?

Because merchant-scoped reads dominate the workload. Balances, settlement, and
reconciliation are all per-merchant, so partitioning this way keeps the common
query inside one shard.

Rejected: `user_id` splits a single merchant's ledger across every shard, which
makes every merchant-level aggregate a scatter-gather. `transaction_id`
distributes evenly and has the same problem, worse.

The follow-up worth pre-empting is *hot merchants* — one merchant large enough
to dominate a shard. I have not solved it. The honest answer is that the logical
layer gives room to move that merchant's shard to its own database, and beyond
that it needs a sub-key, which I did not build.

**64 logical shards, power-of-two physical count.** This is the part that shows
judgment. Doubling the physical count splits every range exactly in half, so
growing capacity is "copy logical shards 32–63 to the new database" — a
contiguous bulk move, verifiable by counting rows per shard key. Hashing
merchants straight onto physical databases makes adding one a rehash of the
entire data set, taken while money is moving.

**Routing reads the stored key, never re-derives it.** Re-deriving means a
change to the hash function silently sends reads to a database that does not
hold the rows.

### 3. How do you guarantee exactly-once?

I do not. Nobody does. The claim is **at-least-once delivery with idempotent
effects**, and being precise about that distinction is the answer.

Three layers:

- **Into the queue:** a transactional outbox. The payment, its audit row, and
  the message commit together. Publishing inside the transaction emits work for
  a payment that may roll back; publishing after it loses the work if the
  process dies in between.
- **Out of the queue:** the relay marks a row published only after the broker
  acknowledges it, so a crash in between republishes. A duplicate is absorbed;
  a lost payment event has nothing to recover it.
- **At the consumer:** a unique index on event id in `processed_events`. The
  database arbitrates, not application code.

And at the provider: an idempotency key derived from the transaction and the
operation, so a retried authorize is the same request rather than a second one.

### 4. What happens when the PSP times out after charging?

This is the case the whole project is organised around.

The wrong answers are symmetric. Retry, and you double-charge — the request
succeeded and only the reply was lost. Mark it failed, and you have written off
a payment the customer's money already left for.

So the outcome is **established, never assumed**:

1. Classify the error. Terminal (declined) is never retried. Retryable (429,
   503) means nothing happened, so the transaction stays where it is. Ambiguous
   (timeout, 5xx) means the money may have moved.
2. For ambiguous, ask the provider — `GetStatus` with the same idempotency key,
   up to three times with a short delay. Once is not enough because a provider's
   status endpoint is often served from a replica that lags its write path, and
   believing a stale "not found" licenses a retry against a real charge.
3. If it still cannot be established, the transaction stays **non-terminal
   forever**. Nothing downstream concludes anything. It is resolved later by a
   webhook, a retry, or reconciliation.

The transaction moves to `authorizing` *before* the provider call, so a crash
mid-flight leaves a state that says an attempt was in progress rather than one
claiming nothing happened.

The simulator has a `timeout_after_success` fault for exactly this, and the test
asserts **the provider's charge count** — because a test asserting only "an
error was returned" passes whether or not the customer was charged.

### 5. How would you scale reconciliation to 100M rows/day?

Straight admission first: **the current implementation cannot.** It loads a
settlement file and its ledger window into memory. That suits demo scale and is
documented as unbenchmarked.

What I would change, in order:

1. **Stream, don't load.** Parse the file as a cursor and pull the ledger window
   with a keyset-paginated query, so memory is bounded by the batch not the day.
2. **Match in the database.** Bulk-load the file into a staging table on the
   provider's shard, then do the matching as a `FULL OUTER JOIN` on the provider
   reference. Postgres joins 100M rows better than Go loops do.
3. **Partition by settlement date**, so a day's work touches one partition and
   old ones can be detached rather than deleted.
4. **Parallelise by provider, then by shard.** Reconciliation is per-provider and
   embarrassingly parallel across them.
5. **Only then** consider moving it out of Postgres.

The property I would protect throughout: matching stays **exact on the provider
reference**. A fuzzy fallback on amount and date produces a confident wrong
answer, and nobody re-examines a confident one.

### 6. What is your biggest design regret here?

**The worker consumes with a single goroutine per partition, and I let that
become a constraint instead of choosing it.**

It was never a decision. It was a consequence of the simplest consumer loop,
and by the time the retry ladder existed — delay tiers whose correctness depends
on messages being processed in order — the coupling was load-bearing. The load
test found the result: **ingestion scales and processing does not.** 55,590
payments sat at `created` behind 42,409 pending outbox rows, because throughput
collapses to roughly the reciprocal of provider latency however many partitions
exist.

The fix is concurrent per-partition consumers, and I deliberately did not build
it, because doing it carelessly is how a delay tier stops being ordered by time.
That is the right call *now* and the wrong position to have ended up in.

What I would do differently: decide the ordering guarantee explicitly, at the
point the retry ladder was designed, and write it down as an ADR. The guarantee
I actually need is per-*payment* ordering, not per-partition — which permits
concurrency across payments within a partition and would have left the door open.

**Second-order regret, if pressed:** the consumer dedup index and the webhook
log live on shard 0 because neither has a merchant to partition on. That is
defensible and I would make the same call, but it means shard 0 is
structurally hotter than its peers, and the design has no answer for that beyond
"give shard 0 a bigger machine".

---

## Bugs worth telling, and what each one teaches

The point of these stories is not the bug. It is what the bug says about how the
system was being verified.

| Bug | What it teaches |
|---|---|
| **A deferred retry stalled the whole consumer.** Delay tiers slept in the handler; every partition's records are processed by one goroutine, so one message on the 5-minute tier held 20 unrelated payments at `created`. | Found by reading consumer-group lag during a demo, not by a test. It is why queue depth and lag are now gauges. Fixed by pausing and rewinding the partition. The regression test was checked against the old code to confirm it actually fails. |
| **Capture charged before validating.** An over-capture of 80.00 against a 50.00 authorisation reached the provider, which charged it, and only then was the record refused. | Order of operations is a money question, not a style question. The test asserts the provider's charge count, because asserting the error and an empty ledger passes in *both* versions. |
| **A sequence of `0` was treated as "absent"** and substituted with a timestamp — turning the oldest event in a batch into the newest and silently disabling the staleness guard for exactly the delivery it exists to catch. | A sentinel that collides with a legal value. |
| **Confirmations were recorded as transitions.** Same-state moves are legal, so a payment resolved by the recovery path and then confirmed by webhook showed 15 `authorized` rows for 12 payments. | "Idempotent" and "records nothing new" are different properties. |
| **`gross_minor` held either EUR or USD** depending on `settlement_currency`. The arithmetic worked because both sides shared the assumption. | A column whose meaning depends on another column. Fixed with a dedicated column and a CHECK requiring the FX fields together. |
| **The outbox relay silently stopped publishing** after `traceparent` was added to `RETURNING` without a matching `Scan` destination. | Caught in seconds only because the relay logs *every* sweep failure, not just when it parks a message. |
| **Removing the advisory lock overdraws an account to −200.** | Not a bug that shipped — a deliberate check that the lock is load-bearing. Same technique used on the confirm path and the commit-point rule. |

**The reusable point:** several of these were invisible to tests that existed at
the time. The habit that catches them is asserting against the *external* party
— the provider's charge count, the row count on the other database, the suspense
total across shards — rather than against the thing under test agreeing with
itself.

---

## Things to say unprompted

- **"The invariant checker is itself tested against planted violations."** One
  that always returned zero would pass every load test ever run against it.
- **"Every invariant is a database constraint, not a Go check."** Application
  code can be bypassed by a migration, an admin session, or a repair script.
  History that can be edited is not an audit trail.
- **"Migrations are verified in both directions."** A down migration nobody has
  run is not a rollback plan.
- **"The webhook payload is stored as bytes, not JSONB."** The signature was
  computed over those exact bytes; normalising them means the stored event can
  never be verified again.
- **"There is no API that opens a transaction across shards."** Postgres cannot
  commit across databases, so such a method could only be two transactions
  wearing one name.

## Things to admit before being asked

Volunteering these is worth more than being caught by them.

- **No authentication.** `X-Merchant-Id` is an unauthenticated header. It is
  also why cross-shard transfers have a CLI and not an HTTP endpoint.
- **The 2,000 req/s target was not met** and is not close on this hardware.
- **No outage, spike, or soak run.** Those profiles exist and have never been
  executed, so nothing is claimed about failover timing or memory over hours.
- **Reconciliation is unbenchmarked**, and question 5 above is the answer.
- **Phase 06 (instrument vault) was skipped**, deliberately — it was not a
  dependency for FX and reconciliation, and tokenisation without a real PSP is
  theatre.
- **No resharding tooling.** The mapping makes the move contiguous; nothing
  automates it.

## The framing to keep returning to

*Everything interesting in a payment system comes from things going wrong.* The
happy path is a CRUD app. What this project is actually about is the four or
five failure modes that cost real money, and the fact that each of them is
answered by a mechanism that can be pointed at in the code and demonstrated
failing when it is removed.
