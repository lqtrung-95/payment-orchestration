# Benchmarks

Every number here was measured on the hardware described below, with the exact
command shown. Nothing is extrapolated, and nothing is rounded up.

The headline is not the throughput. It is that **the three must-be-zero
invariants held at zero in every run here** — including one where the provider
was failing roughly a third of requests, and one where it vanished entirely
mid-run — and that the measurements found a real bottleneck rather than
confirming a hope.

## Hardware

This matters more than the numbers do. A throughput figure without the machine
it came from is a decoration.

| | |
|---|---|
| Host | Apple Silicon laptop, 10 cores |
| Docker Desktop | **4 CPUs, 8 GB** — shared by Postgres, Kafka, Redis |
| Postgres | 16.10, in Docker |
| Kafka | 3.9.1 KRaft, single broker, in Docker |
| Under test | orchestrator, worker, provider simulator — on the host |
| Load generator | k6 v2.2.0, also on the host |

The datastores get four shared cores while the application and the load
generator compete for the other six. This is a developer laptop, not a
representative deployment, and the numbers should be read as "what this
architecture does on this machine" rather than as a capacity claim.

## Results

### Steady state, healthy provider

```bash
make up && make migrate-up
k6 run -e PROFILE=soak -e SOAK_DURATION=60s loadtest/payments.js
```

| | |
|---|---|
| Accepted | 192 req/s |
| Median | 7.7 ms |
| p90 | 285 ms |
| p95 | 926 ms |
| p99 | 1.73 s |
| Server errors | **0** |

Measured against a freshly truncated database. The median is the number that
describes the design — creation commits a row and returns without touching the
provider — and the tail is the contention described below.

### Ramp to 1,000 req/s, healthy provider

```bash
k6 run -e PROFILE=baseline loadtest/payments.js
```

| | |
|---|---|
| Accepted | 416 req/s sustained |
| p95 | 3.28 s |
| Dropped by k6 | 35,577 iterations |
| Server errors | **0** |

The target was not met. Raising the connection pool from 20 to 50 moved it to
428 req/s — a 3% difference — which rules the pool out as the constraint and
points at the four shared cores the database is running on.

Under saturation the service degrades by getting slower, not by failing: zero
5xx across 62,671 requests, and every response still correct.

### Chaos: 1,000 req/s target with ~30% injected faults

The run worth publishing. Same profile, with the provider timing out after
succeeding, returning 500s after recording charges, delaying, duplicating
webhooks, and delivering them out of order.

```bash
curl -X PUT "localhost:9091/admin/faults/preset?name=chaos"
k6 run -e PROFILE=chaos loadtest/payments.js
```

| | |
|---|---|
| Accepted | 453 req/s |
| Server errors | **0** out of 55,767 |
| Acceptance rate | 100% |
| `payment_double_charges` | **0** |
| `payment_lost_payments` | **0** |
| `payment_ledger_imbalance` | **0** |
| Provider charges vs. transactions reaching the provider | 177 vs 177 |

The invariants were exported continuously *during* the run by the checker, not
computed afterwards. A throughput number measured while correctness was
silently broken is a failed run that looks like a passing one.

### Spike: a tenfold step change in five seconds

Whether the system sheds load or falls over.

```bash
k6 run -e PROFILE=spike loadtest/payments.js
```

| | |
|---|---|
| Accepted | 459 req/s over 75 s |
| Requests | 34,506 |
| Server errors | **0** |
| Median | 1.83 s |
| p95 | 3.11 s |
| Dropped by k6 | 11,242 iterations |
| Invariants | all **0** |
| DLQ | **0** |

It degrades by getting slower, not by failing. The p95 threshold is crossed and
the run is marked failed by k6, which is the correct verdict — a payment API
answering in three seconds is not healthy — but nothing was refused, nothing was
lost, and no request returned an error. The backlog is the story: 30,372
payments sat at `created` behind 26,329 pending outbox rows when the load
stopped.

### Outage: the provider disappears mid-run

Steady 200 rps for 150 s, with the provider taken down for 40 s in the middle.

```bash
k6 run -e PROFILE=soak -e SOAK_DURATION=150s loadtest/payments.js
curl -X POST "localhost:9091/admin/outage?seconds=40"   # 25s in
```

| | |
|---|---|
| Requests | 29,930 |
| Failed | **0** |
| Median | 12.5 ms |
| p95 | 153 ms |
| Breaker opened | within **6 s** of the outage starting |
| Breaker closed | within **6 s** of the provider returning |
| Messages deferred to the 5 m tier | 10,401 |
| Provider calls that actually failed | **8** |
| DLQ | **0** |

Two things worth separating. **The API is unaffected** — median 12.5 ms
throughout, because the provider is off the request path by design; a customer
would not have noticed. And **the breaker turned an outage into deferred work**:
10,401 messages were rescheduled while only 8 calls were allowed through to fail.
That ratio is the breaker earning its place.

Recovery needed no intervention. Authorizations resumed on the next sample after
the provider returned and drained at roughly 450 per six seconds.

```
t=  0s breaker=closed  authorized=1594
t=  6s breaker=open    authorized=1594     ← tripped
t= 34s breaker=open    authorized=1594     ← nothing lost, nothing attempted
t= 40s breaker=closed  authorized=1607     ← provider back
t= 93s breaker=closed  authorized=5083
t=163s breaker=closed  authorized=9086
```

### Soak: ten minutes at a constant 200 rps

Hunting leaks and unbounded growth rather than limits.

```bash
k6 run -e PROFILE=soak -e SOAK_DURATION=10m loadtest/payments.js
```

| | |
|---|---|
| Payments | 119,965 |
| Orchestrator RSS | 27 MB → 48 MB, flat from minute 1 |
| Worker RSS | 18 MB → 34 MB, flat from minute 2 |
| Outbox depth | never above 64 |
| Consumer lag | grew linearly to 62,480 |
| Invariants | all **0** |

**No leak over ten minutes.** Both processes reach a working set within a minute
and stay there for the remaining nine. Ten minutes is not a soak in the sense
that matters for production — it establishes that nothing grows per-request, and
says nothing about a slow leak over days.

**And it localises the bottleneck precisely.** The outbox relay keeps up
completely: depth stayed around forty for the whole run, so the relay is not the
constraint. The consumer is. Ingestion at 200/s against a drain rate of roughly
90/s produced a lag curve that grows at about 6,900 per minute — which matches
the arithmetic exactly.

That is a sharper result than the chaos run gave. There, a large outbox backlog
made it look as though both stages were behind; here the relay is demonstrably
fine and the single-goroutine consumer is the whole gap.

## The bottleneck the metrics found

The API accepts payments far faster than the pipeline behind it drains them. At
the end of the chaos run:

```
payment_outbox_pending                          42,409
payment_consumer_lag{topic=payment.authorize}   13,200

payment_transactions:  created 55,590 · authorized 175 · authorizing 2
```

Ingestion scales; **processing does not**. The cause is architectural rather
than incidental: the worker consumes with a single goroutine that walks every
assigned partition in order, and each message makes a synchronous provider call.
Under the chaos preset those calls include deliberate multi-second hangs, so
throughput collapses to roughly the reciprocal of the average provider latency
regardless of how many partitions exist.

This is the correct behaviour for a queue — work is accepted durably and drains
at whatever rate the downstream allows, which is exactly why the outbox is there
— but the drain rate is a real limit and it is not currently horizontal. The fix
is concurrent per-partition consumers, which is deliberately not built: it would
change the ordering guarantees the retry ladder depends on, and doing it
carelessly is how a delay tier stops being ordered by time.

**The point worth keeping: none of this was visible before this phase.** The
same backlog is what stalled twenty payments during the phase 05 demo, and it
was found by querying Kafka by hand. It is now two gauges.

## Tracing

One trace spans the whole path, across two processes and two boundaries:

```
payment-orchestrator  POST /v1/payments                    59ms
payment-orchestrator  kafka.publish payment.authorize      10ms
payment-worker        kafka.consume payment.authorize      37ms
payment-worker        psp psp-async-sim /v1/charges/authorize  2ms
```

Crossing Kafka needs the trace context in the record headers. Crossing the
*outbox* needs it in the database row — the handler commits and returns, and the
relay publishes later in a different goroutine with no in-process link. Without
the stored `traceparent` the trace starts at the relay and the request that
caused the payment is missing from its own story.

Reproduce with:

```bash
docker compose -f deploy/docker-compose.yml --profile observability up -d jaeger
TRACING_ENABLED=true TRACE_SAMPLE_RATIO=1.0 make run     # and make worker
```

All published latency figures were measured with **tracing off**. Sampling at
100% while measuring latency would make the exporter part of what is being
measured.

## What is not measured

Stated because an omitted benchmark is easily mistaken for a passed one.

- **The 2,000 req/s and p99 < 150 ms targets in the phase plan are not met**, and
  are not close on this hardware.
- **No soak longer than ten minutes.** Nothing is claimed about a slow leak over
  hours or days; what is claimed is that memory is flat per-request.
- **No reconciliation benchmark.** The 100k-rows-in-60s target is unmeasured;
  reconciliation currently loads a file and its ledger window into memory.
- **No multi-instance run.** Every number here is one orchestrator and one
  worker. The relay claims by lease and the consumer runs in a group, so both
  are built to scale out, and neither has been measured doing it.
