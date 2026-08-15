# Benchmarks

Every number here comes with the command that produced it and the machine it
ran on. A throughput figure without both is a claim, not a measurement.

**There is no published throughput number yet, deliberately.** See
[Why throughput is not published](#why-throughput-is-not-published).

## Method

```bash
make up && make migrate-up
make pspsim            # the provider that fails on purpose
make worker            # consumes Kafka, calls providers
make run               # the API, exporting /metrics

k6 run -e PROFILE=baseline -e RUN_ID=$(date +%s) loadtest/payments.js
```

Profiles are in [`loadtest/payments.js`](../../loadtest/payments.js):
`smoke`, `baseline`, `chaos`, `spike`, `soak`. They share one request path on
purpose — a chaos run has to exercise exactly what the baseline exercises, or
comparing them means nothing.

Fault injection is set out of band, so the same script measures a healthy and a
failing provider:

```bash
make healthy    # no faults
make chaos      # the full fault catalogue
make outage SECONDS=90
```

k6 thresholds are assertions, not decoration: the run exits non-zero when one is
crossed, so a degraded run cannot be reported as a passing one.

## What was measured

Runs on 2026-08-15. Apple Silicon laptop, 10 cores, with Postgres and Kafka in
Docker Desktop limited to **4 CPUs and 8 GB**. The API, worker, provider
simulator, and k6 all ran on the host alongside the operator's normal
applications.

### Correctness under load — the part that is publishable

| Measure | Result |
|---|---|
| Payments created across all load runs | **164,933** |
| Ledger imbalance | **0** |
| Double charges | **0** |
| Lost payments (captured, no ledger entry) | **0** |
| 5xx responses | 4 |
| Payments returned in a state other than `created` | 0 |

The four failures were `statement timeout` on the idempotency claim, during a
deliberately degraded experiment while the host was heavily oversubscribed. That
is the failure mode worth having: under contention the system **refused work
rather than corrupting state**. Every invariant held throughout, including
across that period.

The invariant checker runs continuously during load — a run that reports
throughput while the ledger is quietly unbalanced is a failed run that looks
like a passing one. It is itself tested against seeded violations
([`checker_test.go`](../../internal/invariant/checker_test.go)), because a
checker that always returns zero would pass every load test ever run against it.

### Latency, on a clean database at 200 requests/second

| Percentile | Latency |
|---|---|
| median | **7.65 ms** |
| p90 | 285 ms |
| p95 | 927 ms |
| p99 | 1.73 s |

The median is the interesting figure: creation writes the transaction, its audit
row, and the outbox message in one database transaction and returns, without
waiting on a provider. The tail is not the application's own work — see below.

## Why throughput is not published

The measured host was at a **load average of 46 on 10 cores** for the duration
of testing, dominated by unrelated desktop applications (one at 420% CPU) and
Docker Desktop's own virtualisation. Repeated runs of the identical profile
produced between **33 and 428 requests/second**.

A spread that wide is not a measurement of this system. Publishing the best of
those runs would be picking a number; publishing the mean would be averaging
noise. The plan for this phase anticipated the constraint and said an honest
800 beats a fabricated 2,000 — this goes one step further: an honest *nothing*
beats an honest number that is really a measurement of a laptop's spare capacity.

What the runs do establish:

- **Postgres is the ceiling, not the pool.** Raising `POSTGRES_MAX_CONNS` from
  20 to 50 moved throughput from 416 to 428 requests/second — inside the noise.
  With four shared CPUs and roughly five writes per payment, the database
  saturates first.
- **The tail is contention, not application work.** Median 7.65 ms against a p99
  of 1.73 s is a hundredfold spread with no corresponding work to explain it.
  Disabling the invariant checker made the tail *worse*, not better, which rules
  out the obvious suspect and points at the environment.

## To produce a number worth publishing

Run the same profiles against dedicated hardware, or against managed Postgres
and Kafka, with nothing else on the box:

```bash
KAFKA_TOPIC_PREFIX=bench- k6 run -e PROFILE=baseline -e RUN_ID=bench1 loadtest/payments.js
```

Record the host specification, the seed, and the exact command alongside the
result. Until that happens, this document says what it knows and no more.

## Unresolved

- Throughput and p99 under load, on hardware that can support the measurement.
- Recovery time after a full provider outage — the `outage` profile exists but
  has not been run to completion.
- Soak behaviour over hours: the `soak` profile has only been run for minutes,
  which is long enough to exercise the path and far too short to find a leak.
