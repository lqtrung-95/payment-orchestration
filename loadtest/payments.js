// Load profiles for the payment orchestrator.
//
// One script, several profiles, selected with `-e PROFILE=`. They share the
// request logic on purpose: a chaos run has to exercise exactly the same path
// as the baseline, or the comparison between them means nothing.
//
//   k6 run -e PROFILE=smoke    loadtest/payments.js
//   k6 run -e PROFILE=baseline loadtest/payments.js
//   k6 run -e PROFILE=chaos    loadtest/payments.js
//   k6 run -e PROFILE=spike    loadtest/payments.js
//   k6 run -e PROFILE=soak     loadtest/payments.js
//
// The fault rate is set on the simulator out of band (make chaos / make
// healthy); this script only generates traffic and checks responses.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import exec from 'k6/execution';

const API = __ENV.API || 'http://localhost:8080';
const MERCHANT = __ENV.MERCHANT || 'm_loadtest';

// A run id namespaces idempotency keys, so repeated runs against the same
// database do not collide and replay each other's payments — which would
// silently turn a load test into a replay test.
const RUN = __ENV.RUN_ID || `${Date.now()}`;

// Counted separately from k6's own http_req_failed, which treats any non-2xx as
// a failure. A 409 on a conflicting idempotency key is correct behaviour, and
// lumping it in with 500s would make the error rate meaningless.
const created = new Counter('payments_created');
const conflicts = new Counter('payments_conflict');
const serverErrors = new Counter('payments_server_error');
const acceptedRate = new Rate('payments_accepted');

const PROFILES = {
  // Correctness only. If this fails, nothing below is worth running.
  smoke: {
    executor: 'constant-arrival-rate',
    rate: 10, timeUnit: '1s', duration: '30s',
    preAllocatedVUs: 10, maxVUs: 50,
  },

  // Ramp to the target and hold, against a healthy provider.
  baseline: {
    executor: 'ramping-arrival-rate',
    startRate: 50, timeUnit: '1s',
    preAllocatedVUs: 100, maxVUs: 800,
    stages: [
      { target: 250, duration: '30s' },
      { target: 500, duration: '30s' },
      { target: 1000, duration: '30s' },
      { target: 1000, duration: '60s' },
    ],
  },

  // The same shape, run while the provider misbehaves. This is the number
  // worth publishing: throughput under load is ordinary, throughput under load
  // *with correctness held* is not.
  chaos: {
    executor: 'ramping-arrival-rate',
    startRate: 50, timeUnit: '1s',
    preAllocatedVUs: 100, maxVUs: 800,
    stages: [
      { target: 500, duration: '30s' },
      { target: 1000, duration: '30s' },
      { target: 1000, duration: '60s' },
    ],
  },

  // A step change, to see whether the system sheds load or falls over.
  spike: {
    executor: 'ramping-arrival-rate',
    startRate: 100, timeUnit: '1s',
    preAllocatedVUs: 200, maxVUs: 1500,
    stages: [
      { target: 100, duration: '20s' },
      { target: 1000, duration: '5s' },
      { target: 1000, duration: '30s' },
      { target: 100, duration: '20s' },
    ],
  },

  // Long and gentle, hunting leaks and unbounded growth rather than limits.
  soak: {
    executor: 'constant-arrival-rate',
    rate: 200, timeUnit: '1s', duration: __ENV.SOAK_DURATION || '10m',
    preAllocatedVUs: 100, maxVUs: 400,
  },
};

const profile = __ENV.PROFILE || 'smoke';
if (!PROFILES[profile]) {
  throw new Error(`unknown PROFILE ${profile}; expected one of ${Object.keys(PROFILES).join(', ')}`);
}

export const options = {
  scenarios: { [profile]: PROFILES[profile] },
  // Thresholds are assertions, not decoration: k6 exits non-zero when one
  // fails, so a run that degraded cannot be reported as a run that passed.
  thresholds: {
    // A server error is never acceptable, however busy the system is.
    payments_server_error: ['count==0'],
    // Latency is measured on creation, which does not wait on the provider —
    // so this is a claim about our own path, not about the provider's.
    http_req_duration: ['p(99)<500'],
    payments_accepted: ['rate>0.99'],
  },
  discardResponseBodies: false,
};

// Amounts avoid the magic decline codes. The last two digits of an amount are
// the ISO-8583 response code the simulator declines with, so an unlucky value
// would decline by design and read as an error under load.
const DECLINE_CODES = new Set([51, 5, 59, 54]);

function amount() {
  let cents;
  do {
    cents = 1000 + Math.floor(Math.random() * 90000);
  } while (DECLINE_CODES.has(cents % 100));
  return cents;
}

export default function () {
  // Unique per iteration, so every request is a genuinely new payment rather
  // than an idempotent replay of an earlier one.
  const key = `load-${RUN}-${exec.scenario.iterationInTest}`;

  const res = http.post(`${API}/v1/payments`,
    JSON.stringify({ amount: amount(), currency: 'USD' }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Merchant-Id': MERCHANT,
        'Idempotency-Key': key,
      },
      tags: { name: 'create_payment' },
    });

  if (res.status === 201) {
    created.add(1);
  } else if (res.status === 409) {
    conflicts.add(1);
  } else if (res.status >= 500) {
    serverErrors.add(1);
  }

  acceptedRate.add(res.status === 201 || res.status === 409);

  check(res, {
    'created or conflicted': (r) => r.status === 201 || r.status === 409,
    'no server error': (r) => r.status < 500,
    // Creation must return before the provider is involved. A body carrying a
    // resolved state would mean the provider leaked back onto the request path.
    'returned in created state': (r) =>
      r.status !== 201 || (r.json() || {}).state === 'created',
  });
}
