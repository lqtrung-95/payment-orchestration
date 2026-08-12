#!/usr/bin/env bash
#
# End-to-end demo of the payment orchestrator, and its own test.
#
# Every claim it narrates is asserted, and the script exits non-zero if any of
# them stops being true. That is deliberate: a demo nobody verifies rots
# silently, and the worst place to discover it has rotted is on camera.
#
#   ./scripts/demo.sh            paced for recording
#   ./scripts/demo.sh --fast     no pauses, for CI or a quick check
#
# Requires: docker, go, curl, jq.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FAST=0
[[ "${1:-}" == "--fast" ]] && FAST=1

API=http://localhost:8080
SIM=http://localhost:9091
MERCHANT=m_acme
COMPOSE=(docker compose -f deploy/docker-compose.yml)
export POSTGRES_DSN="${POSTGRES_DSN:-postgres://payment:payment@localhost:5432/payment?sslmode=disable}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"

# A unique topic namespace per run. Without it a demo inherits the previous
# run's backlog and spends its first minute working through history.
export KAFKA_TOPIC_PREFIX="demo-$(date +%s)."

LOGS=".demo-logs"
FAILURES=0

if [[ -t 1 ]]; then
  B=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'; CYAN=$'\033[36m'; R=$'\033[0m'
else
  B=""; DIM=""; GREEN=""; RED=""; CYAN=""; R=""
fi

beat()  { [[ $FAST == 1 ]] || sleep "${1:-1.2}"; }
act()   { printf '\n%s══ %s %s\n\n' "$B$CYAN" "$*" "$R"; beat 0.8; }
say()   { printf '%s%s%s\n' "$DIM" "$*" "$R"; }
run()   { printf '%s$ %s%s\n' "$B" "$*" "$R"; eval "$@"; }

pass()  { printf '  %s✓%s %s\n' "$GREEN" "$R" "$*"; }
fail()  { printf '  %s✗%s %s\n' "$RED" "$R" "$*"; FAILURES=$((FAILURES + 1)); }
check() { # label actual expected
  if [[ "$2" == "$3" ]]; then pass "$1 → $2"; else fail "$1 → got '$2', want '$3'"; fi
}
check_min() { # label actual minimum
  if [[ "$2" -ge "$3" ]]; then pass "$1 → $2"; else fail "$1 → got '$2', want at least '$3'"; fi
}

# q returns a scalar, stripped for comparison. show prints a formatted result,
# keeping the whitespace the query built.
q()    { "${COMPOSE[@]}" exec -T postgres psql -U payment -d payment -tAc "$1" | tr -d ' \r'; }
show() { "${COMPOSE[@]}" exec -T postgres psql -U payment -d payment -tAc "$1" | tr -d '\r'; }
charges() { curl -sf "$SIM/admin/charges" | jq -r .count; }
preset()  { curl -sf -X PUT "$SIM/admin/faults/preset?name=$1" >/dev/null; }
fault()   { curl -sf -X PUT "$SIM/admin/faults" -H 'Content-Type: application/json' \
              -d "{\"probabilities\":{\"$1\":$2}}" >/dev/null; }

# pay creates a payment and echoes its id.
pay() { # key amount
  curl -sf -X POST "$API/v1/payments" \
    -H "X-Merchant-Id: $MERCHANT" -H "Idempotency-Key: $1" \
    -H 'Content-Type: application/json' \
    -d "{\"amount\":$2,\"currency\":\"USD\"}" | jq -r .id
}

state() { curl -sf -H "X-Merchant-Id: $MERCHANT" "$API/v1/payments/$1" | jq -r .state; }

# await_state polls until the payment reaches one of the wanted states.
await_state() { # id timeout_s want...
  local id=$1 timeout=$2 deadline=$((SECONDS + $2)) s=""; shift 2
  while ((SECONDS < deadline)); do
    s=$(state "$id" || echo "?")
    for want in "$@"; do [[ "$s" == "$want" ]] && { echo "$s"; return 0; }; done
    sleep 0.3
  done
  echo "${s:-timeout after ${timeout}s}"
  return 1
}

cleanup() {
  local code=$?
  say "stopping demo processes"
  if [[ -n "${PIDS:-}" ]]; then
    kill $PIDS 2>/dev/null || true
    # Give them a moment to shut down cleanly, then insist. A server still
    # holding its port is what makes the *next* run fail to start.
    for _ in $(seq 20); do
      kill -0 $PIDS 2>/dev/null || break
      sleep 0.25
    done
    kill -9 $PIDS 2>/dev/null || true
  fi
  wait 2>/dev/null || true
  [[ -n "${BIN:-}" ]] && rm -rf "$BIN"

  # Each run provisions six topics of twelve partitions. Left behind, they
  # accumulate on the development broker until metadata handling gets slow and
  # unrelated tests start looking flaky.
  say "removing this run's topics"
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete \
    --topic "${KAFKA_TOPIC_PREFIX//./\\.}.*" >/dev/null 2>&1 || true

  exit $code
}
trap cleanup EXIT INT TERM

# ── Setup ────────────────────────────────────────────────────────────────────

act "Setup"

for port in 8080 9091; do
  if pids=$(lsof -ti tcp:$port 2>/dev/null) && [[ -n "$pids" ]]; then
    echo "port $port is already in use by pid(s): $pids" >&2
    echo "free it with:  lsof -ti tcp:$port | xargs kill" >&2
    exit 1
  fi
done

mkdir -p "$LOGS"
run "${COMPOSE[*]} up -d --wait" >/dev/null
run "go run ./cmd/migrate up"

q "TRUNCATE webhook_events_raw, transaction_state_changes, postings, journal_entries,
     idempotency_keys, outbox, processed_events, payment_transactions, ledger_accounts
   RESTART IDENTITY CASCADE" >/dev/null

say "starting the provider simulator, the API, and a worker"
say "the worker uses the ASYNC provider: it answers 'pending', so the outcome"
say "can only ever arrive as a webhook"

# Built first, then run directly. `go run` starts a compiled child and killing
# the parent leaves that child alive, still holding its port — so cleanup would
# quietly orphan a server and the next run would refuse to start.
BIN=$(mktemp -d)
for c in pspsim orchestrator worker; do go build -o "$BIN/$c" "./cmd/$c"; done

PSPSIM_WEBHOOK_URL="$API/webhooks/psp-sim" "$BIN/pspsim" >"$LOGS/pspsim.log" 2>&1 &
PIDS="$!"
LOG_FORMAT=text "$BIN/orchestrator" >"$LOGS/orchestrator.log" 2>&1 &
PIDS="$PIDS $!"
LOG_FORMAT=text PSP_DEFAULT_PROVIDER=psp-async-sim "$BIN/worker" >"$LOGS/worker.log" 2>&1 &
PIDS="$PIDS $!"

printf '%swaiting for services%s' "$DIM" "$R"
for _ in $(seq 60); do
  if curl -sf "$API/healthz" >/dev/null 2>&1 && curl -sf "$SIM/healthz" >/dev/null 2>&1; then break; fi
  printf '.'; sleep 1
done
printf '\n'
curl -sf "$API/readyz" | jq -c . || { echo "service never became ready" >&2; exit 1; }
preset healthy

# ── 1. Authorization is off the request path ─────────────────────────────────

act "1. The caller never waits on a third party"

say "POST /v1/payments writes the transaction, its audit row, and the queue"
say "message in ONE database transaction, then returns."

start=$(date +%s%N)
id=$(pay demo-001 12550)
elapsed=$(( ($(date +%s%N) - start) / 1000000 ))

printf '  created %s in %sms\n' "$id" "$elapsed"
check "state on create" "$(state "$id")" "created"
[[ $elapsed -lt 200 ]] && pass "returned in ${elapsed}ms (no provider in the request path)" \
                       || fail "took ${elapsed}ms — a provider call leaked into the request path"

say "the worker authorizes, the provider answers 'pending', and a signed webhook"
say "carries the real outcome back"
check "resolved by webhook" "$(await_state "$id" 40 authorized)" "authorized"
check "provider charges" "$(charges)" "1"
beat

# ── 2. Idempotency ───────────────────────────────────────────────────────────

act "2. One key, one payment"

say "the same key again — byte-identical replay, no second transaction"
replay_hdr=$(curl -sf -D - -o /dev/null -X POST "$API/v1/payments" \
  -H "X-Merchant-Id: $MERCHANT" -H "Idempotency-Key: demo-001" \
  -H 'Content-Type: application/json' -d '{"amount":12550,"currency":"USD"}' \
  | grep -i '^idempotency-replayed' | tr -d '\r' || true)
printf '  %s\n' "${replay_hdr:-<no replay header>}"
check "replayed" "$(echo "$replay_hdr" | grep -ci true || true)" "1"
check "transactions" "$(q 'SELECT count(*) FROM payment_transactions')" "1"

say "the same key with a DIFFERENT amount — 409, not a silent replay that would"
say "discard the second payment"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/v1/payments" \
  -H "X-Merchant-Id: $MERCHANT" -H "Idempotency-Key: demo-001" \
  -H 'Content-Type: application/json' -d '{"amount":99999,"currency":"USD"}')
check "conflicting body" "$code" "409"
beat

# ── 3. The headline: charged exactly once ────────────────────────────────────

act "3. The provider succeeds, then hides it — and we do not charge twice"

say "forcing timeout_after_success: the charge IS recorded, then the connection"
say "hangs. The caller is never told it worked. Retrying here charges twice;"
say "assuming failure writes off money the customer actually paid."
fault timeout_after_success 1.0

before=$(charges)
id=$(pay demo-ambiguous 33300)
check "recovered" "$(await_state "$id" 60 authorized)" "authorized"
after=$(charges)

printf '  provider charges before %s, after %s\n' "$before" "$after"
check "charges created" "$((after - before))" "1"
say "resolved by asking GetStatus, never by guessing:"
show "SELECT '    ' || rpad(to_state::text, 13) || COALESCE(reason,'')
      FROM transaction_state_changes WHERE transaction_id='$id' ORDER BY id"
fault timeout_after_success 0
beat

# ── 4. A decline is terminal ─────────────────────────────────────────────────

act "4. A decline is never retried"

say "amount ending in 51 — the last two digits are the real ISO-8583 code, so"
say "this declines for insufficient funds"
before=$(charges)
id=$(pay demo-declined 10051)
check "state" "$(await_state "$id" 40 failed)" "failed"
check "charges created" "$(($(charges) - before))" "0"
say "repeating a decline cannot change the issuer's answer, and it escalates"
say "fraud controls — so the retry ladder is never entered"
beat

# ── 5. Webhooks under chaos ──────────────────────────────────────────────────

act "5. Webhooks: duplicated, out of order, sometimes early"

say "chaos preset, with the three webhook faults forced on so this act shows the"
say "same thing every run rather than whatever the dice allow"
preset chaos
fault duplicate_webhook 1.0        # every event delivered three times
fault out_of_order_webhook 1.0     # a stale event after a newer one
fault webhook_before_response 0.34 # a third arrive before the API reply

# Measured as deltas against this point, so the earlier acts cannot skew it.
# Amounts step by 100 to stay clear of the magic decline codes — an amount
# ending in 05 or 51 declines by design, which would look like a chaos failure.
auth_before=$(q "SELECT count(*) FROM payment_transactions WHERE state='authorized'")
chg_before=$(charges)

for i in $(seq 1 12); do pay "demo-chaos-$i" $((20000 + i * 100)) >/dev/null; done
say "waiting for the callbacks to land"
for _ in $(seq 90); do
  now=$(q "SELECT count(*) FROM payment_transactions WHERE state='authorized'")
  [[ $((now - auth_before)) -ge 12 ]] && break
  sleep 1
done
preset healthy
# A real sleep, not a narration beat: the duplicates and the stale event arrive
# after the payment is already authorized, so --fast must still wait for them.
sleep 3

printf '\n  %swebhook outcomes%s\n' "$B" "$R"
show "SELECT '    ' || rpad(status::text, 12) || count(*)
      FROM webhook_events_raw GROUP BY status ORDER BY 1"

served=$(grep -c 'path=/webhooks/psp-sim status=200' "$LOGS/orchestrator.log" || true)
other=$(grep 'path=/webhooks/psp-sim' "$LOGS/orchestrator.log" | grep -vc 'status=200' || true)
dupes=$(grep -c 'duplicate webhook delivery' "$LOGS/orchestrator.log" || true)
printf '\n  %s deliveries served 200, %s duplicates absorbed\n' "$served" "$dupes"

check "non-200 responses" "$other" "0"
check "chaos payments authorized" \
  "$(($(q "SELECT count(*) FROM payment_transactions WHERE state='authorized'") - auth_before))" "12"
check "charges at the provider" "$(($(charges) - chg_before))" "12"
check "transactions authorized twice" \
  "$(q "SELECT count(*) FROM (SELECT transaction_id FROM transaction_state_changes
        WHERE to_state='authorized' GROUP BY transaction_id HAVING count(*)>1) x")" "0"
check_min "stale events caught by the sequence guard" \
  "$(q "SELECT count(*) FROM webhook_events_raw WHERE status='superseded'")" "1"
say "a duplicate is the provider doing its job — every one is answered 200"
say "a stale event is recorded as superseded: never applied, never dropped"
beat

# ── 6. The log is safe to replay ─────────────────────────────────────────────

act "6. Replaying the whole webhook log changes nothing"

say "a log that quietly re-applies itself is worse than no log: it invites the"
say "exact recovery procedure that corrupts state"
# Checked before the replay itself, so a run that stored nothing cannot pass
# this act simply by having no work to do.
stored=$(q 'SELECT count(*) FROM webhook_events_raw')
if [[ "$stored" -eq 0 ]]; then
  fail "no webhook events stored — this act would pass vacuously"
elif run "go run ./cmd/webhookctl replay"; then
  pass "$stored stored events, none would re-apply"
else
  fail "replay would change state"
fi
beat

# ── 7. Nothing terminal without evidence ─────────────────────────────────────

act "7. An unreachable provider never produces a failed payment"

say "taking the provider down entirely"
curl -sf -X POST "$SIM/admin/outage?seconds=25" | jq -c .
id=$(pay demo-outage 7700)
sleep 8
s=$(state "$id")
printf '  state while the provider is down: %s\n' "$s"
if [[ "$s" == "failed" ]]; then
  fail "marked failed on no evidence — that is money quietly written off"
else
  pass "still $s, not failed"
fi
say "a false 'authorized' is caught by reconciliation; a false 'failed' is a"
say "customer charged with no record of it"

# ── Summary ──────────────────────────────────────────────────────────────────

act "Summary"
show "SELECT '  ' || rpad(state::text, 14) || count(*) FROM payment_transactions GROUP BY state ORDER BY 1"
printf '\n'
if [[ $FAILURES -eq 0 ]]; then
  printf '%s  every claim in this demo was asserted and held.%s\n\n' "$GREEN$B" "$R"
else
  printf '%s  %d assertion(s) failed — see above.%s\n\n' "$RED$B" "$FAILURES" "$R"
  exit 1
fi
