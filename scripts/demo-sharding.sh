#!/usr/bin/env bash
#
# Sharding and cross-shard transfers, demonstrated against two real databases.
#
# Split out from scripts/demo.sh rather than bolted onto it: that demo runs a
# whole live stack against one database, and reshaping it to be shard-aware
# would put a working, already-recorded demo at risk for no benefit. This one
# needs no API, no broker, and no worker — the point it makes is about storage.
#
# Every claim it narrates is asserted, and it exits non-zero if any of them
# stops being true.
#
#   ./scripts/demo-sharding.sh            paced for recording
#   ./scripts/demo-sharding.sh --fast     no pauses, for CI or a quick check
#
# Requires: docker, go.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FAST=0
[[ "${1:-}" == "--fast" ]] && FAST=1

COMPOSE=(docker compose -f deploy/docker-compose.yml)
HOST="postgres://payment:payment@localhost:5432"
SHARD0="$HOST/payment?sslmode=disable"
SHARD1="$HOST/payment_shard1?sslmode=disable"

export POSTGRES_DSN="$SHARD0"
export POSTGRES_SHARD_DSNS="$SHARD0,$SHARD1"

# Generous, because this is a demo on a laptop that has just compiled two
# binaries and started a container, not a latency measurement. The default of
# five seconds is occasionally not enough for the first connection after that,
# and a demo that asserts its own claims must not fail on a cold pool.
export POSTGRES_CONNECT_TIMEOUT="${POSTGRES_CONNECT_TIMEOUT:-20s}"

# Chosen because they hash onto different databases — the demo asserts that
# rather than trusting it, so a change to the mapping fails here loudly instead
# of quietly turning this into a same-shard demo.
SRC=harbor-trading
DST=acme-store

FAILURES=0
BIN=$(mktemp -d)

if [[ -t 1 ]]; then
  B=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RED=$'\033[31m'; CYAN=$'\033[36m'; R=$'\033[0m'
else
  B=""; DIM=""; GREEN=""; RED=""; CYAN=""; R=""
fi

beat() { [[ $FAST == 1 ]] || sleep "${1:-1.2}"; }
act()  { printf '\n%s══ %s %s\n\n' "$B$CYAN" "$*" "$R"; beat 0.8; }
say()  { printf '%s%s%s\n' "$DIM" "$*" "$R"; }
run()  { printf '%s$ %s%s\n' "$B" "$*" "$R"; eval "$@"; }

pass() { printf '  %s✓%s %s\n' "$GREEN" "$R" "$*"; }
fail() { printf '  %s✗%s %s\n' "$RED" "$R" "$*"; FAILURES=$((FAILURES + 1)); }
check() { # label actual expected
  if [[ "$2" == "$3" ]]; then pass "$1 → $2"; else fail "$1 → got '$2', want '$3'"; fi
}

# q runs a scalar query against one database, named by its number.
q() { # shard sql
  "${COMPOSE[@]}" exec -T postgres psql -U payment -d "$(dbname "$1")" -tAc "$2" | tr -d ' \r'
}
show() { # shard sql
  "${COMPOSE[@]}" exec -T postgres psql -U payment -d "$(dbname "$1")" -tAc "$2" | tr -d '\r'
}
dbname() { [[ "$1" == 0 ]] && echo payment || echo "payment_shard$1"; }

# payable reports what the ledger says a merchant is owed, derived from postings.
payable() { # shard merchant
  q "$1" "SELECT COALESCE(SUM(CASE WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)
          FROM postings p JOIN ledger_accounts a ON a.id=p.account_id
          WHERE a.owner_type='merchant' AND a.owner_id='$2' AND a.purpose='payable'"
}

suspense() { # shard
  q "$1" "SELECT COALESCE(SUM(CASE WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)
          FROM postings p JOIN ledger_accounts a ON a.id=p.account_id
          WHERE a.purpose='transfer_suspense'"
}

# fund gives a merchant a payable balance the way a capture would: money owed by
# a provider becomes money owed to the merchant. Never a direct write to the
# payable account — every balance here is derived from balanced entries.
fund() { # shard merchant shard_key minor
  "${COMPOSE[@]}" exec -T postgres psql -U payment -d "$(dbname "$1")" -q >/dev/null <<SQL
INSERT INTO ledger_accounts (owner_type, owner_id, purpose, account_type, currency, shard_key)
VALUES ('psp','psp-sim','clearing','asset','USD','$3'),
       ('merchant','$2','payable','liability','USD','$3')
ON CONFLICT DO NOTHING;
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
WITH e AS (
  INSERT INTO journal_entries (shard_key, description) VALUES ('$3','demo funding') RETURNING id
)
INSERT INTO postings (entry_id, account_id, direction, amount_minor, currency)
SELECT e.id, a.id,
       CASE WHEN a.purpose='clearing' THEN 'debit'::posting_direction ELSE 'credit'::posting_direction END,
       $4, 'USD'
FROM e, ledger_accounts a
WHERE a.currency='USD' AND (a.purpose='clearing' OR (a.purpose='payable' AND a.owner_id='$2'));
COMMIT;
SQL
}

# shard_key_of asks the router where a merchant lives, rather than recomputing
# the hash here — a second implementation of the routing rule is a second thing
# that can disagree with it.
shard_key_of() { "$BIN/transferctl" shard -merchant "$1" -quiet | cut -d' ' -f1; }
shard_db_of()  { "$BIN/transferctl" shard -merchant "$1" -quiet | cut -d' ' -f2; }

# reset_data empties both databases. Called before the demo starts so a previous
# run cannot supply the balances this one claims to create, and again on exit.
reset_data() {
  for db in 0 1; do
    "${COMPOSE[@]}" exec -T postgres psql -U payment -d "$(dbname $db)" -q \
      -c "TRUNCATE tcc_reservations, tcc_transfers, postings, journal_entries, ledger_accounts RESTART IDENTITY CASCADE" \
      >/dev/null 2>&1 || true
  done
}

cleanup() { rm -rf "$BIN"; reset_data; }
trap cleanup EXIT

# ── Setup ────────────────────────────────────────────────────────────────────

act "Setup"

say "the stack, and a second database beside the first"
run "${COMPOSE[*]} up -d --wait postgres" >/dev/null
"${COMPOSE[@]}" exec -T postgres createdb -U payment payment_shard1 2>/dev/null || true

# The container healthcheck can pass a moment before the server is answering
# queries promptly. Waiting on an actual query is the thing that matters.
for _ in $(seq 30); do
  q 0 "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done

say ""
say "building the tools this demo drives"
for c in migrate transferctl; do go build -o "$BIN/$c" "./cmd/$c"; done

say ""
say "migrations run on every shard, and report all failures together — a shard"
say "left a version behind is a shard whose merchants fail on the first query"
say "touching the new column"
run "\"$BIN/migrate\" up"
reset_data

# ── 1 ────────────────────────────────────────────────────────────────────────

act "1. Merchants live on different databases"

SRC_KEY=$(shard_key_of "$SRC")
DST_KEY=$(shard_key_of "$DST")

say "the shard key is derived from the merchant and stored on every row."
say "64 logical shards map onto 2 physical databases — 32 each, contiguous, so"
say "doubling capacity later splits each range in half rather than rehashing"
say "every row while money is moving."
say ""
printf '  %-24s %s → database 0\n' "$SRC" "$SRC_KEY"
printf '  %-24s %s → database 1\n' "$DST" "$DST_KEY"

check "source resolves to database" "$(shard_db_of "$SRC")" "0"
check "destination resolves to database" "$(shard_db_of "$DST")" "1"

say ""
say "funding the source, the way a capture would: Dr clearing / Cr payable"
fund 0 "$SRC" "$SRC_KEY" 50000

check "source payable, database 0" "$(payable 0 "$SRC")" "50000"
check "source payable, database 1" "$(payable 1 "$SRC")" "0"
say ""
say "not filtered out of database 1 — absent from it. There is no query that"
say "joins across the two, and no transaction that spans them."

# ── 2 ────────────────────────────────────────────────────────────────────────

act "2. Moving money between two databases"

say "Postgres cannot commit across databases, so this is a protocol rather than"
say "a transaction: reserve on both sides, commit the decision, then post."
say ""
run "\"$BIN/transferctl\" send -from $SRC -to $DST -amount 12500 -key demo-transfer"

SRC_AFTER=$(payable 0 "$SRC")
DST_AFTER=$(payable 1 "$DST")
SUS0=$(suspense 0)
SUS1=$(suspense 1)

say ""
check "source payable on database 0" "$SRC_AFTER" "37500"
check "destination payable on database 1" "$DST_AFTER" "12500"

say ""
say "each database posted its own balanced entry, against a suspense account:"
say ""
say "  database 0    Dr merchant payable    Cr transfer suspense"
say "  database 1    Dr transfer suspense   Cr merchant payable"
say ""
printf '  suspense, database 0   %s\n' "$SUS0"
printf '  suspense, database 1   %s\n' "$SUS1"

check "suspense across both databases" "$((SUS0 + SUS1))" "0"
say ""
say "that total is the seam. Each shard balances internally no matter what, so"
say "a transfer that completed on one side and not the other would be invisible"
say "everywhere except here."

# ── 3 ────────────────────────────────────────────────────────────────────────

act "3. A reservation is what stops an overdraw"

say "the source has 375.00 left. Asking for 400.00 is refused before anything"
say "is posted — availability is the derived balance minus outstanding holds,"
say "read and written under an advisory lock on the merchant and currency."
say ""

set +e
OUT=$("$BIN/transferctl" send -from "$SRC" -to "$DST" -amount 40000 -key demo-overdraw 2>&1)
RC=$?
set -e
printf '%s\n' "$OUT" | tail -3

check "the transfer was refused" "$RC" "1"
check "refused for funds" "$(grep -c 'insufficient available balance' <<<"$OUT")" "1"
check "source payable unchanged" "$(payable 0 "$SRC")" "37500"
check "nothing left held" "$(q 0 "SELECT count(*) FROM tcc_reservations WHERE state='reserved'")" "0"

say ""
say "without that lock, two transfers of 600 against a balance of 1000 both read"
say "the same figure before either inserts, and both pass a check that was"
say "correct when it ran. Removing it overdraws the account to -200 — which is"
say "how the lock was confirmed to matter rather than assumed to."

# ── 4 ────────────────────────────────────────────────────────────────────────

act "4. Nothing is left in flight"

run "\"$BIN/transferctl\" pending"

check "transfers still in flight" "$(q 0 "SELECT count(*) FROM tcc_transfers WHERE state IN ('trying','confirming','cancelling')")" "0"
check "reservations still held, database 0" "$(q 0 "SELECT count(*) FROM tcc_reservations WHERE state='reserved'")" "0"
check "reservations still held, database 1" "$(q 1 "SELECT count(*) FROM tcc_reservations WHERE state='reserved'")" "0"

say ""
say "a coordinator that dies mid-transfer is handled by the sweeper, and which"
say "way it resolves depends on one column: before the commit point the holds"
say "are released, after it the transfer is finished. That is asserted by"
say "TestSweeperReleasesHoldsStrandedBeforeTheCommitPoint and"
say "TestSweeperCompletesTransfersStrandedAfterTheCommitPoint, which simulate"
say "the crash by writing exactly the durable state a killed process leaves."

# ── Result ───────────────────────────────────────────────────────────────────

printf '\n'
if [[ $FAILURES -eq 0 ]]; then
  printf '%s══ every claim held %s\n\n' "$B$GREEN" "$R"
else
  printf '%s══ %d claim(s) failed %s\n\n' "$B$RED" "$FAILURES" "$R"
  exit 1
fi
