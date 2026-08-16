package tcc_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/tcc"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

// fixture wires the coordinator against two genuinely separate databases.
//
// Two databases rather than two schemas, because the protocol only earns its
// complexity when a single transaction is impossible. Against one database
// every one of these tests would pass with the protocol removed.
type fixture struct {
	router      *postgres.Router
	coordinator *tcc.Coordinator
	sweeper     *tcc.Sweeper
	participant *tcc.Participant
	ledger      *ledgerstore.Repository
}

func newFixture(t *testing.T, cfg tcc.Config) *fixture {
	t.Helper()

	router := testsupport.FreshRouter(t, 2)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	coordinator := tcc.NewCoordinator(router, cfg, logger)

	return &fixture{
		router:      router,
		coordinator: coordinator,
		sweeper:     tcc.NewSweeper(coordinator, 50, logger),
		participant: tcc.NewParticipant(),
		ledger:      ledgerstore.NewRepository(),
	}
}

// merchantOnShard returns a merchant name the mapping sends to the given
// physical database, so a test can state which side of the split it means.
func merchantOnShard(t *testing.T, f *fixture, physical int, label string) string {
	t.Helper()

	for i := 0; i < 500; i++ {
		name := fmt.Sprintf("%s-%d", label, i)
		got, err := f.router.Mapping().Resolve(sharding.KeyForMerchant(name))
		if err != nil {
			t.Fatal(err)
		}
		if got == physical {
			return name
		}
	}
	t.Fatalf("no merchant named %s-* maps to physical shard %d", label, physical)
	return ""
}

// fund gives a merchant a payable balance the way a capture would: money owed
// by a provider becomes money owed to the merchant.
//
// Deliberately not a direct write to the payable account. Every balance in this
// system is derived from balanced entries, and a fixture that bypassed that
// would be testing the transfer against a state the application could never
// produce.
func fund(t *testing.T, f *fixture, merchant string, minor int64) {
	t.Helper()

	ctx := context.Background()
	shardKey := sharding.KeyForMerchant(merchant)
	amount := money.MustNew(minor, "USD")

	err := f.router.WithTx(ctx, shardKey, func(ctx context.Context, tx pgx.Tx) error {
		clearing, err := f.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
			Owner:    ledgerdomain.Owner{Type: "psp", ID: "psp-test"},
			Purpose:  ledgerdomain.PurposeClearing,
			Type:     ledgerdomain.AccountTypeAsset,
			Currency: "USD", ShardKey: shardKey,
		})
		if err != nil {
			return err
		}
		payable, err := f.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
			Owner:    ledgerdomain.Owner{Type: "merchant", ID: merchant},
			Purpose:  ledgerdomain.PurposePayable,
			Type:     ledgerdomain.AccountTypeLiability,
			Currency: "USD", ShardKey: shardKey,
		})
		if err != nil {
			return err
		}

		entry, err := ledgerdomain.NewEntry(nil, shardKey, "test funding", time.Now().UTC(),
			ledgerdomain.Posting{AccountID: clearing.ID, Direction: ledgerdomain.Debit, Amount: amount},
			ledgerdomain.Posting{AccountID: payable.ID, Direction: ledgerdomain.Credit, Amount: amount},
		)
		if err != nil {
			return err
		}
		return f.ledger.RecordEntry(ctx, tx, entry)
	})
	if err != nil {
		t.Fatalf("fund %s: %v", merchant, err)
	}
}

// payableBalance is what the ledger says a merchant is owed, derived from
// postings on their own shard.
func payableBalance(t *testing.T, f *fixture, merchant string) int64 {
	t.Helper()

	db, err := f.router.Shard(sharding.KeyForMerchant(merchant))
	if err != nil {
		t.Fatal(err)
	}

	const query = `
		SELECT COALESCE(SUM(CASE WHEN p.direction = 'credit' THEN p.amount_minor ELSE -p.amount_minor END), 0)
		FROM postings p
		JOIN ledger_accounts a ON a.id = p.account_id
		WHERE a.owner_type = 'merchant' AND a.owner_id = $1 AND a.purpose = 'payable'`

	var balance int64
	if err := db.Pool().QueryRow(context.Background(), query, merchant).Scan(&balance); err != nil {
		t.Fatalf("payable balance for %s: %v", merchant, err)
	}
	return balance
}

// totalByPurpose sums a purpose's position across every shard, credit-positive.
//
// Summing across databases is the only way to see a cross-shard transfer whole.
// Each database's books balance on their own; the question these tests ask is
// whether the two halves agree, and that question has no answer inside either
// one of them.
func totalByPurpose(t *testing.T, f *fixture, purpose ledgerdomain.Purpose) int64 {
	t.Helper()

	const query = `
		SELECT COALESCE(SUM(CASE WHEN p.direction = 'credit' THEN p.amount_minor ELSE -p.amount_minor END), 0)
		FROM postings p
		JOIN ledger_accounts a ON a.id = p.account_id
		WHERE a.purpose = $1`

	var total int64
	for shard, db := range f.router.Shards() {
		var subtotal int64
		if err := db.Pool().QueryRow(context.Background(), query, string(purpose)).Scan(&subtotal); err != nil {
			t.Fatalf("sum %s on shard %d: %v", purpose, shard, err)
		}
		total += subtotal
	}
	return total
}

// outstandingHolds totals reservations still held across every shard. Anything
// left after a run finished is funds a merchant cannot spend and nobody owns.
func outstandingHolds(t *testing.T, f *fixture) int64 {
	t.Helper()

	var total int64
	for shard, db := range f.router.Shards() {
		held, err := f.participant.OutstandingHolds(context.Background(), db.Pool())
		if err != nil {
			t.Fatalf("outstanding holds on shard %d: %v", shard, err)
		}
		total += held
	}
	return total
}

// countEntriesFor counts the journal entries written for a transfer across all
// shards, which is how a duplicated confirm becomes visible.
func countEntriesFor(t *testing.T, f *fixture, transferID fmt.Stringer) int {
	t.Helper()

	var total int
	for shard, db := range f.router.Shards() {
		var count int
		err := db.Pool().QueryRow(context.Background(),
			`SELECT count(*) FROM journal_entries WHERE description LIKE $1`,
			"cross-shard transfer "+transferID.String()+"%").Scan(&count)
		if err != nil {
			t.Fatalf("count entries on shard %d: %v", shard, err)
		}
		total += count
	}
	return total
}
