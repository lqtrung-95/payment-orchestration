package payment_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	"github.com/lequoctrung/payment-orchestrator/internal/psp/simclient"
	"github.com/lequoctrung/payment-orchestrator/internal/service/payment"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

// A payment and everything written alongside it must land on one database.
//
// The outbox row is the one that matters most. It is written in the same
// transaction as the payment, so if it were ever to land elsewhere the write
// would either fail or — worse — succeed as two transactions, and the guarantee
// the outbox exists to provide would be gone. Asserting its location is
// asserting that atomicity survived sharding.
func TestPaymentAndItsOutboxRowShareOneShard(t *testing.T) {
	router := testsupport.FreshRouter(t, 2)
	service := shardedService(t, router)
	ctx := context.Background()

	type placed struct {
		id       uuid.UUID
		physical int
	}

	var payments []placed
	used := map[int]int{}

	for i := 0; i < 30; i++ {
		merchant := merchantName(i)

		physical, err := router.Mapping().Resolve(sharding.KeyForMerchant(merchant))
		if err != nil {
			t.Fatalf("%s: %v", merchant, err)
		}

		created, err := service.Create(ctx, payment.CreateInput{
			MerchantID:     merchant,
			IdempotencyKey: "idem-" + merchant,
			Amount:         money.MustNew(1500, "USD"),
			Actor:          "test",
		})
		if err != nil {
			t.Fatalf("%s: create: %v", merchant, err)
		}

		payments = append(payments, placed{id: created.ID, physical: physical})
		used[physical]++
	}

	if len(used) < 2 {
		t.Fatalf("all merchants landed on one database (%v); routing is not being exercised", used)
	}

	for _, p := range payments {
		for physical, db := range router.Shards() {
			want := 0
			if physical == p.physical {
				want = 1
			}

			for table, query := range map[string]string{
				"payment_transactions":      `SELECT count(*) FROM payment_transactions WHERE id = $1`,
				"transaction_state_changes": `SELECT count(*) FROM transaction_state_changes WHERE transaction_id = $1`,
				"outbox":                    `SELECT count(*) FROM outbox WHERE aggregate_id = $1`,
			} {
				var count int
				if err := db.Pool().QueryRow(ctx, query, p.id).Scan(&count); err != nil {
					t.Fatalf("count %s on shard %d: %v", table, physical, err)
				}
				if count != want {
					t.Errorf("payment %s: %s on shard %d has %d rows, want %d",
						p.id, table, physical, count, want)
				}
			}
		}
	}
}

// Reads route by merchant, so a payment is only reachable through the merchant
// that owns it. Asking with the wrong merchant looks in the wrong database and
// must answer not-found rather than reaching across.
func TestPaymentIsOnlyReachableThroughItsOwnMerchant(t *testing.T) {
	router := testsupport.FreshRouter(t, 2)
	service := shardedService(t, router)
	ctx := context.Background()

	owner, stranger := merchantsOnDifferentShards(t, router)

	created, err := service.Create(ctx, payment.CreateInput{
		MerchantID:     owner,
		IdempotencyKey: "idem-owner",
		Amount:         money.MustNew(2500, "USD"),
		Actor:          "test",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := service.Get(ctx, owner, created.ID); err != nil {
		t.Fatalf("owner cannot read its own payment: %v", err)
	}

	if _, err := service.Get(ctx, stranger, created.ID); err == nil {
		t.Errorf("a merchant on another shard read payment %s", created.ID)
	}

	// The ownership check alone would satisfy the assertion above, so the
	// sharding-specific half is asserted directly: the row is not merely
	// filtered out of the stranger's view, it is absent from the database their
	// reads go to. Nothing in the service could return it even if the ownership
	// check were removed.
	strangerDB, err := router.Shard(sharding.KeyForMerchant(stranger))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := strangerDB.Pool().QueryRow(ctx,
		`SELECT count(*) FROM payment_transactions WHERE id = $1`, created.ID).Scan(&count); err != nil {
		t.Fatalf("count on stranger's shard: %v", err)
	}
	if count != 0 {
		t.Errorf("payment %s exists in the stranger's database", created.ID)
	}
}

// merchantsOnDifferentShards finds two merchants the mapping sends to different
// databases, so the test is exercising a real cross-database miss rather than
// two names that happen to collide on one shard.
func merchantsOnDifferentShards(t *testing.T, router *postgres.Router) (a, b string) {
	t.Helper()

	first := merchantName(0)
	firstShard, err := router.Mapping().Resolve(sharding.KeyForMerchant(first))
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < 200; i++ {
		candidate := merchantName(i)
		shard, err := router.Mapping().Resolve(sharding.KeyForMerchant(candidate))
		if err != nil {
			t.Fatal(err)
		}
		if shard != firstShard {
			return first, candidate
		}
	}

	t.Fatal("no two merchants map to different databases; the mapping is not distributing")
	return "", ""
}

func merchantName(i int) string {
	return "shard-test-merchant-" + uuid.NewSHA1(uuid.Nil, []byte{byte(i)}).String()[:8]
}

// shardedService builds the real service over a multi-database router. The
// provider is never reached — every test here stops at Create — so a client
// pointed at an unused address is honest about what is under test.
func shardedService(t *testing.T, router *postgres.Router) *payment.Service {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	adapter := simclient.New(simclient.Config{Name: "psp-test", BaseURL: "http://127.0.0.1:1"})

	return payment.NewService(router, txstore.NewRepository(),
		psp.NewRegistry("psp-test", adapter), outbox.NewWriter(),
		messaging.DefaultTopics(), logger)
}
