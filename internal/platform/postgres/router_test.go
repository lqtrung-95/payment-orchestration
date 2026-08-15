package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

// The claim under test is physical, not arithmetic: a merchant's rows exist in
// one database and genuinely do not exist in the other. The mapping tests cover
// which database gets chosen; this one covers that the choice is honoured all
// the way down to storage.
func TestRouterPlacesEachMerchantOnExactlyOneDatabase(t *testing.T) {
	router := testsupport.FreshRouter(t, 2)
	ctx := context.Background()

	// Enough merchants that both shards are certain to be exercised. A test
	// that happened to pick only shard-0 merchants would pass against a router
	// that ignored the key entirely.
	placements := map[string]int{}
	for i := 0; i < 40; i++ {
		merchant := fmt.Sprintf("merchant-%d", i)
		key := sharding.KeyForMerchant(merchant)

		physical, err := router.Mapping().Resolve(key)
		if err != nil {
			t.Fatalf("%s: resolve %q: %v", merchant, key, err)
		}
		placements[merchant] = physical

		if err := router.WithTx(ctx, key, func(ctx context.Context, tx pgx.Tx) error {
			return insertTransaction(ctx, tx, merchant, key)
		}); err != nil {
			t.Fatalf("%s: write to shard %d: %v", merchant, physical, err)
		}
	}

	used := map[int]int{}
	for _, physical := range placements {
		used[physical]++
	}
	if len(used) < 2 {
		t.Fatalf("every merchant landed on the same database (%v); the routing is not being exercised", used)
	}

	for merchant, expected := range placements {
		for physical, db := range router.Shards() {
			var count int
			err := db.Pool().QueryRow(ctx,
				`SELECT count(*) FROM payment_transactions WHERE merchant_id = $1`, merchant).Scan(&count)
			if err != nil {
				t.Fatalf("count %s on shard %d: %v", merchant, physical, err)
			}

			want := 0
			if physical == expected {
				want = 1
			}
			if count != want {
				t.Errorf("%s: shard %d holds %d rows, want %d (merchant belongs to shard %d)",
					merchant, physical, count, want, expected)
			}
		}
	}
}

// Reads must land where the writes did. Routing a read by re-deriving the key
// would be the same computation twice and would pass even if the stored key and
// the derived one disagreed; routing by the stored key is what is checked here.
func TestRouterReadsFollowTheStoredShardKey(t *testing.T) {
	router := testsupport.FreshRouter(t, 2)
	ctx := context.Background()

	const merchant = "merchant-read-path"
	key := sharding.KeyForMerchant(merchant)

	var id uuid.UUID
	if err := router.WithTx(ctx, key, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		id, err = insertTransactionReturningID(ctx, tx, merchant, key)
		return err
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	db, err := router.Shard(key)
	if err != nil {
		t.Fatalf("resolve shard: %v", err)
	}

	var stored string
	if err := db.Pool().QueryRow(ctx,
		`SELECT shard_key FROM payment_transactions WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatalf("read back from routed shard: %v", err)
	}
	if stored != key {
		t.Errorf("stored shard key %q, routed on %q", stored, key)
	}
}

func TestRouterRejectsUnusableShardKeys(t *testing.T) {
	router := testsupport.FreshRouter(t, 2)

	for _, key := range []string{"", "s64", "nonsense", "S00"} {
		if _, err := router.Shard(key); err == nil {
			t.Errorf("Shard(%q) resolved to a pool; a malformed key must not route anywhere", key)
		}
	}
}

func TestSingleDSNRouterIsOneShard(t *testing.T) {
	router := testsupport.FreshRouter(t, 1)

	if got := router.Mapping().Physical(); got != 1 {
		t.Fatalf("physical count = %d, want 1", got)
	}
	if got := len(router.Shards()); got != 1 {
		t.Fatalf("pool count = %d, want 1", got)
	}
	// Every logical shard must resolve; an unsharded deployment cannot have
	// merchants it is unable to route.
	for l := 0; l < sharding.LogicalShards; l++ {
		if _, err := router.Shard(fmt.Sprintf("s%02d", l)); err != nil {
			t.Fatalf("logical shard %d does not route on a single-database deployment: %v", l, err)
		}
	}
}

func insertTransaction(ctx context.Context, tx pgx.Tx, merchant, shardKey string) error {
	_, err := insertTransactionReturningID(ctx, tx, merchant, shardKey)
	return err
}

func insertTransactionReturningID(ctx context.Context, tx pgx.Tx, merchant, shardKey string) (uuid.UUID, error) {
	id := uuid.New()
	const query = `
		INSERT INTO payment_transactions (id, merchant_id, shard_key, idempotency_key, amount_minor, currency)
		VALUES ($1, $2, $3, $4, 1000, 'USD')`

	_, err := tx.Exec(ctx, query, id, merchant, shardKey, "idem-"+id.String())
	return id, err
}
