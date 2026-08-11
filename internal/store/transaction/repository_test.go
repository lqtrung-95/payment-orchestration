package transaction_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

func newTransaction(t *testing.T, merchant string, amount int64) *domain.Transaction {
	t.Helper()
	tx, err := domain.New(merchant, "key-"+merchant+"-"+fmt.Sprint(amount), money.MustNew(amount, "USD"))
	if err != nil {
		t.Fatalf("domain.New returned error: %v", err)
	}
	return tx
}

// The transition matrix exists twice: as a map in Go and as rows in Postgres.
// Two encodings of one rule drift apart unless something compares them, and a
// drift here means the application believes a transition is legal that the
// database will reject at runtime — or worse, the reverse.
func TestTransitionMatrixMatchesDatabase(t *testing.T) {
	db := testsupport.NewDB(t)
	ctx := context.Background()

	rows, err := db.Pool().Query(ctx,
		`SELECT from_state, to_state FROM transaction_state_transitions ORDER BY from_state, to_state`)
	if err != nil {
		t.Fatalf("query transitions: %v", err)
	}
	defer rows.Close()

	var fromDB []string
	for rows.Next() {
		var from, to string
		if err := rows.Scan(&from, &to); err != nil {
			t.Fatalf("scan transition: %v", err)
		}
		fromDB = append(fromDB, from+" -> "+to)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate transitions: %v", err)
	}

	var fromGo []string
	for _, tr := range domain.Transitions() {
		fromGo = append(fromGo, string(tr.From)+" -> "+string(tr.To))
	}
	sort.Strings(fromDB)
	sort.Strings(fromGo)

	if len(fromDB) != len(fromGo) {
		t.Fatalf("transition count differs: database has %d, Go has %d\ndatabase: %v\nGo:       %v",
			len(fromDB), len(fromGo), fromDB, fromGo)
	}
	for i := range fromGo {
		if fromDB[i] != fromGo[i] {
			t.Errorf("transition %d differs: database %q, Go %q", i, fromDB[i], fromGo[i])
		}
	}
}

func TestInsertAndGetRoundTrip(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()
	ctx := context.Background()

	original := newTransaction(t, "m_roundtrip", 12345)
	if err := repo.Insert(ctx, db.Pool(), original); err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}

	loaded, err := repo.Get(ctx, db.Pool(), original.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if loaded.MerchantID != original.MerchantID {
		t.Errorf("merchant = %q, want %q", loaded.MerchantID, original.MerchantID)
	}
	if !loaded.Amount.Equal(original.Amount) {
		t.Errorf("amount = %s, want %s", loaded.Amount, original.Amount)
	}
	if loaded.State != domain.StateCreated {
		t.Errorf("state = %s, want created", loaded.State)
	}
	if loaded.ShardKey != original.ShardKey {
		t.Errorf("shard key = %q, want %q", loaded.ShardKey, original.ShardKey)
	}
}

func TestGetReturnsNotFoundForMissingRow(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()

	_, err := repo.Get(context.Background(), db.Pool(), newTransaction(t, "m_absent", 1).ID)
	if !errors.Is(err, txstore.ErrNotFound) {
		t.Errorf("Get on a missing row error = %v, want ErrNotFound", err)
	}
}

// Two writers read the same version and both try to update. Exactly one must
// win — this is what stops a double capture from being recorded twice.
func TestUpdateDetectsConcurrentModification(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()
	ctx := context.Background()

	tx := newTransaction(t, "m_optimistic", 5000)
	if err := repo.Insert(ctx, db.Pool(), tx); err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}

	first, err := repo.Get(ctx, db.Pool(), tx.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	second, err := repo.Get(ctx, db.Pool(), tx.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if err := first.TransitionTo(domain.StateAuthorizing); err != nil {
		t.Fatalf("TransitionTo returned error: %v", err)
	}
	if err := repo.Update(ctx, db.Pool(), first); err != nil {
		t.Fatalf("first Update returned error: %v", err)
	}

	if err := second.TransitionTo(domain.StateCancelled); err != nil {
		t.Fatalf("TransitionTo returned error: %v", err)
	}
	if err := repo.Update(ctx, db.Pool(), second); !errors.Is(err, txstore.ErrVersionConflict) {
		t.Fatalf("stale Update error = %v, want ErrVersionConflict", err)
	}

	current, err := repo.Get(ctx, db.Pool(), tx.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if current.State != domain.StateAuthorizing {
		t.Errorf("state = %s, want authorizing (the winning write)", current.State)
	}
	if current.Version != 2 {
		t.Errorf("version = %d, want 2", current.Version)
	}
}

// The database rejects an illegal transition even when the aggregate is
// bypassed entirely, which is the point of enforcing it in both places.
func TestDatabaseRejectsIllegalTransition(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()
	ctx := context.Background()

	tx := newTransaction(t, "m_illegal", 5000)
	if err := repo.Insert(ctx, db.Pool(), tx); err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}

	_, err := db.Pool().Exec(ctx,
		`UPDATE payment_transactions SET state = 'settled' WHERE id = $1`, tx.ID)
	if err == nil {
		t.Fatal("database accepted created -> settled, want rejection")
	}
}

// Under concurrent updates the version column must advance exactly once per
// successful write, with the failures reporting a conflict rather than being
// lost silently.
func TestConcurrentUpdatesSerializeOnVersion(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()
	ctx := context.Background()

	tx := newTransaction(t, "m_concurrent", 5000)
	if err := repo.Insert(ctx, db.Pool(), tx); err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}

	const writers = 20
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		conflicts int
	)

	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			loaded, err := repo.Get(ctx, db.Pool(), tx.ID)
			if err != nil {
				return
			}
			if err := loaded.TransitionTo(domain.StateAuthorizing); err != nil {
				return
			}

			err = repo.Update(ctx, db.Pool(), loaded)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, txstore.ErrVersionConflict):
				conflicts++
			default:
				t.Errorf("unexpected update error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if succeeded+conflicts != writers {
		t.Errorf("accounted for %d of %d writers", succeeded+conflicts, writers)
	}

	final, err := repo.Get(ctx, db.Pool(), tx.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	// Every accepted write bumps the version exactly once, so the final version
	// is the initial value plus the number of winners. Any other value means a
	// write landed without being counted.
	if want := 1 + succeeded; final.Version != want {
		t.Errorf("version = %d, want %d (1 + %d successful writes)", final.Version, want, succeeded)
	}
}

func TestRecordStateChangeIsAppendOnly(t *testing.T) {
	db := testsupport.FreshDB(t)
	repo := txstore.NewRepository()
	ctx := context.Background()

	tx := newTransaction(t, "m_audit", 900)

	err := db.WithTx(ctx, func(ctx context.Context, q pgx.Tx) error {
		if err := repo.Insert(ctx, q, tx); err != nil {
			return err
		}
		return repo.RecordStateChange(ctx, q, txstore.StateChange{
			TransactionID: tx.ID,
			To:            domain.StateCreated,
			Reason:        "payment created",
			Actor:         "test",
			SourceIP:      "203.0.113.7",
		})
	})
	if err != nil {
		t.Fatalf("WithTx returned error: %v", err)
	}

	var count int
	if err := db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM transaction_state_changes WHERE transaction_id = $1`, tx.ID,
	).Scan(&count); err != nil {
		t.Fatalf("count state changes: %v", err)
	}
	if count != 1 {
		t.Fatalf("state change count = %d, want 1", count)
	}

	// History that can be rewritten is not an audit trail.
	if _, err := db.Pool().Exec(ctx, `UPDATE transaction_state_changes SET actor = 'tampered'`); err == nil {
		t.Error("database accepted an UPDATE on the audit trail, want rejection")
	}
	if _, err := db.Pool().Exec(ctx, `DELETE FROM transaction_state_changes`); err == nil {
		t.Error("database accepted a DELETE on the audit trail, want rejection")
	}
}
