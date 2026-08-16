package tcc_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/tcc"
)

func TestCrossShardTransferMovesTheMoney(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "src")
	dest := merchantOnShard(t, f, 1, "dst")
	fund(t, f, source, 10_000)

	transfer, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source,
		DestMerchant:   dest,
		Amount:         money.MustNew(2_500, "USD"),
		IdempotencyKey: "transfer-1",
	})
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	if !transfer.CrossShard() {
		t.Fatal("the two merchants are on one shard; this test is not testing what it claims")
	}
	if transfer.State != tcc.StateConfirmed {
		t.Fatalf("state = %s, want confirmed (last error: %s)", transfer.State, transfer.LastError)
	}

	if got := payableBalance(t, f, source); got != 7_500 {
		t.Errorf("source payable = %d, want 7500", got)
	}
	if got := payableBalance(t, f, dest); got != 2_500 {
		t.Errorf("destination payable = %d, want 2500", got)
	}

	// The suspense account is the seam between the two databases. Each shard's
	// books balance on their own regardless, so this total is the only place a
	// half-applied transfer would show.
	if got := totalByPurpose(t, f, ledgerdomain.PurposeTransferSuspense); got != 0 {
		t.Errorf("suspense across shards = %d, want 0", got)
	}
	if got := outstandingHolds(t, f); got != 0 {
		t.Errorf("%d still held after a completed transfer", got)
	}
}

// Money is conserved: whatever the transfers do, the total owed to merchants
// across every database is exactly what was paid in.
//
// This is the assertion the whole phase exists for. It is checked against the
// sum over both databases, because each one balancing internally says nothing
// about whether a transfer between them lost or duplicated anything.
func TestConcurrentTransfersConserveMoney(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	const (
		merchants   = 8
		startingPot = 10_000
		transfers   = 120
	)

	names := make([]string, 0, merchants)
	for i := 0; i < merchants; i++ {
		// Alternating shards, so most transfers genuinely cross databases.
		name := merchantOnShard(t, f, i%2, fmt.Sprintf("conserve-%d", i))
		fund(t, f, name, startingPot)
		names = append(names, name)
	}

	before := totalByPurpose(t, f, ledgerdomain.PurposePayable)
	if want := int64(merchants * startingPot); before != want {
		t.Fatalf("starting payable total = %d, want %d", before, want)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		confirmed int
		refused   int
	)

	for i := 0; i < transfers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			source := names[i%merchants]
			dest := names[(i+1+i/merchants)%merchants]
			if source == dest {
				return
			}

			result, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
				SourceMerchant: source,
				DestMerchant:   dest,
				Amount:         money.MustNew(int64(100+(i%7)*50), "USD"),
				IdempotencyKey: fmt.Sprintf("conserve-transfer-%d", i),
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && result.State == tcc.StateConfirmed:
				confirmed++
			case errors.Is(err, tcc.ErrInsufficientFunds):
				refused++
			case err != nil:
				t.Errorf("transfer %d failed unexpectedly: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if confirmed == 0 {
		t.Fatal("no transfer completed; the test proved nothing about conservation")
	}
	t.Logf("confirmed=%d refused_for_funds=%d", confirmed, refused)

	after := totalByPurpose(t, f, ledgerdomain.PurposePayable)
	if after != before {
		t.Errorf("payable total changed from %d to %d; %d was created or destroyed",
			before, after, after-before)
	}
	if got := totalByPurpose(t, f, ledgerdomain.PurposeTransferSuspense); got != 0 {
		t.Errorf("suspense across shards = %d, want 0", got)
	}
	if got := outstandingHolds(t, f); got != 0 {
		t.Errorf("%d still held after every transfer resolved", got)
	}

	// No merchant may be overdrawn. Conservation alone would be satisfied by one
	// account going negative while another went up by the same amount.
	for _, name := range names {
		if got := payableBalance(t, f, name); got < 0 {
			t.Errorf("%s is overdrawn at %d", name, got)
		}
	}
}

// Two transfers that each fit the balance but do not fit together: exactly one
// must be refused.
//
// The reservation is what makes this work. A balance check alone passes for
// both, because neither has posted anything by the time the other looks.
func TestConcurrentSpendsCannotExceedTheBalance(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "overdraw-src")
	destA := merchantOnShard(t, f, 1, "overdraw-a")
	destB := merchantOnShard(t, f, 1, "overdraw-b")
	fund(t, f, source, 1_000)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []error
	)

	for i, dest := range []string{destA, destB} {
		wg.Add(1)
		go func(i int, dest string) {
			defer wg.Done()

			_, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
				SourceMerchant: source,
				DestMerchant:   dest,
				Amount:         money.MustNew(600, "USD"),
				IdempotencyKey: fmt.Sprintf("overdraw-%d", i),
			})

			mu.Lock()
			results = append(results, err)
			mu.Unlock()
		}(i, dest)
	}
	wg.Wait()

	var succeeded, refused int
	for _, err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, tcc.ErrInsufficientFunds):
			refused++
		default:
			t.Errorf("unexpected failure: %v", err)
		}
	}

	if succeeded != 1 || refused != 1 {
		t.Errorf("succeeded=%d refused=%d, want exactly one of each", succeeded, refused)
	}
	if got := payableBalance(t, f, source); got != 400 {
		t.Errorf("source payable = %d, want 400 — a second 600 was allowed through", got)
	}
}

// Submitting one intent twice moves the money once.
func TestRepeatedIdempotencyKeyTransfersOnce(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "idem-src")
	dest := merchantOnShard(t, f, 1, "idem-dst")
	fund(t, f, source, 5_000)

	in := tcc.TransferInput{
		SourceMerchant: source,
		DestMerchant:   dest,
		Amount:         money.MustNew(1_000, "USD"),
		IdempotencyKey: "same-intent",
	}

	first, err := f.coordinator.Transfer(ctx, in)
	if err != nil {
		t.Fatalf("first transfer: %v", err)
	}
	second, err := f.coordinator.Transfer(ctx, in)
	if err != nil {
		t.Fatalf("second transfer: %v", err)
	}

	if first.ID != second.ID {
		t.Errorf("two transfers were created: %s and %s", first.ID, second.ID)
	}
	if got := payableBalance(t, f, source); got != 4_000 {
		t.Errorf("source payable = %d, want 4000 — the money moved twice", got)
	}
	if got := countEntriesFor(t, f, first.ID); got != 2 {
		t.Errorf("%d journal entries for the transfer, want 2 (one per side)", got)
	}
}

func TestTransferRejectsUnusableInput(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "reject-src")
	fund(t, f, source, 1_000)

	if _, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source, DestMerchant: source,
		Amount: money.MustNew(100, "USD"), IdempotencyKey: "self",
	}); !errors.Is(err, tcc.ErrSameMerchant) {
		t.Errorf("transfer to self: got %v, want ErrSameMerchant", err)
	}

	dest := merchantOnShard(t, f, 1, "reject-dst")
	if _, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source, DestMerchant: dest,
		Amount: money.MustNew(5_000, "USD"), IdempotencyKey: "too-much",
	}); !errors.Is(err, tcc.ErrInsufficientFunds) {
		t.Errorf("transfer beyond balance: got %v, want ErrInsufficientFunds", err)
	}

	// A refused transfer must leave nothing behind. A hold surviving a failure
	// is funds frozen by an operation that did not happen.
	if got := outstandingHolds(t, f); got != 0 {
		t.Errorf("%d held after refused transfers", got)
	}
	if got := payableBalance(t, f, source); got != 1_000 {
		t.Errorf("source payable = %d, want 1000 unchanged", got)
	}
}
