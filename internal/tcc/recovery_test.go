package tcc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
	"github.com/lequoctrung/payment-orchestrator/internal/tcc"
)

// A coordinator that dies after taking holds but before committing leaves funds
// frozen. The sweeper is what unfreezes them, and this is the test that says so.
//
// The crash is simulated by doing exactly what the coordinator does and then
// stopping — the transfer is recorded, the holds are taken, and nothing
// advances the state. That is precisely the durable state a killed process
// leaves behind.
func TestSweeperReleasesHoldsStrandedBeforeTheCommitPoint(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "stranded-src")
	dest := merchantOnShard(t, f, 1, "stranded-dst")
	fund(t, f, source, 1_000)

	stranded := strandTransfer(t, f, source, dest, 800, "stranded-try", tcc.StateTrying, false)

	// While the hold stands, the money is not spendable — that is what a
	// reservation is for, and it is why leaving one behind matters.
	if _, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source, DestMerchant: dest,
		Amount: money.MustNew(500, "USD"), IdempotencyKey: "blocked-by-hold",
	}); !errors.Is(err, tcc.ErrInsufficientFunds) {
		t.Fatalf("spending against a held balance: got %v, want ErrInsufficientFunds", err)
	}

	resolved, err := f.sweeper.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if resolved != 1 {
		t.Fatalf("sweeper resolved %d transfers, want 1", resolved)
	}

	after, err := f.coordinator.Get(ctx, stranded.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != tcc.StateCancelled {
		t.Errorf("state = %s, want cancelled", after.State)
	}
	if got := outstandingHolds(t, f); got != 0 {
		t.Errorf("%d still held after the sweep", got)
	}
	if got := payableBalance(t, f, source); got != 1_000 {
		t.Errorf("source payable = %d, want 1000 — a cancelled transfer moved money", got)
	}

	// The funds are spendable again, which is the outcome that matters to the
	// merchant. Asserting only the reservation's state would pass even if the
	// available-balance query still counted it.
	if _, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source, DestMerchant: dest,
		Amount: money.MustNew(900, "USD"), IdempotencyKey: "after-release",
	}); err != nil {
		t.Errorf("spending after the hold was released: %v", err)
	}
}

// Past the commit point the answer is the opposite: every participant has
// agreed, so the transfer is owed completion and the sweeper must finish it
// rather than roll it back.
//
// This is the state a crash between the two confirms leaves behind — one shard
// posted, the other not, and the system-wide suspense position non-zero.
func TestSweeperCompletesTransfersStrandedAfterTheCommitPoint(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "committed-src")
	dest := merchantOnShard(t, f, 1, "committed-dst")
	fund(t, f, source, 3_000)

	stranded := strandTransfer(t, f, source, dest, 1_200, "stranded-confirm", tcc.StateConfirming, true)

	// Half-applied: the source paid into suspense and the destination has not
	// drawn it out. The money exists and is owed to nobody.
	if got := totalByPurpose(t, f, ledgerdomain.PurposeTransferSuspense); got != 1_200 {
		t.Fatalf("suspense = %d, want 1200 — the crash was not simulated as intended", got)
	}
	if got := payableBalance(t, f, dest); got != 0 {
		t.Fatalf("destination payable = %d, want 0 before the sweep", got)
	}

	if _, err := f.sweeper.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	after, err := f.coordinator.Get(ctx, stranded.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != tcc.StateConfirmed {
		t.Errorf("state = %s, want confirmed — a committed transfer was not completed", after.State)
	}
	if got := payableBalance(t, f, dest); got != 1_200 {
		t.Errorf("destination payable = %d, want 1200", got)
	}
	if got := payableBalance(t, f, source); got != 1_800 {
		t.Errorf("source payable = %d, want 1800", got)
	}
	if got := totalByPurpose(t, f, ledgerdomain.PurposeTransferSuspense); got != 0 {
		t.Errorf("suspense across shards = %d, want 0", got)
	}
	if got := outstandingHolds(t, f); got != 0 {
		t.Errorf("%d still held after completion", got)
	}
}

// Sweeping twice must not post twice. The sweeper has no way to know whether a
// previous pass got through, so every step it takes has to be repeatable.
func TestSweepingACompletedTransferAgainChangesNothing(t *testing.T) {
	f := newFixture(t, tcc.DefaultConfig())
	ctx := context.Background()

	source := merchantOnShard(t, f, 0, "resweep-src")
	dest := merchantOnShard(t, f, 1, "resweep-dst")
	fund(t, f, source, 2_000)

	transfer, err := f.coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: source, DestMerchant: dest,
		Amount: money.MustNew(700, "USD"), IdempotencyKey: "resweep",
	})
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// Force it back into the sweeper's view, as if a coordinator had recorded
	// the confirms but died before marking the transfer done.
	setState(t, f, transfer.ID, tcc.StateConfirming)
	expireDeadline(t, f, transfer.ID)

	for i := 0; i < 3; i++ {
		if _, err := f.sweeper.Sweep(ctx); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}

	if got := payableBalance(t, f, dest); got != 700 {
		t.Errorf("destination payable = %d, want 700 — repeated sweeps posted again", got)
	}
	if got := countEntriesFor(t, f, transfer.ID); got != 2 {
		t.Errorf("%d journal entries for the transfer, want 2", got)
	}
	if got := totalByPurpose(t, f, ledgerdomain.PurposeTransferSuspense); got != 0 {
		t.Errorf("suspense = %d, want 0", got)
	}
}

// strandTransfer builds the durable state a killed coordinator leaves behind:
// a recorded transfer, holds on both sides, an expired deadline, and — when
// asked — the source half already posted.
func strandTransfer(
	t *testing.T,
	f *fixture,
	source, dest string,
	minor int64,
	key string,
	state tcc.State,
	confirmSource bool,
) *tcc.Transfer {
	t.Helper()

	ctx := context.Background()
	store := tcc.NewStore()

	transfer := &tcc.Transfer{
		ID:             uuid.New(),
		State:          tcc.StateTrying,
		SourceMerchant: source,
		SourceShardKey: sharding.KeyForMerchant(source),
		DestMerchant:   dest,
		DestShardKey:   sharding.KeyForMerchant(dest),
		Amount:         money.MustNew(minor, "USD"),
		IdempotencyKey: key,
		// Already overdue, so the sweeper's first pass picks it up.
		TimeoutAt: time.Now().Add(-time.Minute).UTC(),
	}

	created, err := store.Create(ctx, f.router.Global().Pool(), transfer)
	if err != nil {
		t.Fatalf("record stranded transfer: %v", err)
	}

	for _, side := range []struct {
		role     tcc.Role
		shardKey string
	}{
		{tcc.RoleSource, created.SourceShardKey},
		{tcc.RoleDestination, created.DestShardKey},
	} {
		err := f.router.WithTx(ctx, side.shardKey, func(ctx context.Context, tx pgx.Tx) error {
			_, err := f.participant.Try(ctx, tx, created, side.role)
			return err
		})
		if err != nil {
			t.Fatalf("take %s hold: %v", side.role, err)
		}
	}

	if state != tcc.StateTrying {
		setState(t, f, created.ID, state)
		created.State = state
	}

	if confirmSource {
		err := f.router.WithTx(ctx, created.SourceShardKey, func(ctx context.Context, tx pgx.Tx) error {
			_, err := f.participant.Confirm(ctx, tx, created, tcc.RoleSource)
			return err
		})
		if err != nil {
			t.Fatalf("confirm source half: %v", err)
		}
	}

	return created
}

func setState(t *testing.T, f *fixture, id uuid.UUID, state tcc.State) {
	t.Helper()

	_, err := f.router.Global().Pool().Exec(context.Background(),
		`UPDATE tcc_transfers SET state = $2::tcc_state, resolved_at = NULL WHERE id = $1`,
		id, string(state))
	if err != nil {
		t.Fatalf("set transfer state: %v", err)
	}
}

func expireDeadline(t *testing.T, f *fixture, id uuid.UUID) {
	t.Helper()

	_, err := f.router.Global().Pool().Exec(context.Background(),
		`UPDATE tcc_transfers SET timeout_at = now() - interval '1 minute' WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("expire transfer deadline: %v", err)
	}
}
