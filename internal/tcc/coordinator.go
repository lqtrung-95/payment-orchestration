package tcc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
)

// Config tunes the coordinator.
type Config struct {
	// HoldTTL is how long a reservation stays valid before the sweeper releases
	// it. It bounds how long a merchant's funds can be frozen by a coordinator
	// that died mid-transfer, so it is a money-safety control rather than a
	// performance knob.
	HoldTTL time.Duration

	// ConfirmGrace is how far the deadline is pushed out when a sweeper picks a
	// transfer up, so a slow round of confirms is not re-swept underneath
	// itself while it is still running.
	ConfirmGrace time.Duration
}

func DefaultConfig() Config {
	return Config{HoldTTL: 30 * time.Second, ConfirmGrace: 30 * time.Second}
}

// Coordinator drives a transfer through try, confirm, and cancel.
//
// It holds no state of its own. Everything it needs is in tcc_transfers, which
// is why any instance can pick up a transfer whose original coordinator died —
// the sweeper does exactly that, and it is the same code path.
type Coordinator struct {
	router *postgres.Router
	store  *Store
	part   *Participant
	cfg    Config
	logger *slog.Logger
}

func NewCoordinator(router *postgres.Router, cfg Config, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		router: router, store: NewStore(), part: NewParticipant(),
		cfg: cfg, logger: logger,
	}
}

// TransferInput asks for money to move between two merchants.
type TransferInput struct {
	SourceMerchant string
	DestMerchant   string
	Amount         money.Money

	// IdempotencyKey identifies the intent. Submitting it twice returns the
	// same transfer rather than moving the money again.
	IdempotencyKey string
}

// Transfer moves funds, coordinating across whichever databases the two
// merchants live on.
func (c *Coordinator) Transfer(ctx context.Context, in TransferInput) (*Transfer, error) {
	if in.SourceMerchant == in.DestMerchant {
		return nil, ErrSameMerchant
	}
	if err := in.Amount.Currency().Validate(); err != nil {
		return nil, err
	}
	if !in.Amount.IsPositive() {
		return nil, fmt.Errorf("transfer amount must be positive, got %s", in.Amount)
	}

	t := &Transfer{
		ID:             uuid.New(),
		State:          StateTrying,
		SourceMerchant: in.SourceMerchant,
		SourceShardKey: sharding.KeyForMerchant(in.SourceMerchant),
		DestMerchant:   in.DestMerchant,
		DestShardKey:   sharding.KeyForMerchant(in.DestMerchant),
		Amount:         in.Amount,
		IdempotencyKey: in.IdempotencyKey,
		TimeoutAt:      time.Now().Add(c.cfg.HoldTTL).UTC(),
	}

	// The coordinator record is committed before any participant is contacted.
	// A reservation whose transfer was never recorded is a hold nothing knows
	// how to release.
	created, err := c.store.Create(ctx, c.router.Global().Pool(), t)
	if err != nil {
		return nil, err
	}

	return c.Resume(ctx, created)
}

// Resume drives a transfer from wherever it currently is.
//
// Used both for a fresh transfer and by the sweeper for one whose coordinator
// disappeared. There is deliberately one implementation: a separate recovery
// path is a path that only runs during incidents, which is when it is least
// affordable for it to be the untested one.
func (c *Coordinator) Resume(ctx context.Context, t *Transfer) (*Transfer, error) {
	switch t.State {
	case StateTrying:
		if err := c.try(ctx, t); err != nil {
			// Nothing has been posted, so the whole transfer is abandoned and
			// every hold taken so far released.
			if cancelErr := c.cancel(ctx, t); cancelErr != nil {
				c.logger.ErrorContext(ctx, "failed to release holds after a failed try",
					slog.String("transfer_id", t.ID.String()), slog.Any("error", cancelErr))
			}
			return t, err
		}

		// The commit point. Every participant has agreed, so from here the
		// transfer will complete: a failing confirm is retried, never turned
		// into a cancel. Recording it before any confirm runs is what lets a
		// coordinator that dies immediately afterwards be resumed correctly.
		moved, err := c.store.Advance(ctx, c.router.Global().Pool(), t.ID, StateTrying, StateConfirming)
		if err != nil {
			return t, err
		}
		if !moved {
			// Someone else advanced it — most likely a sweeper that decided the
			// deadline had passed. Act on what the record now says.
			return c.reload(ctx, t.ID)
		}
		t.State = StateConfirming
		return c.confirm(ctx, t)

	case StateConfirming:
		return c.confirm(ctx, t)

	case StateCancelling:
		return t, c.cancel(ctx, t)

	default:
		return t, nil
	}
}

// try takes a hold on each side. Either side may refuse; the source refuses
// when the funds are not available, which is the only refusal with a business
// meaning.
func (c *Coordinator) try(ctx context.Context, t *Transfer) error {
	for _, side := range []struct {
		role     Role
		shardKey string
	}{
		{RoleSource, t.SourceShardKey},
		{RoleDestination, t.DestShardKey},
	} {
		err := c.router.WithTx(ctx, side.shardKey, func(ctx context.Context, tx pgx.Tx) error {
			_, err := c.part.Try(ctx, tx, t, side.role)
			return err
		})
		if err != nil {
			_ = c.store.RecordAttempt(ctx, c.router.Global().Pool(), t.ID, err)
			return fmt.Errorf("try %s side of transfer %s: %w", side.role, t.ID, err)
		}
	}
	return nil
}

// confirm posts both halves and closes the transfer.
//
// Between the two confirms the money is genuinely in flight: one shard's
// suspense account is credited and the other's is not yet debited, so the
// system-wide suspense position is briefly non-zero. That window is the honest
// representation of a distributed transfer, and it closes on the second commit
// or on the sweeper's retry.
func (c *Coordinator) confirm(ctx context.Context, t *Transfer) (*Transfer, error) {
	for _, side := range []struct {
		role     Role
		shardKey string
	}{
		{RoleSource, t.SourceShardKey},
		{RoleDestination, t.DestShardKey},
	} {
		err := c.router.WithTx(ctx, side.shardKey, func(ctx context.Context, tx pgx.Tx) error {
			_, err := c.part.Confirm(ctx, tx, t, side.role)
			return err
		})
		if err != nil {
			_ = c.store.RecordAttempt(ctx, c.router.Global().Pool(), t.ID, err)
			// Left in confirming on purpose. The sweeper will try again; there
			// is no state that would let anything conclude the transfer failed.
			return t, fmt.Errorf("confirm %s side of transfer %s: %w", side.role, t.ID, err)
		}
	}

	if _, err := c.store.Advance(ctx, c.router.Global().Pool(), t.ID, StateConfirming, StateConfirmed); err != nil {
		return t, err
	}
	t.State = StateConfirmed

	c.logger.InfoContext(ctx, "cross-shard transfer confirmed",
		slog.String("transfer_id", t.ID.String()),
		slog.String("amount", t.Amount.String()),
		slog.Bool("cross_shard", t.CrossShard()))

	return t, nil
}

// cancel releases every hold and closes the transfer.
func (c *Coordinator) cancel(ctx context.Context, t *Transfer) error {
	if t.State == StateTrying {
		moved, err := c.store.Advance(ctx, c.router.Global().Pool(), t.ID, StateTrying, StateCancelling)
		if err != nil {
			return err
		}
		if !moved {
			// Another instance moved it first. If it committed, cancelling now
			// would release a hold the other side is owed.
			current, err := c.reload(ctx, t.ID)
			if err != nil {
				return err
			}
			if current.State.PastCommitPoint() {
				return fmt.Errorf("%w: transfer %s is committed", ErrAlreadyResolved, t.ID)
			}
		}
		t.State = StateCancelling
	}

	for _, side := range []struct {
		role     Role
		shardKey string
	}{
		{RoleSource, t.SourceShardKey},
		{RoleDestination, t.DestShardKey},
	} {
		err := c.router.WithTx(ctx, side.shardKey, func(ctx context.Context, tx pgx.Tx) error {
			return c.part.Cancel(ctx, tx, t.ID, side.role)
		})
		if err != nil {
			return fmt.Errorf("cancel %s side of transfer %s: %w", side.role, t.ID, err)
		}
	}

	if _, err := c.store.Advance(ctx, c.router.Global().Pool(), t.ID, StateCancelling, StateCancelled); err != nil {
		return err
	}
	t.State = StateCancelled
	return nil
}

func (c *Coordinator) reload(ctx context.Context, id uuid.UUID) (*Transfer, error) {
	return c.store.Get(ctx, c.router.Global().Pool(), id)
}

// Get returns a transfer's current state.
func (c *Coordinator) Get(ctx context.Context, id uuid.UUID) (*Transfer, error) {
	return c.reload(ctx, id)
}
