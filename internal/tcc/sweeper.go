package tcc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Sweeper finishes transfers whose coordinator stopped.
//
// This is the component that makes the protocol survive a crash. A reservation
// is unspendable funds; without something that eventually resolves it, a
// coordinator dying between try and confirm freezes a merchant's balance with
// no expiry and no owner. The sweeper is that something, and its failure is
// therefore a money-safety failure rather than a background-job failure — it is
// logged at error level every pass, not once.
//
// What it does depends entirely on which side of the commit point the transfer
// is on. Before it, nothing has been posted and cancelling is free. After it,
// every participant has already agreed, so the only correct action is to keep
// confirming until it works.
type Sweeper struct {
	coordinator *Coordinator
	batchSize   int
	logger      *slog.Logger
}

func NewSweeper(coordinator *Coordinator, batchSize int, logger *slog.Logger) *Sweeper {
	return &Sweeper{coordinator: coordinator, batchSize: batchSize, logger: logger}
}

// Sweep resolves one batch of overdue transfers and reports how many it
// finished.
func (s *Sweeper) Sweep(ctx context.Context) (int, error) {
	claimed, err := s.claim(ctx)
	if err != nil {
		return 0, err
	}

	resolved := 0
	for _, t := range claimed {
		if err := s.resolve(ctx, t); err != nil {
			s.logger.ErrorContext(ctx, "failed to resolve a stranded transfer",
				slog.String("transfer_id", t.ID.String()),
				slog.String("state", string(t.State)),
				slog.Any("error", err))
			continue
		}
		resolved++
	}
	return resolved, nil
}

// claim takes a batch and pushes each deadline out, in one transaction.
//
// Extending inside the claim is what stops two sweepers, or the same sweeper on
// its next pass, from working the same transfer while the first attempt is
// still contacting participants. The row lock ends at commit; the extended
// deadline is what carries the exclusion beyond it.
func (s *Sweeper) claim(ctx context.Context) ([]*Transfer, error) {
	var claimed []*Transfer

	err := s.coordinator.router.Global().WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		overdue, err := s.coordinator.store.Unresolved(ctx, tx, s.batchSize)
		if err != nil {
			return err
		}
		for _, t := range overdue {
			if err := s.coordinator.store.ExtendDeadline(ctx, tx, t.ID, s.coordinator.cfg.ConfirmGrace); err != nil {
				return err
			}
		}
		claimed = overdue
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("claim stranded transfers: %w", err)
	}
	return claimed, nil
}

func (s *Sweeper) resolve(ctx context.Context, t *Transfer) error {
	switch {
	case t.State.PastCommitPoint():
		// Owed completion. Confirm is idempotent, so repeating it against a
		// side that already posted is a no-op.
		s.logger.WarnContext(ctx, "resuming a committed transfer",
			slog.String("transfer_id", t.ID.String()),
			slog.Int("previous_attempts", t.Attempts))

		_, err := s.coordinator.Resume(ctx, t)
		return err

	case t.State == StateTrying || t.State == StateCancelling:
		// Nothing was posted, so the holds are released and the funds become
		// spendable again.
		s.logger.WarnContext(ctx, "cancelling a stranded transfer",
			slog.String("transfer_id", t.ID.String()),
			slog.String("state", string(t.State)),
			slog.Int("previous_attempts", t.Attempts))

		return s.coordinator.cancel(ctx, t)

	default:
		return nil
	}
}

// Run sweeps on an interval until the context is cancelled.
func (s *Sweeper) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	s.logger.InfoContext(ctx, "transfer sweeper started", slog.Duration("interval", interval))

	for {
		select {
		case <-ctx.Done():
			s.logger.InfoContext(ctx, "transfer sweeper stopped")
			return nil

		case <-ticker.C:
			resolved, err := s.Sweep(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				s.logger.ErrorContext(ctx, "transfer sweep failed", slog.Any("error", err))
				continue
			}
			if resolved > 0 {
				s.logger.InfoContext(ctx, "resolved stranded transfers", slog.Int("count", resolved))
			}
		}
	}
}
