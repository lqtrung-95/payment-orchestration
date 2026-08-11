package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

// ReplayEntry is what one stored event would do if it arrived again now.
type ReplayEntry struct {
	RawEventID      int64
	ProviderEventID string
	Reference       string
	Sequence        int64
	RecordedOutcome Outcome
	ReplayOutcome   Outcome
	Note            string
}

// Changed reports whether replaying this event would alter the current state.
func (e ReplayEntry) Changed() bool { return e.ReplayOutcome == OutcomeApplied }

// ReplayReport is the result of re-running the whole log against current state.
type ReplayReport struct {
	Entries []ReplayEntry
	Changed int
}

// Replay re-evaluates every stored event against current state and reports what
// would happen, writing nothing.
//
// This is the property worth asserting, and it is narrower than "reprocessing
// the log rebuilds the world from empty". It could not be that: transactions are
// created by API calls and moved by provider responses, neither of which is in
// this log, so an empty database has nothing for these events to correlate
// against. What *is* both true and useful is that the log is convergent —
// replaying it changes nothing, in any order, however many times. An event log
// that quietly re-applies itself is worse than no log, because it invites
// exactly the recovery procedure that corrupts state.
//
// Every event is evaluated inside one transaction that is rolled back, so the
// evaluation sees a consistent snapshot and leaves no trace.
func (p *Processor) Replay(ctx context.Context, q postgres.Querier) (*ReplayReport, error) {
	events, err := p.events.ListForReplay(ctx, q)
	if err != nil {
		return nil, err
	}

	report := &ReplayReport{Entries: make([]ReplayEntry, 0, len(events))}

	err = p.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		for _, raw := range events {
			entry := ReplayEntry{
				RawEventID:      raw.ID,
				ProviderEventID: raw.ProviderEventID,
				Reference:       raw.Reference,
				Sequence:        raw.Sequence,
				RecordedOutcome: raw.Status,
			}

			d, err := p.evaluate(ctx, tx, raw)
			switch {
			case errors.Is(err, ErrNotCorrelated):
				entry.ReplayOutcome = OutcomeUnmatched
				entry.Note = "no transaction matches the reference"
			case err != nil:
				return fmt.Errorf("replay event %d: %w", raw.ID, err)
			default:
				entry.ReplayOutcome = d.outcome
				entry.Note = d.note
			}

			if entry.Changed() {
				report.Changed++
			}
			report.Entries = append(report.Entries, entry)
		}

		// Nothing is written. The row locks evaluate() takes are released by the
		// rollback, and the caller gets an answer rather than a mutation.
		return errRollbackAfterReplay
	})
	if err != nil && !errors.Is(err, errRollbackAfterReplay) {
		return nil, err
	}

	return report, nil
}

// errRollbackAfterReplay unwinds the evaluation transaction. Returning an error
// is how WithTx is told to roll back, and a sentinel keeps that intent explicit
// rather than leaving a reader wondering what failed.
var errRollbackAfterReplay = errors.New("replay complete, rolling back")
