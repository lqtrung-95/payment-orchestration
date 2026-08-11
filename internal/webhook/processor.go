package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

// ErrNotCorrelated means no transaction matches the event's charge reference.
//
// Almost always transient: the provider's callback and its HTTP reply race, so
// a webhook regularly arrives before this service has recorded the reference the
// same call was about to return. The event is left alone and retried.
//
// What must never happen is creating a transaction from a webhook. A payment
// this service has no record of asking for is not a payment; inventing one from
// a callback is how phantom transactions appear in a ledger.
var ErrNotCorrelated = errors.New("no transaction matches the provider reference")

// Processor applies provider events to transactions.
type Processor struct {
	db       *postgres.DB
	registry *Registry
	events   *Repository
	txRepo   *txstore.Repository
	logger   *slog.Logger
}

func NewProcessor(
	db *postgres.DB,
	registry *Registry,
	events *Repository,
	txRepo *txstore.Repository,
	logger *slog.Logger,
) *Processor {
	return &Processor{db: db, registry: registry, events: events, txRepo: txRepo, logger: logger}
}

// decision is what the guards conclude about an event, before anything is
// written. Separated from the write so replay can ask the same question without
// touching the database.
type decision struct {
	outcome     Outcome
	target      domain.State
	note        string
	transaction *domain.Transaction
}

// Process applies one stored event.
func (p *Processor) Process(ctx context.Context, rawEventID int64) (Outcome, error) {
	var outcome Outcome

	err := p.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// Locked for the whole decision. Two deliveries of events for one charge
		// can be processed concurrently, and without the lock both would read
		// the same high-water mark and both conclude they are the newer event.
		raw, err := p.events.GetForUpdate(ctx, tx, rawEventID)
		if err != nil {
			return err
		}

		// Already decided. A redelivery that slipped past the unique index and
		// the consumer's deduplication still stops here.
		if raw.Status != OutcomeReceived {
			outcome = raw.Status
			return nil
		}

		d, err := p.evaluate(ctx, tx, *raw)
		if err != nil {
			return err
		}
		outcome = d.outcome

		return p.commit(ctx, tx, *raw, d)
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// evaluate runs every guard and returns what should happen, writing nothing.
func (p *Processor) evaluate(ctx context.Context, q postgres.Querier, raw RawEvent) (decision, error) {
	verifier, err := p.registry.Get(raw.Provider)
	if err != nil {
		return decision{}, err
	}

	// Re-parsed from the stored bytes rather than from a stored interpretation.
	// Keeping raw payloads is only worth anything if the mapping can be
	// corrected later and the log re-read through the corrected one.
	event, err := raw.Normalized(verifier)
	if err != nil {
		return decision{}, err
	}

	t, err := p.txRepo.GetByProviderReferenceForUpdate(ctx, q, raw.Reference)
	if errors.Is(err, txstore.ErrNotFound) {
		return decision{}, ErrNotCorrelated
	}
	if err != nil {
		return decision{}, err
	}

	// Staleness is judged by the provider's own ordering token, not by arrival
	// order. An event at or below the high-water mark is stale by its own
	// account, whenever it happens to turn up.
	if raw.Sequence <= t.LastAppliedEventSeq {
		return decision{
			outcome:     OutcomeSuperseded,
			transaction: t,
			note: fmt.Sprintf("sequence %d not newer than applied %d",
				raw.Sequence, t.LastAppliedEventSeq),
		}, nil
	}

	if statusImpliesNoChange(event.Status) {
		return decision{
			outcome:     OutcomeIgnored,
			transaction: t,
			note:        "status " + event.RawStatus + " implies no state change",
		}, nil
	}

	target, ok := stateFor(event.Status)
	if !ok {
		return decision{
			outcome:     OutcomeIgnored,
			transaction: t,
			note:        "no transition defined for status " + event.RawStatus,
		}, nil
	}

	// Already there. The event is a confirmation, not a change — routinely so,
	// because the recovery path can establish the same outcome by asking the
	// provider before the callback arrives.
	//
	// The state machine permits a same-state move, so this would otherwise be
	// recorded as a transition, and an audit trail that shows a payment being
	// authorized twice is worse than useless during an incident: it invites the
	// reader to go looking for the second authorization.
	if t.State == target {
		return decision{
			outcome:     OutcomeIgnored,
			transaction: t,
			note:        "already " + string(target) + "; event confirms rather than changes",
		}, nil
	}

	// Refused by the state machine rather than by an ad-hoc check. A late
	// authorization event arriving after a capture is rejected because that edge
	// does not exist, which is a structural answer rather than a special case
	// somebody remembered to write.
	if !t.State.CanTransitionTo(target) {
		return decision{
			outcome:     OutcomeRejected,
			transaction: t,
			note:        fmt.Sprintf("illegal transition %s -> %s", t.State, target),
		}, nil
	}

	return decision{outcome: OutcomeApplied, target: target, transaction: t}, nil
}

// commit writes the decision. The state change, the high-water mark, the audit
// row, and the event's outcome all land in the caller's transaction, so the raw
// log can never claim an event was applied to a change that rolled back.
func (p *Processor) commit(ctx context.Context, tx pgx.Tx, raw RawEvent, d decision) error {
	t := d.transaction

	if d.outcome != OutcomeApplied {
		// Not applied, and deliberately not dropped either. "We received this and
		// chose not to act on it" is the answer an investigation needs.
		p.logger.InfoContext(ctx, "webhook event not applied",
			slog.Int64("raw_event_id", raw.ID),
			slog.String("outcome", string(d.outcome)),
			slog.String("reason", d.note))

		if d.outcome == OutcomeIgnored {
			// Understood and current, so it does move the mark: an older event
			// arriving later is stale relative to this one too.
			t.LastAppliedEventSeq = raw.Sequence
			if err := p.txRepo.Update(ctx, tx, t); err != nil {
				return err
			}
		}
		return p.events.Resolve(ctx, tx, raw.ID, d.outcome, t.ID, d.note)
	}

	from := t.State
	if err := t.TransitionTo(d.target); err != nil {
		return err
	}
	t.LastAppliedEventSeq = raw.Sequence

	if err := p.txRepo.Update(ctx, tx, t); err != nil {
		return err
	}
	if err := p.txRepo.RecordStateChange(ctx, tx, txstore.StateChange{
		TransactionID: t.ID,
		From:          from,
		To:            d.target,
		Reason:        "provider webhook: " + raw.ProviderEventID,
		Actor:         "webhook:" + raw.Provider,
	}); err != nil {
		return err
	}

	p.logger.InfoContext(ctx, "webhook event applied",
		slog.Int64("raw_event_id", raw.ID),
		slog.String("transaction_id", t.ID.String()),
		slog.String("from", string(from)),
		slog.String("to", string(d.target)))

	return p.events.Resolve(ctx, tx, raw.ID, OutcomeApplied, t.ID, "")
}

// MarkUnmatched records that an event never found its transaction.
//
// Reached only after the retry ladder is exhausted. It is a loud outcome rather
// than a quiet one: a provider telling us about a charge we have no record of
// means either a correlation bug or a payment created outside this system, and
// both need a person.
func (p *Processor) MarkUnmatched(ctx context.Context, rawEventID int64, note string) error {
	p.logger.ErrorContext(ctx, "webhook event never correlated to a transaction",
		slog.Int64("raw_event_id", rawEventID), slog.String("reason", note))

	return p.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return p.events.Resolve(ctx, tx, rawEventID, OutcomeUnmatched, uuid.Nil, note)
	})
}

// stateFor maps a normalized provider status onto the transaction state it
// implies, reporting false for statuses this receiver does not act on.
//
// Capture and refund outcomes are deliberately absent: neither operation has a
// surface in this service yet, and applying a capture from a webhook would post
// to the ledger from a path with no amount reconciliation behind it.
func stateFor(s psp.Status) (domain.State, bool) {
	switch s {
	case psp.StatusAuthorized:
		return domain.StateAuthorized, true
	case psp.StatusFailed:
		return domain.StateFailed, true
	case psp.StatusVoided:
		return domain.StateCancelled, true
	default:
		return "", false
	}
}
