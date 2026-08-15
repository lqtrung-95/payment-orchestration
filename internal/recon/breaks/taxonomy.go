// Package breaks holds the reconciliation break taxonomy and the workflow that
// carries a break to a decision.
//
// The taxonomy is the point. "The totals do not match" is not a finding — it is
// the absence of one. An operator needs to know whether to chase the provider,
// fix an ingestion gap, wait for the next file, or write the difference off,
// and those are four different categories with four different responses.
package breaks

import (
	"fmt"
	"time"
)

// Category is why a record disagrees.
type Category string

const (
	// MissingAtPSP: we captured it, the provider's file does not mention it.
	// Either it has not settled yet or it was lost at the provider.
	MissingAtPSP Category = "missing_at_psp"

	// MissingInternally: the provider settled something we have no record of.
	// Usually a dropped webhook or an ingestion gap — and always urgent,
	// because it is money moving outside our books.
	MissingInternally Category = "missing_internally"

	// AmountMismatch: both sides have the payment, the amounts differ, and
	// nothing else explains it.
	AmountMismatch Category = "amount_mismatch"

	// FXDrift: the amounts differ and the difference is accounted for by the
	// rate moving between authorisation and settlement. Expected, not a fault.
	FXDrift Category = "fx_drift"

	// FeeMismatch: the net differs by the fee, because the provider applied a
	// schedule we had already superseded.
	FeeMismatch Category = "fee_mismatch"

	// TimingCutoff: present in the adjacent settlement window rather than this
	// one. A boundary artefact, not a missing payment.
	TimingCutoff Category = "timing_cutoff"

	// DuplicateSettlement: the provider settled one charge twice. Money we did
	// not earn, and the one category where doing nothing is itself a problem.
	DuplicateSettlement Category = "duplicate_settlement"

	// CurrencyMismatch: the currencies disagree outright, which is a routing or
	// configuration error rather than an arithmetic one.
	CurrencyMismatch Category = "currency_mismatch"
)

// All lists every category, in the order the classifier considers them.
//
// Order is significant and is not alphabetical. A duplicate settlement also
// looks like an amount mismatch; an FX drift also looks like an amount
// mismatch. The specific explanations have to be tested before the general
// one, or every break lands in the vaguest bucket that happens to fit.
var All = []Category{
	DuplicateSettlement,
	CurrencyMismatch,
	FXDrift,
	FeeMismatch,
	AmountMismatch,
	TimingCutoff,
	MissingInternally,
	MissingAtPSP,
}

func (c Category) Validate() error {
	for _, known := range All {
		if known == c {
			return nil
		}
	}
	return fmt.Errorf("unknown reconciliation break category %q", string(c))
}

// AutoResolvable reports whether a category may be closed without a human.
//
// Only two qualify, and both because the difference is explained rather than
// merely small. Everything else needs a decision: an amount mismatch might be a
// provider error or our own, and guessing on behalf of an operator is how a
// real discrepancy gets closed as noise.
func (c Category) AutoResolvable() bool {
	return c == FXDrift || c == TimingCutoff
}

// Status is where a break sits in its workflow.
type Status string

const (
	StatusOpen          Status = "open"
	StatusInvestigating Status = "investigating"
	StatusResolved      Status = "resolved"
	StatusWrittenOff    Status = "written_off"
)

// IsTerminal reports whether the break has reached a decision.
func (s Status) IsTerminal() bool {
	return s == StatusResolved || s == StatusWrittenOff
}

// allowedTransitions is the workflow, mirrored by a CHECK constraint requiring
// attribution on any terminal status.
var allowedTransitions = map[Status][]Status{
	StatusOpen:          {StatusInvestigating, StatusResolved, StatusWrittenOff},
	StatusInvestigating: {StatusResolved, StatusWrittenOff},
}

// CanTransitionTo reports whether s -> target is permitted.
//
// A resolved break is not reopened. Reopening would rewrite the record of a
// decision somebody made; the correct move is a new break referencing the same
// records, so the history of what was decided when survives.
func (s Status) CanTransitionTo(target Status) bool {
	for _, allowed := range allowedTransitions[s] {
		if allowed == target {
			return true
		}
	}
	return false
}

// Resolution is a decision recorded against a break.
type Resolution struct {
	Status Status

	// Actor and Note are mandatory for a terminal status, and the database
	// enforces it independently. A break closed with no reason and no name is
	// indistinguishable from one that was quietly deleted.
	Actor string
	Note  string
	At    time.Time
}

func (r Resolution) Validate() error {
	if !r.Status.IsTerminal() && r.Status != StatusInvestigating {
		return fmt.Errorf("unknown resolution status %q", string(r.Status))
	}
	if r.Status.IsTerminal() {
		if r.Actor == "" {
			return fmt.Errorf("resolving a break requires an actor")
		}
		if r.Note == "" {
			return fmt.Errorf("resolving a break requires a reason")
		}
	}
	return nil
}
