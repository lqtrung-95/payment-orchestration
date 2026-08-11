package transaction

import (
	"errors"
	"fmt"
	"sort"
)

// State is the lifecycle position of a payment transaction.
type State string

const (
	StateCreated     State = "created"
	StateAuthorizing State = "authorizing"
	StateAuthorized  State = "authorized"
	StateCapturing   State = "capturing"
	StateCaptured    State = "captured"
	StateSettled     State = "settled"

	StateFailed            State = "failed"
	StateCancelled         State = "cancelled"
	StateRefunded          State = "refunded"
	StatePartiallyRefunded State = "partially_refunded"
	StateExpired           State = "expired"
)

var ErrIllegalTransition = errors.New("illegal state transition")

// allowedTransitions is the authoritative in-process copy of the transition
// matrix. It is mirrored by the transaction_state_transitions table, and a test
// asserts the two are identical — two encodings of one rule drift apart unless
// something compares them.
//
// A state with no outgoing entry is terminal.
var allowedTransitions = map[State][]State{
	// Nothing has reached a provider yet, so the payment can still be abandoned.
	StateCreated: {StateAuthorizing, StateCancelled, StateFailed, StateExpired},

	// Expired covers a redirect-based authorisation the customer never finished.
	StateAuthorizing: {StateAuthorized, StateFailed, StateExpired},

	// Expired covers an authorisation hold that lapsed before capture.
	StateAuthorized: {StateCapturing, StateCancelled, StateFailed, StateExpired},

	// Returning to authorized lets a retryable capture failure be reattempted
	// without putting the customer through authorisation again.
	StateCapturing: {StateCaptured, StateFailed, StateAuthorized},

	StateCaptured: {StateSettled, StatePartiallyRefunded, StateRefunded},
	StateSettled:  {StatePartiallyRefunded, StateRefunded},

	StatePartiallyRefunded: {StateRefunded},
}

// Validate reports whether s is a known state.
func (s State) Validate() error {
	if _, known := allStates[s]; !known {
		return fmt.Errorf("unknown transaction state %q", string(s))
	}
	return nil
}

func (s State) String() string { return string(s) }

// IsTerminal reports whether no further transition is possible. Same-state
// updates remain allowed: a second partial refund changes amounts, not state.
func (s State) IsTerminal() bool { return len(allowedTransitions[s]) == 0 }

// CanTransitionTo reports whether s -> target is legal. A transition to the
// same state is treated as a no-op and permitted.
func (s State) CanTransitionTo(target State) bool {
	if s == target {
		return true
	}
	for _, allowed := range allowedTransitions[s] {
		if allowed == target {
			return true
		}
	}
	return false
}

// Transition is one edge of the matrix, used for comparison against the
// database's copy.
type Transition struct {
	From State
	To   State
}

// Transitions returns every legal edge, sorted, excluding same-state no-ops.
func Transitions() []Transition {
	out := make([]Transition, 0, 32)
	for from, targets := range allowedTransitions {
		for _, to := range targets {
			out = append(out, Transition{From: from, To: to})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

// allStates is every state the enum admits, including terminal ones that never
// appear as a key in allowedTransitions.
var allStates = map[State]struct{}{
	StateCreated: {}, StateAuthorizing: {}, StateAuthorized: {},
	StateCapturing: {}, StateCaptured: {}, StateSettled: {},
	StateFailed: {}, StateCancelled: {}, StateRefunded: {},
	StatePartiallyRefunded: {}, StateExpired: {},
}

// States returns every known state, sorted.
func States() []State {
	out := make([]State, 0, len(allStates))
	for s := range allStates {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
