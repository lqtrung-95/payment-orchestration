package transaction

import "testing"

func TestTerminalStates(t *testing.T) {
	terminal := map[State]bool{
		StateFailed:    true,
		StateCancelled: true,
		StateExpired:   true,
		StateRefunded:  true,
	}

	for _, s := range States() {
		if got, want := s.IsTerminal(), terminal[s]; got != want {
			t.Errorf("%s.IsTerminal() = %t, want %t", s, got, want)
		}
	}
}

func TestCanTransitionTo(t *testing.T) {
	tests := []struct {
		from  State
		to    State
		legal bool
	}{
		{StateCreated, StateAuthorizing, true},
		{StateAuthorizing, StateAuthorized, true},
		{StateAuthorized, StateCapturing, true},
		{StateCapturing, StateCaptured, true},
		{StateCaptured, StateSettled, true},

		// A capture that failed retryably returns to authorized so it can be
		// reattempted without re-authorising the customer.
		{StateCapturing, StateAuthorized, true},

		// Authorisation holds lapse.
		{StateAuthorized, StateExpired, true},

		// Skipping capture would settle money that was never taken.
		{StateAuthorizing, StateSettled, false},
		{StateCreated, StateCaptured, false},

		// Terminal states never move again.
		{StateRefunded, StateCaptured, false},
		{StateFailed, StateAuthorizing, false},
		{StateCancelled, StateAuthorized, false},
		{StateExpired, StateAuthorizing, false},

		// Money already captured cannot be un-captured by cancelling.
		{StateCaptured, StateCancelled, false},

		// Same-state updates change amounts, not state, and are permitted.
		{StateCaptured, StateCaptured, true},
		{StatePartiallyRefunded, StatePartiallyRefunded, true},
	}

	for _, tt := range tests {
		if got := tt.from.CanTransitionTo(tt.to); got != tt.legal {
			t.Errorf("%s -> %s = %t, want %t", tt.from, tt.to, got, tt.legal)
		}
	}
}

// Every state must be reachable from created, or it is dead configuration that
// will confuse the next person to read the matrix.
func TestEveryStateIsReachableFromCreated(t *testing.T) {
	seen := map[State]bool{StateCreated: true}
	queue := []State{StateCreated}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range allowedTransitions[current] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}

	for _, s := range States() {
		if !seen[s] {
			t.Errorf("state %s is unreachable from created", s)
		}
	}
}

func TestValidateRejectsUnknownState(t *testing.T) {
	if err := State("settling").Validate(); err == nil {
		t.Error("Validate() accepted an unknown state")
	}
	for _, s := range States() {
		if err := s.Validate(); err != nil {
			t.Errorf("Validate() rejected known state %s: %v", s, err)
		}
	}
}

// The transition table must contain no duplicate or self-referential edges.
func TestTransitionsAreWellFormed(t *testing.T) {
	seen := make(map[Transition]bool)
	for _, tr := range Transitions() {
		if tr.From == tr.To {
			t.Errorf("self-transition %s -> %s should not be listed explicitly", tr.From, tr.To)
		}
		if seen[tr] {
			t.Errorf("duplicate transition %s -> %s", tr.From, tr.To)
		}
		seen[tr] = true

		if err := tr.From.Validate(); err != nil {
			t.Errorf("transition references unknown from-state %s", tr.From)
		}
		if err := tr.To.Validate(); err != nil {
			t.Errorf("transition references unknown to-state %s", tr.To)
		}
	}
}
