package psp

import (
	"errors"
	"fmt"
	"testing"
)

// The three predicates must partition the taxonomy: every class is exactly one
// of terminal, retryable, or ambiguous. A class that is none of them would fall
// through every branch of the error handling; one that is two would take
// whichever branch happened to be checked first.
func TestErrorClassesArePartitioned(t *testing.T) {
	all := []ErrorClass{
		ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor, ClassSuspectedFraud,
		ClassInvalidInstrument, ClassTimeout, ClassNetworkError, ClassUnknown,
		ClassRateLimited, ClassUnavailable,
	}

	for _, c := range all {
		count := 0
		for _, in := range []bool{c.IsTerminal(), c.IsRetryable(), c.IsAmbiguous()} {
			if in {
				count++
			}
		}
		if count != 1 {
			t.Errorf("class %s is in %d categories, want exactly 1 (terminal=%t retryable=%t ambiguous=%t)",
				c, count, c.IsTerminal(), c.IsRetryable(), c.IsAmbiguous())
		}
	}
}

// A timeout must never be treated as a failure. The request may have succeeded
// and only the reply been lost; retrying on that basis charges twice.
func TestAmbiguousClassesAreNotTerminal(t *testing.T) {
	for _, c := range []ErrorClass{ClassTimeout, ClassNetworkError, ClassUnknown} {
		if !c.IsAmbiguous() {
			t.Errorf("%s should be ambiguous", c)
		}
		if c.IsTerminal() {
			t.Errorf("%s must not be terminal — the outcome is unknown", c)
		}
		if c.IsRetryable() {
			t.Errorf("%s must not be freely retryable — the outcome must be confirmed first", c)
		}
	}
}

// Declines are decisions, not faults. Retrying them is user-hostile and trips
// issuer fraud controls.
func TestDeclineClassesAreTerminal(t *testing.T) {
	for _, c := range []ErrorClass{
		ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor,
		ClassSuspectedFraud, ClassInvalidInstrument,
	} {
		if !c.IsTerminal() {
			t.Errorf("%s should be terminal", c)
		}
		if c.IsRetryable() {
			t.Errorf("%s must never be retried", c)
		}
	}
}

// An unrecognised failure defaults to ambiguous, never to terminal. Guessing
// "it failed" about an unknown outcome is how a real payment gets written off.
func TestClassOfDefaultsToAmbiguousUnknown(t *testing.T) {
	if got := ClassOf(errors.New("something unexpected")); got != ClassUnknown {
		t.Errorf("ClassOf(plain error) = %s, want %s", got, ClassUnknown)
	}
	if !ClassUnknown.IsAmbiguous() {
		t.Error("ClassUnknown must be ambiguous so unrecognised failures force a status check")
	}
	if got := ClassOf(nil); got != "" {
		t.Errorf("ClassOf(nil) = %q, want empty", got)
	}
}

func TestClassOfUnwrapsNestedErrors(t *testing.T) {
	base := NewError("psp-sync-sim", ClassInsufficientFunds, "51", "insufficient funds", nil)
	wrapped := fmt.Errorf("authorize failed: %w", base)

	if got := ClassOf(wrapped); got != ClassInsufficientFunds {
		t.Errorf("ClassOf(wrapped) = %s, want %s", got, ClassInsufficientFunds)
	}
}

func TestErrorsIsMatchesByClass(t *testing.T) {
	err := NewError("psp-sync-sim", ClassTimeout, "", "no response", nil)

	if !errors.Is(err, ErrTimeout) {
		t.Error("errors.Is should match a sentinel of the same class")
	}
	if errors.Is(err, ErrDeclined) {
		t.Error("errors.Is should not match a sentinel of a different class")
	}

	wrapped := fmt.Errorf("authorize: %w", err)
	if !errors.Is(wrapped, ErrTimeout) {
		t.Error("errors.Is should match through wrapping")
	}
}

func TestErrorPreservesProviderDetail(t *testing.T) {
	underlying := errors.New("dial tcp: connection refused")
	err := NewError("psp-async-sim", ClassNetworkError, "conn_refused", "could not connect", underlying)

	if !errors.Is(err, underlying) {
		t.Error("the underlying cause should remain reachable through Unwrap")
	}
	// The provider's own code has to survive: it is what appears in their
	// dashboard and in any support conversation about the transaction.
	if err.RawCode != "conn_refused" {
		t.Errorf("RawCode = %q, want conn_refused", err.RawCode)
	}
	if got := err.Error(); got == "" {
		t.Error("Error() should render something usable")
	}
}
