package psp

import "testing"

// The success criterion for the retry ladder: a decline is never retried.
// Repeating it cannot change the issuer's answer, annoys the customer, and
// escalates a soft decline into a hard block on the instrument.
func TestDeclinesAreNeverRetried(t *testing.T) {
	for _, class := range []ErrorClass{
		ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor,
		ClassSuspectedFraud, ClassInvalidInstrument,
	} {
		decision := RetryPolicyFor(class)
		if decision.Retry {
			t.Errorf("%s is retried, but a decline must never be", class)
		}
		if decision.Reason == "" {
			t.Errorf("%s has no reason recorded; the decision would be unexplainable in an incident", class)
		}
	}
}

// Anything that might already have taken effect must confirm the outcome before
// trying again. Retrying without that check is the double-charge bug.
func TestAmbiguousFailuresRequireAStatusCheckBeforeRetry(t *testing.T) {
	for _, class := range []ErrorClass{ClassTimeout, ClassNetworkError, ClassUnknown} {
		decision := RetryPolicyFor(class)
		if !decision.Retry {
			t.Errorf("%s should be retryable once the outcome is known", class)
		}
		if !decision.RequiresStatusCheck {
			t.Errorf("%s permits a retry without confirming the outcome — this is how double charges happen", class)
		}
	}
}

// Nothing happened, so a retry is safe without a status check — but it must be
// slower, or the retry keeps the rate limit engaged.
func TestRefusalsRetryWithoutStatusCheckButBackOffHarder(t *testing.T) {
	for _, class := range []ErrorClass{ClassRateLimited, ClassUnavailable} {
		decision := RetryPolicyFor(class)
		if !decision.Retry {
			t.Errorf("%s should be retryable — the request never took effect", class)
		}
		if decision.RequiresStatusCheck {
			t.Errorf("%s should not need a status check; nothing happened", class)
		}
		if !decision.LongBackoff {
			t.Errorf("%s should use the slower tiers", class)
		}
	}
}

// Suspected fraud is terminal and must also be surfaced: repeated attempts
// against a flagged card make the situation materially worse.
func TestSuspectedFraudAlerts(t *testing.T) {
	decision := RetryPolicyFor(ClassSuspectedFraud)
	if decision.Retry {
		t.Error("suspected fraud must never be retried")
	}
	if !decision.Alert {
		t.Error("suspected fraud should raise an alert")
	}
}

// An unrecognised failure gets exactly one confirmation attempt, not the full
// ladder: grinding against a fault nobody understands helps nothing.
func TestUnknownFailuresGetASingleConfirmationAttempt(t *testing.T) {
	decision := RetryPolicyFor(ClassUnknown)
	if !decision.RequiresStatusCheck {
		t.Error("an unknown failure must confirm the outcome before retrying")
	}
	if decision.MaxAttempts != 2 {
		t.Errorf("MaxAttempts = %d, want 2 (the original plus one confirmation)", decision.MaxAttempts)
	}
	if !decision.Alert {
		t.Error("an unrecognised failure should be surfaced")
	}
}

// An unmapped class must default to the cautious branch, never to free retries.
func TestUnmappedClassDefaultsToCautious(t *testing.T) {
	decision := RetryPolicyFor(ErrorClass("something_new"))
	if !decision.RequiresStatusCheck {
		t.Error("an unmapped class must not be retried without confirming the outcome")
	}
	if !decision.Alert {
		t.Error("an unmapped class should be surfaced so the taxonomy gets extended")
	}
}

// Every class in the taxonomy must have an explicit decision, so adding one
// without deciding its policy fails here rather than silently defaulting.
func TestEveryClassHasAPolicy(t *testing.T) {
	all := []ErrorClass{
		ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor, ClassSuspectedFraud,
		ClassInvalidInstrument, ClassTimeout, ClassNetworkError, ClassUnknown,
		ClassRateLimited, ClassUnavailable,
	}

	for _, class := range all {
		decision := RetryPolicyFor(class)
		if decision.Reason == "" {
			t.Errorf("class %s has no retry policy reason", class)
		}
		// Terminal classes must not be retried, and non-terminal ones must be.
		if class.IsTerminal() && decision.Retry {
			t.Errorf("terminal class %s is marked retryable", class)
		}
		if !class.IsTerminal() && !decision.Retry {
			t.Errorf("non-terminal class %s is not retryable", class)
		}
	}
}
