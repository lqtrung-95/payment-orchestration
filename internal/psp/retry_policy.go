package psp

// RetryDecision is what to do with a failed provider operation.
type RetryDecision struct {
	// Retry reports whether another attempt is permitted at all.
	Retry bool

	// RequiresStatusCheck means the outcome must be established via GetStatus
	// before any retry. Retrying without it risks charging twice.
	RequiresStatusCheck bool

	// MaxAttempts caps total attempts including the first. Zero means the
	// ladder's default cap applies.
	MaxAttempts int

	// LongBackoff selects the slower retry tiers, for a provider that has asked
	// to be left alone rather than one that merely stumbled.
	LongBackoff bool

	// Alert marks a failure a human should see regardless of how it resolves.
	Alert bool

	Reason string
}

// RetryPolicyFor maps a normalized error class onto a retry decision.
//
// A uniform "retry everything a few times" policy is the common naive answer and
// it is wrong in both directions: it repeats declines, which is user-hostile and
// trips issuer fraud controls, and it retries ambiguous failures blindly, which
// charges customers twice. The differentiation below is the entire point.
func RetryPolicyFor(class ErrorClass) RetryDecision {
	switch class {
	// Ambiguous. The operation may already have taken effect, so a retry is
	// permitted only after the provider confirms it did not.
	case ClassTimeout, ClassNetworkError:
		return RetryDecision{
			Retry:               true,
			RequiresStatusCheck: true,
			Reason:              "outcome unknown; confirm via status before retrying",
		}

	// Also ambiguous, but less is known: the failure was not even recognisable.
	// One confirmation attempt, then stop and let a human or reconciliation
	// decide rather than grinding against an unexplained fault.
	case ClassUnknown:
		return RetryDecision{
			Retry:               true,
			RequiresStatusCheck: true,
			MaxAttempts:         2,
			Alert:               true,
			Reason:              "unrecognised failure; single confirmation attempt only",
		}

	// The provider asked to be left alone. Nothing happened, so a retry is safe,
	// but it must be slower — retrying a rate limit at the same pace is what
	// keeps the limit engaged.
	case ClassRateLimited:
		return RetryDecision{
			Retry:       true,
			LongBackoff: true,
			Reason:      "provider rate limited the request",
		}

	// Explicitly refused service. Nothing happened; the circuit breaker governs
	// the pace rather than the retry ladder alone.
	case ClassUnavailable:
		return RetryDecision{
			Retry:       true,
			LongBackoff: true,
			Reason:      "provider unavailable",
		}

	// Terminal. The issuer decided, and asking again does not change the answer.
	case ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor:
		return RetryDecision{Reason: "issuer declined; retrying cannot change the outcome"}

	// Terminal and worth surfacing: repeated attempts against a card the issuer
	// has flagged escalate a soft block into a hard one.
	case ClassSuspectedFraud:
		return RetryDecision{Alert: true, Reason: "suspected fraud; never retry"}

	// Terminal until the instrument itself is fixed. Re-verification is wired up
	// in a later phase; retrying the same instrument achieves nothing meanwhile.
	case ClassInvalidInstrument:
		return RetryDecision{Reason: "instrument unusable; requires re-verification"}

	default:
		// An unmapped class is treated as the most cautious option available:
		// possibly effective, so never blindly repeated.
		return RetryDecision{
			Retry:               true,
			RequiresStatusCheck: true,
			MaxAttempts:         2,
			Alert:               true,
			Reason:              "unmapped error class",
		}
	}
}
