// Package psp defines the provider-facing contract that every payment service
// provider adapter implements, and the normalized vocabulary the orchestrator
// reasons about.
//
// Providers disagree about everything: field names, status vocabularies, which
// HTTP code means "declined". Normalizing at the adapter boundary means the
// orchestration logic is written once, and adding a provider cannot change how
// a decline is handled.
package psp

import (
	"errors"
	"fmt"
)

// ErrorClass is the normalized category of a provider failure.
//
// This taxonomy exists to answer two questions that a raw error string cannot:
// may this be retried, and might the money have moved anyway. Getting the second
// one wrong is how double charges happen.
type ErrorClass string

const (
	// Definitive refusals. The provider decided, and the answer is no.
	ClassDeclined          ErrorClass = "declined"
	ClassInsufficientFunds ErrorClass = "insufficient_funds"
	ClassDoNotHonor        ErrorClass = "do_not_honor"
	ClassSuspectedFraud    ErrorClass = "suspected_fraud"
	ClassInvalidInstrument ErrorClass = "invalid_instrument"

	// Ambiguous outcomes. The request may or may not have taken effect; the
	// caller does not know and must not guess.
	ClassTimeout      ErrorClass = "timeout"
	ClassNetworkError ErrorClass = "network_error"
	ClassUnknown      ErrorClass = "unknown"

	// Refusals to service the request at all. Nothing happened, and trying
	// again later is the correct response.
	ClassRateLimited ErrorClass = "rate_limited"
	ClassUnavailable ErrorClass = "unavailable"
)

// IsAmbiguous reports whether the operation may have succeeded despite the
// error.
//
// This is the single most important predicate in the system. A timeout does not
// mean the charge failed — it means the response was lost. Retrying on an
// ambiguous outcome charges the customer twice. The only safe move is to ask
// the provider what actually happened, via GetStatus, before deciding anything.
func (c ErrorClass) IsAmbiguous() bool {
	switch c {
	case ClassTimeout, ClassNetworkError, ClassUnknown:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether the provider gave a definitive refusal.
//
// Terminal errors must never be retried. Retrying a decline is user-hostile,
// achieves nothing, and repeated attempts against the same instrument trip
// issuer fraud controls — turning a soft decline into a hard block.
func (c ErrorClass) IsTerminal() bool {
	switch c {
	case ClassDeclined, ClassInsufficientFunds, ClassDoNotHonor,
		ClassSuspectedFraud, ClassInvalidInstrument:
		return true
	default:
		return false
	}
}

// IsRetryable reports whether the request can be retried without first
// confirming the outcome, because it demonstrably never took effect.
func (c ErrorClass) IsRetryable() bool {
	switch c {
	case ClassRateLimited, ClassUnavailable:
		return true
	default:
		return false
	}
}

// Error carries a normalized class alongside the provider's own code and
// message. The raw values are retained because they are what appears in the
// provider's dashboard and in their support tickets; discarding them makes
// reconciling an incident with the provider considerably harder.
type Error struct {
	Class      ErrorClass
	Provider   string
	RawCode    string
	RawMessage string

	// Retryable overrides the class default when a provider explicitly says
	// otherwise, for example a decline the issuer marks as "try again".
	RetryAfterSeconds int

	wrapped error
}

func (e *Error) Error() string {
	if e.RawCode != "" {
		return fmt.Sprintf("%s: %s (%s: %s)", e.Provider, e.Class, e.RawCode, e.RawMessage)
	}
	return fmt.Sprintf("%s: %s: %s", e.Provider, e.Class, e.RawMessage)
}

func (e *Error) Unwrap() error { return e.wrapped }

// Is supports errors.Is against a sentinel carrying only a class, so callers
// can write errors.Is(err, psp.ErrTimeout) without unwrapping by hand.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Class == e.Class && t.Provider == "" && t.RawCode == ""
}

// Sentinels for errors.Is comparison by class.
var (
	ErrDeclined          = &Error{Class: ClassDeclined}
	ErrInsufficientFunds = &Error{Class: ClassInsufficientFunds}
	ErrDoNotHonor        = &Error{Class: ClassDoNotHonor}
	ErrSuspectedFraud    = &Error{Class: ClassSuspectedFraud}
	ErrInvalidInstrument = &Error{Class: ClassInvalidInstrument}
	ErrTimeout           = &Error{Class: ClassTimeout}
	ErrNetworkError      = &Error{Class: ClassNetworkError}
	ErrRateLimited       = &Error{Class: ClassRateLimited}
	ErrUnavailable       = &Error{Class: ClassUnavailable}
	ErrUnknown           = &Error{Class: ClassUnknown}
)

// NewError builds a normalized provider error.
func NewError(provider string, class ErrorClass, rawCode, rawMessage string, wrapped error) *Error {
	return &Error{
		Class:      class,
		Provider:   provider,
		RawCode:    rawCode,
		RawMessage: rawMessage,
		wrapped:    wrapped,
	}
}

// ClassOf extracts the normalized class from an error, returning ClassUnknown
// for anything that is not a provider error.
//
// Defaulting to Unknown — which is ambiguous, not terminal — is deliberate. An
// unrecognised failure is treated as "the outcome is unclear, go and check",
// never as "it definitely failed, retry freely".
func ClassOf(err error) ErrorClass {
	var pspErr *Error
	if errors.As(err, &pspErr) {
		return pspErr.Class
	}
	if err != nil {
		return ClassUnknown
	}
	return ""
}
