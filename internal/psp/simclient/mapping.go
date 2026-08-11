package simclient

import "github.com/lequoctrung/payment-orchestrator/internal/psp"

// Wire types mirroring the simulator's own vocabulary. They are duplicated here
// rather than imported from the simulator package on purpose: an adapter must
// depend on the provider's published contract, not on its internals. Sharing
// the types would let a change inside the simulator silently alter what the
// orchestrator believes it is talking to.
type chargeRequest struct {
	TransactionID string `json:"transaction_id"`
	Reference     string `json:"reference,omitempty"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
	ReturnURL     string `json:"return_url,omitempty"`
}

type chargeResponse struct {
	Reference   string `json:"reference"`
	Status      string `json:"status"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	RedirectURL string `json:"redirect_url,omitempty"`
	RawStatus   string `json:"raw_status"`
	RawCode     string `json:"raw_code,omitempty"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// mapStatus translates the provider's status vocabulary into the normalized one.
//
// An unrecognised status maps to pending, not to a terminal state. A provider
// that invents a status we have never seen is telling us something is still in
// motion; treating it as settled would be the more dangerous guess.
func mapStatus(raw string) psp.Status {
	switch raw {
	case "AUTH_OK":
		return psp.StatusAuthorized
	case "CAPTURED":
		return psp.StatusCaptured
	case "REFUNDED":
		return psp.StatusRefunded
	case "VOIDED":
		return psp.StatusVoided
	case "PENDING_ASYNC":
		return psp.StatusPending
	case "ACTION_REQUIRED":
		return psp.StatusRequiresAction
	case "FAILED":
		return psp.StatusFailed
	case "NOT_FOUND":
		return psp.StatusNotFound
	default:
		return psp.StatusPending
	}
}

// mapDeclineCode translates the provider's decline codes into normalized
// classes. Every one of these is terminal: the issuer decided.
func mapDeclineCode(code string) psp.ErrorClass {
	switch code {
	case "insufficient_funds":
		return psp.ClassInsufficientFunds
	case "do_not_honor":
		return psp.ClassDoNotHonor
	case "suspected_fraud":
		return psp.ClassSuspectedFraud
	case "invalid_instrument", "expired_card":
		return psp.ClassInvalidInstrument
	default:
		return psp.ClassDeclined
	}
}
