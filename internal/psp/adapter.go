package psp

import (
	"context"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

// Status is the provider's view of a charge, deliberately separate from the
// orchestrator's own transaction state.
//
// Conflating the two would make every provider's vocabulary leak into the
// domain, and the domain would then have to change whenever a provider added a
// status. The adapter maps into this set; the service maps this set onto
// transaction states.
type Status string

const (
	StatusAuthorized Status = "authorized"
	StatusCaptured   Status = "captured"
	StatusRefunded   Status = "refunded"
	StatusVoided     Status = "voided"
	StatusFailed     Status = "failed"

	// StatusPending means the provider accepted the request but the outcome
	// arrives later, out of band. The transaction parks in a non-terminal state
	// until a webhook resolves it.
	StatusPending Status = "pending"

	// StatusRequiresAction means the customer must complete a step elsewhere,
	// such as a 3-D Secure challenge. RedirectURL carries where to send them.
	StatusRequiresAction Status = "requires_action"

	// StatusNotFound is returned only by GetStatus, and is the answer that makes
	// recovery safe: the provider confirms no charge exists under this
	// idempotency key, so the operation may be retried without risking a
	// duplicate. Nothing else licenses a retry after an ambiguous failure.
	StatusNotFound Status = "not_found"
)

// IsTerminal reports whether the provider considers the charge settled one way
// or another, with no further movement expected without a new request.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCaptured, StatusRefunded, StatusVoided, StatusFailed:
		return true
	default:
		return false
	}
}

// Adapter is the contract every provider implements.
//
// Each method takes an IdempotencyKey scoped to the provider. It is not the
// merchant's key: one merchant request can produce several provider operations
// (authorize, then capture, then refund), each needing its own key, and the
// merchant's key namespace is not the provider's. See OperationKey.
type Adapter interface {
	Name() string

	Authorize(ctx context.Context, req AuthorizeRequest) (*Response, error)
	Capture(ctx context.Context, req CaptureRequest) (*Response, error)
	Refund(ctx context.Context, req RefundRequest) (*Response, error)
	Void(ctx context.Context, req VoidRequest) (*Response, error)

	// GetStatus reports what the provider believes happened, keyed by the
	// idempotency key of the original operation. It is the recovery path after
	// any ambiguous failure and must be safe to call at any time.
	GetStatus(ctx context.Context, req StatusRequest) (*Response, error)
}

type AuthorizeRequest struct {
	IdempotencyKey string
	TransactionID  uuid.UUID
	Amount         money.Money

	// ReturnURL is where a redirect-based provider sends the customer back to
	// after a challenge.
	ReturnURL string
}

type CaptureRequest struct {
	IdempotencyKey string
	TransactionID  uuid.UUID

	// ProviderReference identifies the authorization being captured.
	ProviderReference string

	// Amount may be less than the authorized total; partial capture is normal.
	Amount money.Money
}

type RefundRequest struct {
	IdempotencyKey    string
	TransactionID     uuid.UUID
	ProviderReference string
	Amount            money.Money
}

type VoidRequest struct {
	IdempotencyKey    string
	TransactionID     uuid.UUID
	ProviderReference string
}

type StatusRequest struct {
	// IdempotencyKey of the operation whose outcome is being resolved. Keyed by
	// idempotency key rather than by provider reference because after an
	// ambiguous failure the caller may never have received a reference.
	IdempotencyKey string
	TransactionID  uuid.UUID
}

// Response is the normalized result of a provider operation.
type Response struct {
	Status Status

	// ProviderReference is the provider's own identifier for the charge, needed
	// for every subsequent operation and for reconciliation against settlement.
	ProviderReference string

	// Amount is what the provider actually acted on, which can differ from what
	// was requested — a partial capture, or a provider that rounds. The
	// difference is a reconciliation break, so it is recorded rather than
	// assumed equal.
	Amount money.Money

	// RedirectURL is set when Status is StatusRequiresAction.
	RedirectURL string

	RawStatus string
	RawCode   string
}

// OperationKey derives the provider-scoped idempotency key for an operation.
//
// It is deliberately not the merchant's idempotency key. That key is scoped per
// merchant and could collide across providers; more importantly a single
// merchant request produces several provider operations, and reusing one key
// for all of them would make the provider treat a capture as a replay of the
// authorization.
//
// The key is a pure function of the transaction and the operation, so a retry
// of the same logical operation always presents the same key — which is what
// makes the provider's own idempotency protect us.
func OperationKey(transactionID uuid.UUID, operation string) string {
	return transactionID.String() + ":" + operation
}
