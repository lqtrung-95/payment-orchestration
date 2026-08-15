package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

// ErrOutcomeUnresolved means the provider's answer could not be established.
//
// The transaction is left in a non-terminal state on purpose. Marking it failed
// would be a guess, and the wrong guess loses a payment the customer was
// actually charged for. Something has to resolve it later — a webhook, a retry,
// or reconciliation — but nothing may resolve it by assumption.
var ErrOutcomeUnresolved = errors.New("provider outcome unresolved")

// statusRecheckAttempts is how many times GetStatus is re-asked when it reports
// no charge after an ambiguous failure.
//
// A provider's status endpoint is frequently served from a replica that lags
// its write path, so an immediate "not found" can be a lie. Believing it once
// would license a retry against a charge that already exists.
const statusRecheckAttempts = 3

const statusRecheckDelay = 150 * time.Millisecond

// Authorize sends a payment to its provider and records the outcome.
//
// The interesting half of this function is the error path. A provider failure
// falls into one of three categories and each demands a different response:
// a definitive decline is terminal, a refusal to service is retryable, and an
// ambiguous failure means the money may already have moved — which must be
// established by asking, never by assuming.
//
// Return contract, which callers rely on: a provider outcome — including a
// decline or an unresolved result — returns the transaction alongside a
// non-nil error, because the outcome is recorded in the transaction's state.
// A nil transaction means something infrastructural failed and nothing can be
// concluded about the payment at all.
//
// The merchant is a parameter rather than something looked up, because looking
// it up would require the row this call is on its way to fetch. Sharding makes
// the owning merchant part of a payment's address, so the message that asks for
// an authorization has to carry it.
func (s *Service) Authorize(ctx context.Context, merchantID string, transactionID uuid.UUID) (*domain.Transaction, error) {
	db, err := s.shardOf(merchantID)
	if err != nil {
		return nil, err
	}

	t, err := s.txRepo.Get(ctx, db.Pool(), transactionID)
	if err != nil {
		return nil, err
	}
	if t.MerchantID != merchantID {
		// The row was found on the merchant's shard but belongs to someone
		// else, which means a shard key was derived from one merchant and the
		// row written under another. Refusing is the only safe answer: the
		// alternative is authorising a stranger's payment.
		return nil, ErrNotFound
	}

	adapter, err := s.providers.Default()
	if err != nil {
		return nil, err
	}

	if err := s.transition(ctx, t, domain.StateAuthorizing, "sending to provider", adapter.Name()); err != nil {
		return nil, err
	}

	key := psp.OperationKey(t.ID, "authorize")
	resp, authErr := adapter.Authorize(ctx, psp.AuthorizeRequest{
		IdempotencyKey: key,
		TransactionID:  t.ID,
		Amount:         t.Amount,
	})

	if authErr != nil {
		return s.handleAuthorizeError(ctx, t, adapter, key, authErr)
	}

	return s.applyProviderStatus(ctx, t, adapter.Name(), resp, "provider responded")
}

// handleAuthorizeError routes a provider failure by its normalized class.
func (s *Service) handleAuthorizeError(
	ctx context.Context,
	t *domain.Transaction,
	adapter psp.Adapter,
	key string,
	authErr error,
) (*domain.Transaction, error) {
	class := psp.ClassOf(authErr)

	switch {
	case class.IsTerminal():
		// The issuer decided. Retrying achieves nothing and repeated attempts
		// against the same instrument trip fraud controls.
		s.logger.InfoContext(ctx, "provider declined",
			slog.String("transaction_id", t.ID.String()),
			slog.String("class", string(class)))

		if err := s.transition(ctx, t, domain.StateFailed, string(class), adapter.Name()); err != nil {
			return nil, err
		}
		return t, authErr

	case class.IsRetryable():
		// Nothing happened, so the transaction stays where it is and can be
		// attempted again. It is deliberately not failed.
		s.logger.WarnContext(ctx, "provider refused the request",
			slog.String("transaction_id", t.ID.String()),
			slog.String("class", string(class)))
		return t, authErr

	default:
		// Ambiguous. This is the case that produces double charges when handled
		// carelessly, and lost payments when handled pessimistically.
		return s.resolveAmbiguousOutcome(ctx, t, adapter, key, authErr)
	}
}

// resolveAmbiguousOutcome establishes what really happened after a timeout or a
// 5xx, by asking the provider rather than guessing.
//
// A retry here without checking first is the classic double charge: the request
// succeeded and only the reply was lost. Equally, concluding "it failed" is how
// a real payment gets written off as failed while the customer's money is gone.
func (s *Service) resolveAmbiguousOutcome(
	ctx context.Context,
	t *domain.Transaction,
	adapter psp.Adapter,
	key string,
	authErr error,
) (*domain.Transaction, error) {
	s.logger.WarnContext(ctx, "ambiguous provider outcome, querying status",
		slog.String("transaction_id", t.ID.String()),
		slog.String("class", string(psp.ClassOf(authErr))),
		slog.Any("error", authErr))

	for attempt := 1; attempt <= statusRecheckAttempts; attempt++ {
		resp, statusErr := adapter.GetStatus(ctx, psp.StatusRequest{
			IdempotencyKey: key,
			TransactionID:  t.ID,
		})

		if statusErr != nil {
			// The recovery path itself failed. Nothing is known, so nothing is
			// decided; the transaction stays non-terminal.
			s.logger.ErrorContext(ctx, "status query failed",
				slog.String("transaction_id", t.ID.String()),
				slog.Int("attempt", attempt),
				slog.Any("error", statusErr))
			break
		}

		if resp.Status != psp.StatusNotFound {
			s.logger.InfoContext(ctx, "recovered provider outcome",
				slog.String("transaction_id", t.ID.String()),
				slog.String("status", string(resp.Status)),
				slog.Int("attempt", attempt))
			return s.applyProviderStatus(ctx, t, adapter.Name(), resp, "recovered after ambiguous failure")
		}

		// Not found may simply mean the status replica has not caught up. Ask
		// again before believing it.
		if attempt < statusRecheckAttempts {
			select {
			case <-time.After(statusRecheckDelay):
			case <-ctx.Done():
				return t, fmt.Errorf("%w: %w", ErrOutcomeUnresolved, ctx.Err())
			}
		}
	}

	// Either the provider consistently reports no charge, or it could not be
	// reached. Both leave the transaction non-terminal: the first is safe to
	// retry later, and the second must not be resolved by assumption.
	s.logger.WarnContext(ctx, "provider outcome unresolved, leaving transaction open",
		slog.String("transaction_id", t.ID.String()),
		slog.String("state", string(t.State)))

	return t, fmt.Errorf("%w: %w", ErrOutcomeUnresolved, authErr)
}

// applyProviderStatus maps a provider status onto a transaction state.
func (s *Service) applyProviderStatus(
	ctx context.Context,
	t *domain.Transaction,
	providerName string,
	resp *psp.Response,
	reason string,
) (*domain.Transaction, error) {
	t.PSP = providerName
	t.PSPReference = resp.ProviderReference

	switch resp.Status {
	case psp.StatusAuthorized:
		if err := s.transition(ctx, t, domain.StateAuthorized, reason, providerName); err != nil {
			return nil, err
		}

	case psp.StatusPending, psp.StatusRequiresAction:
		// The transaction stays in authorizing. It is genuinely unresolved:
		// the outcome arrives by webhook, or the customer completes a challenge,
		// and until then any other state would be a claim we cannot support.
		if err := s.persist(ctx, t); err != nil {
			return nil, err
		}

	case psp.StatusFailed:
		if err := s.transition(ctx, t, domain.StateFailed, reason, providerName); err != nil {
			return nil, err
		}

	case psp.StatusCaptured:
		// A provider that captured on authorize. Recorded as authorized here;
		// the capture is applied by the capture path so that the ledger entry
		// has exactly one origin.
		if err := s.transition(ctx, t, domain.StateAuthorized, reason, providerName); err != nil {
			return nil, err
		}

	default:
		if err := s.persist(ctx, t); err != nil {
			return nil, err
		}
	}

	return t, nil
}

// transition moves the aggregate and records the change in one database
// transaction, so the audit trail can never disagree with the row it describes.
func (s *Service) transition(ctx context.Context, t *domain.Transaction, target domain.State, reason, actor string) error {
	from := t.State
	if err := t.TransitionTo(target); err != nil {
		return err
	}

	return s.router.WithTx(ctx, t.ShardKey, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.txRepo.Update(ctx, tx, t); err != nil {
			return err
		}
		return s.txRepo.RecordStateChange(ctx, tx, txstore.StateChange{
			TransactionID: t.ID,
			From:          from,
			To:            target,
			Reason:        reason,
			Actor:         "psp:" + actor,
		})
	})
}

// persist writes the aggregate without a state change, used when a provider
// response updates references but leaves the transaction where it was.
func (s *Service) persist(ctx context.Context, t *domain.Transaction) error {
	return s.router.WithTx(ctx, t.ShardKey, func(ctx context.Context, tx pgx.Tx) error {
		return s.txRepo.Update(ctx, tx, t)
	})
}
