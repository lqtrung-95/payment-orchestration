package payment

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

// CaptureInput asks for money to actually be taken.
type CaptureInput struct {
	TransactionID uuid.UUID
	MerchantID    string

	// Amount may be less than the authorised total; partial capture is normal,
	// and a merchant shipping half an order captures half.
	Amount money.Money

	Actor    string
	SourceIP string
}

// Capture takes funds against an existing authorisation and records the
// movement in the ledger.
//
// This is where the ledger stops being decorative. Authorisation posts nothing,
// because a hold at the issuer is a reservation rather than a transfer (see the
// package comment). Capture is the moment money actually moves, so it is the
// first point at which double-entry has anything to say.
//
// The entry has three legs, not two:
//
//	Dr  psp clearing        gross    — the provider now owes us this
//	Cr  merchant payable    gross-fee — we owe the merchant their share
//	Cr  platform fee revenue fee      — our cut
//
// The fee comes out of what the merchant is owed, never out of what the
// customer pays: the customer authorised one amount and that is the amount
// taken. Netting the fee against the customer's charge instead would mean the
// authorisation and the capture disagree, which is a reconciliation break by
// construction.
func (s *Service) Capture(ctx context.Context, in CaptureInput) (*domain.Transaction, error) {
	db, err := s.shardOf(in.MerchantID)
	if err != nil {
		return nil, err
	}

	t, err := s.txRepo.Get(ctx, db.Pool(), in.TransactionID)
	if err != nil {
		return nil, err
	}
	if t.MerchantID != in.MerchantID {
		// Answered as not-found for the same reason Get is: distinguishing
		// "someone else's payment" from "no such payment" confirms which
		// identifiers are real.
		return nil, ErrNotFound
	}
	// Everything checkable is checked before the provider is called.
	//
	// Order matters here in a way that costs real money if it is wrong. The
	// aggregate refuses an over-capture, but if that refusal happens *after* the
	// provider call, the provider has already taken the funds and we then
	// decline to record them — the customer is charged and the ledger knows
	// nothing about it. Validating first means the request never reaches the
	// provider at all.
	if err := s.assertCapturable(t, in.Amount); err != nil {
		return nil, err
	}

	adapter, err := s.providers.Default()
	if err != nil {
		return nil, err
	}

	// Moved to capturing before the provider is called, so a crash mid-flight
	// leaves a state that says an attempt was in progress rather than one that
	// claims nothing happened.
	if err := s.transition(ctx, t, domain.StateCapturing, "sending capture to provider", adapter.Name()); err != nil {
		return nil, err
	}

	key := psp.OperationKey(t.ID, "capture")
	resp, captureErr := adapter.Capture(ctx, psp.CaptureRequest{
		IdempotencyKey:    key,
		TransactionID:     t.ID,
		ProviderReference: t.PSPReference,
		Amount:            in.Amount,
	})
	if captureErr != nil {
		return s.handleCaptureError(ctx, t, adapter, captureErr)
	}

	// The provider reports what it actually captured, which can differ from what
	// was asked for. Recording its figure rather than ours is what lets the
	// difference surface as a reconciliation break instead of being assumed away.
	captured := in.Amount
	if resp.Amount.IsValid() && resp.Amount.IsPositive() {
		captured = resp.Amount
	}

	return t, s.recordCapture(ctx, t, captured, in, adapter.Name())
}

// assertCapturable rejects a capture the transaction could never accept,
// before anything is sent to the provider.
//
// Deliberately duplicating checks the aggregate also makes. The aggregate is
// the authority, but it only gets consulted once the money has already moved;
// this is the pre-flight, and its whole value is being early rather than being
// authoritative.
func (s *Service) assertCapturable(t *domain.Transaction, amount money.Money) error {
	if err := amount.Currency().Validate(); err != nil {
		return err
	}
	if amount.Currency() != t.Amount.Currency() {
		return fmt.Errorf("%w: capture in %s, payment is %s",
			money.ErrCurrencyMismatch, amount.Currency(), t.Amount.Currency())
	}
	if !amount.IsPositive() {
		return fmt.Errorf("%w: got %s", domain.ErrAmountNotPositive, amount)
	}
	if !t.State.CanTransitionTo(domain.StateCapturing) {
		return fmt.Errorf("%w: %s -> capturing", domain.ErrIllegalTransition, t.State)
	}

	remaining, err := t.RemainingCapturable()
	if err != nil {
		return err
	}
	cmp, err := amount.Cmp(remaining)
	if err != nil {
		return err
	}
	if cmp > 0 {
		return fmt.Errorf("%w: %s requested, %s remaining of %s authorized",
			domain.ErrExceedsAuthorized, amount, remaining, t.Amount)
	}
	return nil
}

// handleCaptureError routes a failed capture by its normalized class.
//
// A capture that failed for a retryable reason returns to authorized rather
// than to failed: the authorisation is still good and the attempt can be
// repeated without putting the customer through authorisation again. That edge
// exists in the transition matrix for exactly this case.
func (s *Service) handleCaptureError(
	ctx context.Context,
	t *domain.Transaction,
	adapter psp.Adapter,
	captureErr error,
) (*domain.Transaction, error) {
	class := psp.ClassOf(captureErr)

	switch {
	case class.IsTerminal():
		if err := s.transition(ctx, t, domain.StateFailed, string(class), adapter.Name()); err != nil {
			return nil, err
		}
		return t, captureErr

	case class.IsRetryable():
		if err := s.transition(ctx, t, domain.StateAuthorized, "capture refused, authorisation retained", adapter.Name()); err != nil {
			return nil, err
		}
		return t, captureErr

	default:
		// Ambiguous: the money may have moved. The transaction stays in
		// capturing, which is non-terminal, so nothing downstream concludes the
		// capture failed. Resolution comes from a webhook or reconciliation.
		s.logger.WarnContext(ctx, "ambiguous capture outcome, leaving transaction open",
			slog.String("transaction_id", t.ID.String()),
			slog.String("class", string(class)))
		return t, fmt.Errorf("%w: %w", ErrOutcomeUnresolved, captureErr)
	}
}

// recordCapture writes the state change, the audit row, and the ledger entry in
// one database transaction.
//
// All three or none. A captured transaction with no ledger entry is money that
// moved and was never accounted for; a ledger entry with no captured
// transaction is money accounted for that never moved. Both are worse than the
// capture failing outright.
func (s *Service) recordCapture(
	ctx context.Context,
	t *domain.Transaction,
	captured money.Money,
	in CaptureInput,
	providerName string,
) error {
	from := t.State
	if err := t.Capture(captured); err != nil {
		return err
	}

	return s.router.WithTx(ctx, t.ShardKey, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.txRepo.Update(ctx, tx, t); err != nil {
			return err
		}
		if err := s.txRepo.RecordStateChange(ctx, tx, txstore.StateChange{
			TransactionID: t.ID,
			From:          from,
			To:            t.State,
			Reason:        fmt.Sprintf("captured %s", captured),
			Actor:         in.Actor,
			SourceIP:      in.SourceIP,
		}); err != nil {
			return err
		}
		return s.postCapture(ctx, tx, t, captured, providerName)
	})
}

// postCapture writes the double-entry legs for a capture.
func (s *Service) postCapture(
	ctx context.Context,
	tx pgx.Tx,
	t *domain.Transaction,
	captured money.Money,
	providerName string,
) error {
	schedule, err := s.feeRepo.ScheduleFor(ctx, tx, t.MerchantID, captured.Currency(), time.Now().UTC())
	if err != nil {
		return err
	}
	feeAmount, err := schedule.Calculate(captured)
	if err != nil {
		return err
	}
	payable, err := captured.Sub(feeAmount)
	if err != nil {
		return err
	}

	accounts, err := s.ensureCaptureAccounts(ctx, tx, t, captured.Currency(), providerName)
	if err != nil {
		return err
	}

	entry, err := ledgerdomain.NewEntry(&t.ID, t.ShardKey,
		fmt.Sprintf("capture of %s via %s", captured, providerName), time.Now().UTC(),
		ledgerdomain.Posting{AccountID: accounts.clearing, Direction: ledgerdomain.Debit, Amount: captured},
		ledgerdomain.Posting{AccountID: accounts.payable, Direction: ledgerdomain.Credit, Amount: payable},
		ledgerdomain.Posting{AccountID: accounts.feeRevenue, Direction: ledgerdomain.Credit, Amount: feeAmount},
	)
	if err != nil {
		return err
	}

	if err := s.ledgerRepo.RecordEntry(ctx, tx, entry); err != nil {
		return err
	}

	s.logger.InfoContext(ctx, "capture posted to the ledger",
		slog.String("transaction_id", t.ID.String()),
		slog.String("captured", captured.String()),
		slog.String("fee", feeAmount.String()),
		slog.String("payable", payable.String()))

	return nil
}

// captureAccounts are the three accounts a capture touches.
type captureAccounts struct {
	clearing   uuid.UUID
	payable    uuid.UUID
	feeRevenue uuid.UUID
}

// ensureCaptureAccounts resolves the accounts, creating them on first use.
//
// Created on demand because a merchant's first payment in a new currency should
// not need an out-of-band provisioning step — and because accounts are
// per-currency by construction, so the set grows as the business does.
func (s *Service) ensureCaptureAccounts(
	ctx context.Context,
	tx pgx.Tx,
	t *domain.Transaction,
	currency money.Currency,
	providerName string,
) (captureAccounts, error) {
	var out captureAccounts

	wanted := []struct {
		into *uuid.UUID
		spec ledgerdomain.Account
	}{
		{&out.clearing, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "psp", ID: providerName},
			Purpose: ledgerdomain.PurposeClearing,
			Type:    ledgerdomain.AccountTypeAsset,
		}},
		{&out.payable, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "merchant", ID: t.MerchantID},
			Purpose: ledgerdomain.PurposePayable,
			Type:    ledgerdomain.AccountTypeLiability,
		}},
		{&out.feeRevenue, ledgerdomain.Account{
			Owner:   ledgerdomain.Owner{Type: "platform", ID: "platform"},
			Purpose: ledgerdomain.PurposeFeeRevenue,
			Type:    ledgerdomain.AccountTypeRevenue,
		}},
	}

	for _, w := range wanted {
		w.spec.Currency = currency
		w.spec.ShardKey = t.ShardKey

		account, err := s.ledgerRepo.EnsureAccount(ctx, tx, w.spec)
		if err != nil {
			return captureAccounts{}, err
		}
		*w.into = account.ID
	}
	return out, nil
}
