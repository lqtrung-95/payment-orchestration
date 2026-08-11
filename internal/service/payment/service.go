// Package payment orchestrates payment operations across the transaction
// aggregate and the ledger.
//
// A deliberate accounting decision runs through this package: the ledger
// records money that moved, not intent. Creating or authorising a payment posts
// nothing, because an authorisation is a reservation held at the issuer, not a
// transfer. Postings begin at capture, when funds are actually taken. Recording
// authorisations as ledger entries would inflate every balance by the value of
// holds that may never be captured, and reconciliation against a settlement
// file would then never match.
package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/messaging"
	"github.com/lequoctrung/payment-orchestrator/internal/outbox"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

var ErrNotFound = errors.New("payment not found")

type Service struct {
	db        *postgres.DB
	txRepo    *txstore.Repository
	providers *psp.Registry
	outbox    *outbox.Writer
	topics    messaging.Topics
	logger    *slog.Logger
}

func NewService(
	db *postgres.DB,
	txRepo *txstore.Repository,
	providers *psp.Registry,
	outboxWriter *outbox.Writer,
	topics messaging.Topics,
	logger *slog.Logger,
) *Service {
	return &Service{
		db: db, txRepo: txRepo, providers: providers,
		outbox: outboxWriter, topics: topics, logger: logger,
	}
}

type CreateInput struct {
	MerchantID     string
	IdempotencyKey string
	Amount         money.Money

	// Actor and SourceIP are recorded on the audit trail. Attribution is what
	// makes the trail usable when reconstructing an incident.
	Actor    string
	SourceIP string
}

// Create records a new payment transaction.
//
// The aggregate and its first audit row are written in one transaction: a
// transaction whose history does not start at creation cannot be reconstructed,
// and reconstructing history is the entire purpose of the trail.
func (s *Service) Create(ctx context.Context, in CreateInput) (*domain.Transaction, error) {
	t, err := domain.New(in.MerchantID, in.IdempotencyKey, in.Amount)
	if err != nil {
		return nil, err
	}

	// The transaction, its first audit row, and the message that will drive its
	// authorization are written in one database transaction.
	//
	// This atomicity is the reason the outbox exists. Publishing to the broker
	// inside the transaction would emit work for a payment that then rolls back;
	// publishing after the commit loses the work whenever the process dies in
	// between. Writing both to the same database makes the pair all-or-nothing,
	// and a separate relay carries it to the broker afterwards.
	err = s.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.txRepo.Insert(ctx, tx, t); err != nil {
			return err
		}
		if err := s.txRepo.RecordStateChange(ctx, tx, txstore.StateChange{
			TransactionID: t.ID,
			To:            t.State,
			Reason:        "payment created",
			Actor:         in.Actor,
			SourceIP:      in.SourceIP,
		}); err != nil {
			return err
		}

		_, err := s.outbox.Enqueue(ctx, tx, outbox.Message{
			AggregateID: t.ID,
			// Keyed by merchant so every event for one merchant is ordered
			// relative to the others.
			PartitionKey: t.MerchantID,
			Topic:        s.topics.Authorize,
			Payload: AuthorizeRequested{
				TransactionID: t.ID,
				MerchantID:    t.MerchantID,
			},
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create payment: %w", err)
	}

	// Returned immediately in the `created` state. The caller is not made to
	// wait on a third party, and the outcome is discovered by polling the
	// resource or, in a later phase, by a webhook.
	return t, nil
}

// AuthorizeRequested is the outbox payload that asks a worker to authorize.
//
// It carries identifiers rather than a snapshot of the transaction, so a message
// that waited in a retry tier acts on current state rather than on what was true
// when it was queued.
type AuthorizeRequested struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	MerchantID    string    `json:"merchant_id"`
}

// Get returns a payment belonging to the given merchant.
//
// The merchant is checked here rather than in the query's WHERE clause so that
// a caller asking for someone else's payment gets the same not-found answer as
// one asking for a payment that does not exist. Distinguishing the two would
// let a caller confirm which transaction identifiers are real.
func (s *Service) Get(ctx context.Context, merchantID string, id uuid.UUID) (*domain.Transaction, error) {
	t, err := s.txRepo.Get(ctx, s.db.Pool(), id)
	if errors.Is(err, txstore.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if t.MerchantID != merchantID {
		return nil, ErrNotFound
	}
	return t, nil
}
