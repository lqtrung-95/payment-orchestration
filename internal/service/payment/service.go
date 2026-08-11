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
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/psp"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

var ErrNotFound = errors.New("payment not found")

type Service struct {
	db        *postgres.DB
	txRepo    *txstore.Repository
	providers *psp.Registry
	logger    *slog.Logger
}

func NewService(db *postgres.DB, txRepo *txstore.Repository, providers *psp.Registry, logger *slog.Logger) *Service {
	return &Service{db: db, txRepo: txRepo, providers: providers, logger: logger}
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

	err = s.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.txRepo.Insert(ctx, tx, t); err != nil {
			return err
		}
		return s.txRepo.RecordStateChange(ctx, tx, txstore.StateChange{
			TransactionID: t.ID,
			To:            t.State,
			Reason:        "payment created",
			Actor:         in.Actor,
			SourceIP:      in.SourceIP,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("create payment: %w", err)
	}

	// Authorization runs inline for now. A later phase moves it behind the
	// transactional outbox, at which point this call becomes an enqueue and the
	// request stops waiting on a third party.
	//
	// A provider decline is not a request failure: the payment resource exists
	// and its state carries the outcome. Authorize returns the transaction
	// alongside any provider error and returns nil only when something
	// infrastructural broke, so a non-nil transaction is the answer.
	authorized, authErr := s.Authorize(ctx, t.ID)
	if authorized != nil {
		if authErr != nil {
			s.logger.InfoContext(ctx, "payment created with unsuccessful authorization",
				slog.String("transaction_id", t.ID.String()),
				slog.String("state", string(authorized.State)),
				slog.Any("error", authErr))
		}
		return authorized, nil
	}

	return nil, fmt.Errorf("authorize payment: %w", authErr)
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
