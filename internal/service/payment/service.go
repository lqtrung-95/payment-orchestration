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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	domain "github.com/lequoctrung/payment-orchestrator/internal/domain/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
)

var ErrNotFound = errors.New("payment not found")

type Service struct {
	db     *postgres.DB
	txRepo *txstore.Repository
}

func NewService(db *postgres.DB, txRepo *txstore.Repository) *Service {
	return &Service{db: db, txRepo: txRepo}
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

	return t, nil
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
