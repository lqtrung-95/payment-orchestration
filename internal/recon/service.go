package recon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
	fxstore "github.com/lequoctrung/payment-orchestrator/internal/store/fx"
)

// Service ingests settlement files and reconciles them against the ledger.
type Service struct {
	db         *postgres.DB
	parsers    *Registry
	repo       *Repository
	ledger     *LedgerReader
	fxRepo     *fxstore.Repository
	adjuster   *Adjuster
	classifier *Classifier
	logger     *slog.Logger
}

func NewService(
	db *postgres.DB,
	parsers *Registry,
	repo *Repository,
	ledger *LedgerReader,
	fxRepo *fxstore.Repository,
	ledgerWriter LedgerWriter,
	tolerances Tolerances,
	logger *slog.Logger,
) *Service {
	return &Service{
		db: db, parsers: parsers, repo: repo, ledger: ledger, fxRepo: fxRepo,
		adjuster:   NewAdjuster(ledgerWriter),
		classifier: NewClassifier(tolerances),
		logger:     logger,
	}
}

// Ingest parses and stores a settlement file.
//
// Returns the file and whether it was newly stored. Re-ingesting the same bytes
// is a no-op rather than an error: providers re-send files, and refusing the
// repeat would make an ordinary operational event look like a fault.
func (s *Service) Ingest(ctx context.Context, provider, filename string, r io.Reader) (File, bool, error) {
	parser, err := s.parsers.Get(provider)
	if err != nil {
		return File{}, false, err
	}

	file, err := parser.Parse(r)
	if err != nil {
		return File{}, false, err
	}
	file.Filename = filename

	var inserted bool
	err = s.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		inserted, err = s.repo.InsertFile(ctx, tx, &file)
		return err
	})
	if err != nil {
		return File{}, false, err
	}

	s.logger.InfoContext(ctx, "settlement file ingested",
		slog.String("file_id", file.ID.String()),
		slog.String("provider", provider),
		slog.Int("rows", file.RowCount),
		slog.Bool("new", inserted))

	return file, inserted, nil
}

// Report summarises a reconciliation run.
type Report struct {
	RunID   uuid.UUID
	FileID  uuid.UUID
	Matched int

	// ByCategory counts the breaks raised, keyed by category.
	ByCategory map[breaks.Category]int

	// NewBreaks counts only those not already recorded for this file, which is
	// what makes a repeat run visibly a no-op.
	NewBreaks int

	// AutoResolved counts breaks closed without a human, which only the
	// explained categories are eligible for.
	AutoResolved int

	// Exposure is the total absolute delta across breaks that have one, in
	// minor units per currency. What is actually at stake, rather than a count.
	Exposure map[string]int64
}

// Total returns the number of breaks across every category.
func (r Report) Total() int {
	var n int
	for _, count := range r.ByCategory {
		n += count
	}
	return n
}

// Reconcile matches a stored file against the ledger and records the breaks.
//
// Re-running is safe and is expected: a file is reconciled again after an
// ingestion gap is fixed, and the breaks that were already raised keep their
// identity and any decision recorded against them.
func (s *Service) Reconcile(ctx context.Context, fileID uuid.UUID, actor string) (Report, error) {
	file, err := s.repo.LoadFile(ctx, s.db.Pool(), fileID)
	if err != nil {
		return Report{}, err
	}

	records, err := s.ledger.CapturedInWindow(ctx, s.db.Pool(), file.Provider, file.PeriodStart, file.PeriodEnd)
	if err != nil {
		return Report{}, err
	}

	matched := Match(file.Rows, records)
	report := Report{
		FileID:     fileID,
		Matched:    len(matched.Matched),
		ByCategory: make(map[breaks.Category]int, len(breaks.All)),
		Exposure:   make(map[string]int64),
	}

	found, err := s.classifyAll(ctx, file, matched)
	if err != nil {
		return Report{}, err
	}

	err = s.db.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		runID, err := s.repo.StartRun(ctx, tx, fileID, actor)
		if err != nil {
			return err
		}
		report.RunID = runID

		for _, b := range found {
			isNew, err := s.repo.SaveBreak(ctx, tx, runID, fileID, b)
			if err != nil {
				return err
			}
			report.ByCategory[b.Category]++
			if b.Delta != nil {
				report.Exposure[string(b.Delta.Currency())] += absMinor(*b.Delta)
			}
			if !isNew {
				// Already raised, and possibly already decided. Re-resolving
				// would rewrite somebody's decision.
				continue
			}
			report.NewBreaks++

			resolved, err := s.autoResolve(ctx, tx, file, b)
			if err != nil {
				return err
			}
			if resolved {
				report.AutoResolved++
			}
		}

		return s.repo.FinishRun(ctx, tx, runID, report.Matched, report.Total())
	})
	if err != nil {
		return Report{}, err
	}

	s.logger.InfoContext(ctx, "reconciliation complete",
		slog.String("file_id", fileID.String()),
		slog.Int("matched", report.Matched),
		slog.Int("breaks", report.Total()),
		slog.Int("new_breaks", report.NewBreaks),
		slog.Int("auto_resolved", report.AutoResolved))

	return report, nil
}

// classifyAll runs every record through the classifier.
func (s *Service) classifyAll(ctx context.Context, file File, matched MatchResult) ([]Break, error) {
	var found []Break

	for _, pair := range matched.Matched {
		lock, err := s.lockFor(ctx, pair)
		if err != nil {
			return nil, err
		}

		b, err := s.classifier.ClassifyPair(pair, lock)
		if err != nil {
			return nil, fmt.Errorf("classify %s: %w", pair.Row.ProviderReference, err)
		}
		if b != nil {
			found = append(found, *b)
		}
	}

	for _, row := range matched.DuplicateRows {
		found = append(found, *s.classifier.ClassifyDuplicate(row))
	}
	for _, row := range matched.UnmatchedRows {
		found = append(found, *s.classifier.ClassifyUnmatchedRow(row))
	}
	for _, record := range matched.UnmatchedRecords {
		found = append(found, *s.classifier.ClassifyUnmatchedRecord(record, file))
	}

	return found, nil
}

// autoResolve closes a break that needs no human, reporting whether it did.
//
// Only the two *explained* categories qualify, and they are closed for opposite
// reasons. FX drift is real money and gets an adjustment entry, so the books
// stop disagreeing with the provider. A timing cutoff is not a discrepancy at
// all — the payment settles in the next file — so it is closed with no entry,
// because posting one would invent a movement that never happened.
//
// The actor is recorded as the reconciler rather than left blank. "Who closed
// this" has to have an answer even when the answer is "nobody, automatically".
func (s *Service) autoResolve(ctx context.Context, tx pgx.Tx, file File, b Break) (bool, error) {
	if !b.Category.AutoResolvable() {
		return false, nil
	}

	breakID, err := s.repo.BreakID(ctx, tx, file.ID, b.Category, b.MatchKey)
	if err != nil {
		return false, err
	}

	var adjustment *uuid.UUID
	note := "settles in an adjacent window; no movement to account for"

	if b.Category == breaks.FXDrift {
		if b.TransactionID == nil {
			return false, fmt.Errorf("fx drift break %s has no transaction", b.MatchKey)
		}
		shardKey, err := s.repo.ShardKeyFor(ctx, tx, *b.TransactionID)
		if err != nil {
			return false, err
		}

		entryID, err := s.adjuster.PostFXDrift(ctx, tx, b, *b.TransactionID, shardKey, file.Provider)
		if err != nil {
			return false, err
		}
		adjustment = &entryID
		note = fmt.Sprintf("rate movement accounted to fx gain/loss (%s)", b.Delta)
	}

	if err := s.repo.ResolveTx(ctx, tx, breakID, breaks.Resolution{
		Status: breaks.StatusResolved,
		Actor:  "system:reconciler",
		Note:   note,
		At:     time.Now().UTC(),
	}, adjustment); err != nil {
		return false, err
	}
	return true, nil
}

// lockFor returns the rate lock held for a pair's transaction, or nil.
//
// A missing lock is not an error: most payments never involve a conversion, and
// treating its absence as a fault would turn the common case into noise.
func (s *Service) lockFor(ctx context.Context, pair Pair) (*fx.Lock, error) {
	if !pair.Row.HasFX() {
		return nil, nil
	}

	lock, err := s.fxRepo.GetLock(ctx, s.db.Pool(), pair.Record.TransactionID)
	if errors.Is(err, fxstore.ErrLockNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &lock, nil
}
