package recon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
)

var ErrFileNotFound = errors.New("settlement file not found")

type Repository struct{}

func NewRepository() *Repository { return &Repository{} }

// InsertFile stores a parsed file and its rows, reporting whether it was new.
//
// A conflict on the content hash is an ordinary outcome, not an error:
// providers re-send files routinely, and the correct response is to recognise
// the one already held rather than to store a second copy under a new id.
func (r *Repository) InsertFile(ctx context.Context, q postgres.Querier, file *File) (bool, error) {
	const insertFile = `
		INSERT INTO settlement_files (provider, filename, content_sha256, period_start, period_end, row_count)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (provider, content_sha256) DO NOTHING
		RETURNING id, ingested_at`

	err := q.QueryRow(ctx, insertFile,
		file.Provider, file.Filename, file.ContentSHA256,
		file.PeriodStart, file.PeriodEnd, file.RowCount,
	).Scan(&file.ID, &file.IngestedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		// Already ingested. Load the existing identity so the caller can
		// reconcile against it rather than treating this as a failure.
		const existing = `
			SELECT id, period_start, period_end, row_count, ingested_at
			FROM settlement_files WHERE provider = $1 AND content_sha256 = $2`

		if err := q.QueryRow(ctx, existing, file.Provider, file.ContentSHA256).Scan(
			&file.ID, &file.PeriodStart, &file.PeriodEnd, &file.RowCount, &file.IngestedAt,
		); err != nil {
			return false, fmt.Errorf("load existing settlement file: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert settlement file: %w", err)
	}

	const insertRow = `
		INSERT INTO settlement_rows
			(file_id, line_number, provider_reference, gross_minor, fee_minor, net_minor,
			 currency, settlement_currency, settlement_rate_nano, settled_at, raw)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, 0), $10, $11)
		RETURNING id`

	for i := range file.Rows {
		row := &file.Rows[i]
		if err := q.QueryRow(ctx, insertRow,
			file.ID, row.LineNumber, row.ProviderReference,
			row.Gross.Amount(), row.Fee.Amount(), row.Net.Amount(),
			string(row.Gross.Currency()), string(row.SettlementCurrency), row.SettlementRateNano,
			row.SettledAt, row.Raw,
		).Scan(&row.ID); err != nil {
			return false, fmt.Errorf("insert settlement row %d: %w", row.LineNumber, err)
		}
		row.FileID = file.ID
	}

	return true, nil
}

// LoadFile reads a stored file and its rows.
func (r *Repository) LoadFile(ctx context.Context, q postgres.Querier, fileID uuid.UUID) (File, error) {
	const fileQuery = `
		SELECT id, provider, filename, content_sha256, period_start, period_end, row_count, ingested_at
		FROM settlement_files WHERE id = $1`

	var file File
	err := q.QueryRow(ctx, fileQuery, fileID).Scan(
		&file.ID, &file.Provider, &file.Filename, &file.ContentSHA256,
		&file.PeriodStart, &file.PeriodEnd, &file.RowCount, &file.IngestedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return File{}, fmt.Errorf("%w: %s", ErrFileNotFound, fileID)
	}
	if err != nil {
		return File{}, fmt.Errorf("load settlement file: %w", err)
	}

	file.Rows, err = r.loadRows(ctx, q, fileID)
	return file, err
}

func (r *Repository) loadRows(ctx context.Context, q postgres.Querier, fileID uuid.UUID) ([]Row, error) {
	const query = `
		SELECT id, line_number, provider_reference, gross_minor, fee_minor, net_minor,
		       currency, COALESCE(settlement_currency, ''), COALESCE(settlement_rate_nano, 0),
		       settled_at, raw
		FROM settlement_rows WHERE file_id = $1 ORDER BY line_number`

	rows, err := q.Query(ctx, query, fileID)
	if err != nil {
		return nil, fmt.Errorf("load settlement rows: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var (
			row                      Row
			gross, fee, net          int64
			currency, settleCurrency string
		)
		if err := rows.Scan(&row.ID, &row.LineNumber, &row.ProviderReference,
			&gross, &fee, &net, &currency, &settleCurrency, &row.SettlementRateNano,
			&row.SettledAt, &row.Raw); err != nil {
			return nil, fmt.Errorf("scan settlement row: %w", err)
		}

		cur := money.Currency(currency)
		if row.Gross, err = money.New(gross, cur); err != nil {
			return nil, err
		}
		if row.Fee, err = money.New(fee, cur); err != nil {
			return nil, err
		}
		if row.Net, err = money.New(net, cur); err != nil {
			return nil, err
		}
		row.FileID = fileID
		row.SettlementCurrency = money.Currency(settleCurrency)
		out = append(out, row)
	}
	return out, rows.Err()
}

// StartRun records the beginning of a reconciliation.
func (r *Repository) StartRun(ctx context.Context, q postgres.Querier, fileID uuid.UUID, actor string) (uuid.UUID, error) {
	const query = `INSERT INTO recon_runs (file_id, actor) VALUES ($1, $2) RETURNING id`

	var id uuid.UUID
	if err := q.QueryRow(ctx, query, fileID, actor).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("start reconciliation run: %w", err)
	}
	return id, nil
}

// FinishRun records the totals.
func (r *Repository) FinishRun(ctx context.Context, q postgres.Querier, runID uuid.UUID, matched, breakCount int) error {
	const query = `
		UPDATE recon_runs SET matched_count = $2, break_count = $3, finished_at = now()
		WHERE id = $1`

	if _, err := q.Exec(ctx, query, runID, matched, breakCount); err != nil {
		return fmt.Errorf("finish reconciliation run: %w", err)
	}
	return nil
}

// SaveBreak records a classified break, reporting whether it was new.
//
// Conflicting on the natural identity is what makes re-running a
// reconciliation idempotent: the same disagreement is recognised rather than
// raised a second time, and any decision already recorded against it survives.
func (r *Repository) SaveBreak(
	ctx context.Context,
	q postgres.Querier,
	runID, fileID uuid.UUID,
	b Break,
) (bool, error) {
	const query = `
		INSERT INTO recon_breaks
			(run_id, file_id, category, match_key, transaction_id, settlement_row_id,
			 expected_minor, actual_minor, delta_minor, currency, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10, ''), $11)
		ON CONFLICT (file_id, category, match_key) DO NOTHING
		RETURNING id`

	var id uuid.UUID
	err := q.QueryRow(ctx, query,
		runID, fileID, string(b.Category), b.MatchKey, b.TransactionID, b.SettlementRowID,
		minorOrNil(b.Expected), minorOrNil(b.Actual), minorOrNil(b.Delta),
		currencyOf(b), b.Detail,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("save reconciliation break: %w", err)
	}
	return true, nil
}

// Resolve records a decision against a break.
func (r *Repository) Resolve(
	ctx context.Context,
	q postgres.Querier,
	breakID uuid.UUID,
	res breaks.Resolution,
	adjustmentEntryID *uuid.UUID,
) error {
	if err := res.Validate(); err != nil {
		return err
	}

	const query = `
		UPDATE recon_breaks
		SET status = $2, resolution_note = NULLIF($3, ''), resolved_by = NULLIF($4, ''),
		    resolved_at = $5, adjustment_entry_id = COALESCE($6, adjustment_entry_id)
		WHERE id = $1 AND status IN ('open', 'investigating')`

	at := res.At
	if at.IsZero() {
		at = time.Now().UTC()
	}

	tag, err := q.Exec(ctx, query, breakID, string(res.Status), res.Note, res.Actor, at, adjustmentEntryID)
	if err != nil {
		return fmt.Errorf("resolve break: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Either it does not exist or it has already been decided. Reopening a
		// decided break would rewrite somebody's decision; raising a new break
		// is the correct move.
		return fmt.Errorf("break %s is not open", breakID)
	}
	return nil
}

func minorOrNil(m *money.Money) *int64 {
	if m == nil {
		return nil
	}
	v := m.Amount()
	return &v
}

func currencyOf(b Break) string {
	for _, m := range []*money.Money{b.Expected, b.Actual, b.Delta} {
		if m != nil {
			return string(m.Currency())
		}
	}
	return ""
}
