package tcc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	ledgerdomain "github.com/lequoctrung/payment-orchestrator/internal/domain/ledger"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	ledgerstore "github.com/lequoctrung/payment-orchestrator/internal/store/ledger"
)

// Participant performs one side of a transfer, inside a transaction on that
// side's own database.
//
// Everything here runs on a single shard. That is the constraint the whole
// protocol exists to work within: a participant can be atomic about its own
// half and knows nothing about the other.
type Participant struct {
	ledger *ledgerstore.Repository
}

func NewParticipant() *Participant {
	return &Participant{ledger: ledgerstore.NewRepository()}
}

// Try records a hold. Nothing is posted: the funds become unspendable while the
// balance stays exactly where it was, which is what makes a later cancel exact
// rather than compensating.
//
// Idempotent. A retried Try collides with the one-reservation-per-role
// constraint and returns the existing hold rather than taking a second one.
func (p *Participant) Try(ctx context.Context, tx pgx.Tx, t *Transfer, role Role) (*Reservation, error) {
	merchant, shardKey := t.SourceMerchant, t.SourceShardKey
	if role == RoleDestination {
		merchant, shardKey = t.DestMerchant, t.DestShardKey
	}

	if existing, err := p.reservation(ctx, tx, t.ID, role); err == nil {
		return existing, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	// Only the source is capacity-checked. A destination cannot decline funds,
	// and asking it to would add a failure mode with no corresponding real
	// constraint.
	if role == RoleSource {
		if err := p.assertFundsAvailable(ctx, tx, merchant, t.Amount); err != nil {
			return nil, err
		}
	}

	const insert = `
		INSERT INTO tcc_reservations (
			id, transfer_id, role, merchant_id, shard_key, amount_minor, currency, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (transfer_id, role) DO NOTHING`

	id := uuid.New()
	tag, err := tx.Exec(ctx, insert, id, t.ID, string(role), merchant, shardKey,
		t.Amount.Amount(), string(t.Amount.Currency()), t.TimeoutAt)
	if err != nil {
		return nil, fmt.Errorf("reserve %s side of transfer %s: %w", role, t.ID, err)
	}
	if tag.RowsAffected() == 0 {
		// Lost the race with a concurrent Try for the same side. The winner's
		// hold is the one that counts.
		return p.reservation(ctx, tx, t.ID, role)
	}

	return &Reservation{
		ID: id, TransferID: t.ID, Role: role, MerchantID: merchant,
		ShardKey: shardKey, Amount: t.Amount, State: ReservationReserved,
		ExpiresAt: t.TimeoutAt,
	}, nil
}

// assertFundsAvailable refuses a hold the merchant cannot cover.
//
// Available means the derived payable balance minus every hold already
// outstanding against it. Checking the balance alone would let two concurrent
// transfers of sixty against a hundred both succeed, each seeing a balance the
// other had not yet spent.
//
// An advisory lock on the merchant and currency serialises the read and the
// insert. Without it the two transfers read the same available figure before
// either has inserted, and both pass a check that was correct when it ran. The
// lock is transaction-scoped, so it is released by the commit or rollback that
// ends this call — a crash here cannot strand it.
func (p *Participant) assertFundsAvailable(ctx context.Context, tx pgx.Tx, merchant string, amount money.Money) error {
	lockKey := "tcc:balance:" + merchant + ":" + string(amount.Currency())
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, lockKey); err != nil {
		return fmt.Errorf("lock balance for %s: %w", merchant, err)
	}

	const query = `
		SELECT
			COALESCE((
				SELECT SUM(CASE WHEN p.direction = 'credit' THEN p.amount_minor ELSE -p.amount_minor END)
				FROM postings p
				JOIN ledger_accounts a ON a.id = p.account_id
				WHERE a.owner_type = 'merchant' AND a.owner_id = $1
				  AND a.purpose = 'payable' AND a.currency = $2
			), 0)
			-
			COALESCE((
				SELECT SUM(r.amount_minor)
				FROM tcc_reservations r
				WHERE r.merchant_id = $1 AND r.currency = $2
				  AND r.role = 'source' AND r.state = 'reserved'
			), 0)`

	var available int64
	if err := tx.QueryRow(ctx, query, merchant, string(amount.Currency())).Scan(&available); err != nil {
		return fmt.Errorf("available balance for %s: %w", merchant, err)
	}

	if available < amount.Amount() {
		return fmt.Errorf("%w: %s has %d %s available, needs %d",
			ErrInsufficientFunds, merchant, available, amount.Currency(), amount.Amount())
	}
	return nil
}

// Confirm posts this side's half of the movement and closes the hold.
//
// Idempotent: a reservation already confirmed returns its existing entry rather
// than posting a second one. Confirm is retried until it succeeds — that is the
// contract past the commit point — so it has to be safe to run twice.
func (p *Participant) Confirm(ctx context.Context, tx pgx.Tx, t *Transfer, role Role) (uuid.UUID, error) {
	res, err := p.lockReservation(ctx, tx, t.ID, role)
	if err != nil {
		return uuid.Nil, err
	}

	switch res.State {
	case ReservationConfirmed:
		return *res.EntryID, nil
	case ReservationCancelled:
		// Cancelled then confirmed means the coordinator cancelled a transfer
		// it had already committed to, which is a protocol violation rather
		// than a race. Posting anyway would move money the other side has been
		// told is not coming.
		return uuid.Nil, fmt.Errorf("transfer %s: %s side was already cancelled", t.ID, role)
	}

	entryID, err := p.postHalf(ctx, tx, t, role, res)
	if err != nil {
		return uuid.Nil, err
	}

	const update = `
		UPDATE tcc_reservations
		SET state = 'confirmed', entry_id = $2, resolved_at = now()
		WHERE id = $1 AND state = 'reserved'`

	tag, err := tx.Exec(ctx, update, res.ID, entryID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("confirm reservation %s: %w", res.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return uuid.Nil, fmt.Errorf("confirm reservation %s: hold changed underneath", res.ID)
	}
	return entryID, nil
}

// Cancel releases a hold without posting anything.
//
// This is the property TCC buys over a saga: there is nothing to reverse,
// because nothing moved. The balance was never touched, so the merchant's books
// show no trace of a transfer that did not happen.
func (p *Participant) Cancel(ctx context.Context, tx pgx.Tx, transferID uuid.UUID, role Role) error {
	res, err := p.lockReservation(ctx, tx, transferID, role)
	if errors.Is(err, pgx.ErrNoRows) {
		// Try never reached this participant, so there is nothing to release.
		return nil
	}
	if err != nil {
		return err
	}

	switch res.State {
	case ReservationCancelled:
		return nil
	case ReservationConfirmed:
		return fmt.Errorf("%w: %s side of transfer %s is already posted",
			ErrAlreadyResolved, role, transferID)
	}

	const update = `
		UPDATE tcc_reservations SET state = 'cancelled', resolved_at = now()
		WHERE id = $1 AND state = 'reserved'`

	if _, err := tx.Exec(ctx, update, res.ID); err != nil {
		return fmt.Errorf("cancel reservation %s: %w", res.ID, err)
	}
	return nil
}

// OutstandingHolds reports the total still reserved on this shard, used by the
// tests and by operational tooling to confirm nothing was stranded.
func (p *Participant) OutstandingHolds(ctx context.Context, q postgres.Querier) (int64, error) {
	const query = `SELECT COALESCE(SUM(amount_minor), 0) FROM tcc_reservations WHERE state = 'reserved'`

	var total int64
	if err := q.QueryRow(ctx, query).Scan(&total); err != nil {
		return 0, fmt.Errorf("sum outstanding holds: %w", err)
	}
	return total, nil
}

const reservationColumns = `id, transfer_id, role, merchant_id, shard_key, amount_minor, currency, state, entry_id, expires_at`

func (p *Participant) reservation(ctx context.Context, q postgres.Querier, transferID uuid.UUID, role Role) (*Reservation, error) {
	row := q.QueryRow(ctx,
		`SELECT `+reservationColumns+` FROM tcc_reservations WHERE transfer_id = $1 AND role = $2`,
		transferID, string(role))
	return scanReservation(row)
}

// lockReservation takes a row lock so two confirms of the same side serialise
// rather than both deciding the hold is still open.
func (p *Participant) lockReservation(ctx context.Context, tx pgx.Tx, transferID uuid.UUID, role Role) (*Reservation, error) {
	row := tx.QueryRow(ctx,
		`SELECT `+reservationColumns+` FROM tcc_reservations
		 WHERE transfer_id = $1 AND role = $2 FOR UPDATE`,
		transferID, string(role))

	res, err := scanReservation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("lock reservation for transfer %s: %w", transferID, err)
	}
	return res, nil
}

// postHalf writes the balanced entry for one side.
//
// Source:      Dr merchant payable   Cr transfer suspense
// Destination: Dr transfer suspense  Cr merchant payable
//
// Each entry balances within its own database, which it must — the balance
// trigger is per-entry and the databases share nothing. Across shards the two
// suspense legs are equal and opposite, so the system-wide suspense position is
// zero whenever no transfer is mid-flight.
func (p *Participant) postHalf(ctx context.Context, tx pgx.Tx, t *Transfer, role Role, res *Reservation) (uuid.UUID, error) {
	payable, err := p.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
		Owner:    ledgerdomain.Owner{Type: "merchant", ID: res.MerchantID},
		Purpose:  ledgerdomain.PurposePayable,
		Type:     ledgerdomain.AccountTypeLiability,
		Currency: t.Amount.Currency(),
		ShardKey: res.ShardKey,
	})
	if err != nil {
		return uuid.Nil, err
	}

	suspense, err := p.ledger.EnsureAccount(ctx, tx, ledgerdomain.Account{
		Owner:    ledgerdomain.Owner{Type: "platform", ID: "platform"},
		Purpose:  ledgerdomain.PurposeTransferSuspense,
		Type:     ledgerdomain.AccountTypeLiability,
		Currency: t.Amount.Currency(),
		ShardKey: res.ShardKey,
	})
	if err != nil {
		return uuid.Nil, err
	}

	debit, credit := payable.ID, suspense.ID
	if role == RoleDestination {
		debit, credit = suspense.ID, payable.ID
	}

	entry, err := ledgerdomain.NewEntry(nil, res.ShardKey,
		fmt.Sprintf("cross-shard transfer %s (%s)", t.ID, role), time.Now().UTC(),
		ledgerdomain.Posting{AccountID: debit, Direction: ledgerdomain.Debit, Amount: t.Amount},
		ledgerdomain.Posting{AccountID: credit, Direction: ledgerdomain.Credit, Amount: t.Amount},
	)
	if err != nil {
		return uuid.Nil, err
	}
	if err := p.ledger.RecordEntry(ctx, tx, entry); err != nil {
		return uuid.Nil, err
	}
	return entry.ID, nil
}

func scanReservation(row pgx.Row) (*Reservation, error) {
	var (
		res      Reservation
		role     string
		state    string
		minor    int64
		currency string
	)
	if err := row.Scan(&res.ID, &res.TransferID, &role, &res.MerchantID, &res.ShardKey,
		&minor, &currency, &state, &res.EntryID, &res.ExpiresAt); err != nil {
		return nil, err
	}

	amount, err := money.New(minor, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("reservation %s has an unusable amount: %w", res.ID, err)
	}

	res.Role = Role(role)
	res.State = ReservationState(state)
	res.Amount = amount
	return &res, nil
}
