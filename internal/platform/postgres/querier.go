package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is the subset of pgx satisfied by both *pgxpool.Pool and pgx.Tx.
//
// Repositories accept a Querier rather than holding a pool, which is what lets
// a caller compose several repository operations into one transaction. The
// transactional outbox in a later phase depends on exactly this: the domain
// write and the outbox write must share a transaction or the guarantee is lost.
//
// It also makes tests cheap to isolate — each one runs inside a transaction
// that is rolled back, so no test needs its own database.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Both implementations must keep satisfying Querier; a signature change in
// either should fail the build here rather than at the first call site.
var (
	_ Querier = (*pgxpool.Pool)(nil)
	_ Querier = (pgx.Tx)(nil)
)
