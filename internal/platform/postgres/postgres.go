// Package postgres owns the connection pool and the single sanctioned path for
// running work inside a database transaction.
//
// WithTx is deliberately the only way transactional work happens. The
// transactional outbox pattern introduced in a later phase requires that domain
// writes and outbox writes commit atomically; if callers can open transactions
// by other means, that guarantee erodes quietly and reappears as lost or
// phantom payment events.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
)

type DB struct {
	pool *pgxpool.Pool
}

// New builds and verifies a connection pool. It returns only after a successful
// round trip, so a healthy return value means the database is genuinely
// reachable rather than merely configured.
func New(ctx context.Context, cfg config.Postgres) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	// A server-side statement timeout is the backstop for a query that would
	// otherwise hold a connection and any locks it acquired indefinitely. Client
	// context cancellation alone does not stop work already running in Postgres.
	if cfg.StatementTimeout > 0 {
		if poolCfg.ConnConfig.RuntimeParams == nil {
			poolCfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		ms := cfg.StatementTimeout.Milliseconds()
		poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", ms)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Pool exposes the underlying pool for read paths that need no transaction.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

func (db *DB) Close() { db.pool.Close() }

func (db *DB) Ping(ctx context.Context) error {
	if err := db.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}

// WithTx runs fn inside a transaction, committing when fn returns nil and
// rolling back otherwise.
//
// A panic inside fn triggers a rollback and is then re-raised, so a bug in
// domain code can never leave a transaction open holding row locks.
func (db *DB) WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = rollback(ctx, tx)
			panic(p)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := rollback(ctx, tx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return fmt.Errorf("%w (rollback also failed: %w)", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// rollback aborts tx using a context that carries the caller's values but none
// of its cancellation. Cleanup must still run when the request that triggered
// it has already been cancelled — otherwise a client disconnect leaves the
// transaction open, holding row locks until the server reaps it.
func rollback(ctx context.Context, tx pgx.Tx) error {
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	return tx.Rollback(rbCtx)
}

const rollbackTimeout = 5 * time.Second
