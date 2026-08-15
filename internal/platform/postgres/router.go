package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
)

// Router owns one connection pool per physical database and resolves which of
// them a piece of work belongs to.
//
// Every write that carries a shard key goes through Router.WithTx, which picks
// the pool from the key rather than from anything the caller decides. That is
// the property worth having: routing cannot drift from what is stored, because
// the stored key is the only input.
//
// There is deliberately no method that opens a transaction spanning two shards.
// Postgres cannot commit across databases, so such a method could only ever be
// two transactions wearing one name — and the moment one commits and the other
// does not, money exists in one place and not the other. Work that genuinely
// spans shards goes through the TCC coordinator instead, where the partial
// state is explicit and recoverable.
type Router struct {
	mapping sharding.Mapping
	shards  []*DB
}

// NewRouter opens a pool per DSN. The DSN order is the physical shard order and
// is part of the deployment contract: reordering the list silently reassigns
// every merchant to a different database.
func NewRouter(ctx context.Context, cfg config.Postgres, dsns []string) (*Router, error) {
	if len(dsns) == 0 {
		return nil, errors.New("router needs at least one shard dsn")
	}

	mapping, err := sharding.NewMapping(len(dsns))
	if err != nil {
		return nil, err
	}

	r := &Router{mapping: mapping, shards: make([]*DB, 0, len(dsns))}
	for i, dsn := range dsns {
		shardCfg := cfg
		shardCfg.DSN = dsn

		db, err := New(ctx, shardCfg)
		if err != nil {
			// Everything opened so far is closed before returning: a partially
			// constructed router that the caller never receives would otherwise
			// leak its pools for the lifetime of the process.
			r.Close()
			return nil, fmt.Errorf("open shard %d: %w", i, err)
		}
		r.shards = append(r.shards, db)
	}
	return r, nil
}

// Mapping exposes the logical-to-physical assignment for tooling that reports
// or moves what lives where.
func (r *Router) Mapping() sharding.Mapping { return r.mapping }

// Shards returns every pool, in physical order.
//
// For work that must visit all of them — the outbox relay, the invariant
// checker, migrations. Each visit is its own transaction; nothing here implies
// they commit together.
func (r *Router) Shards() []*DB { return r.shards }

// Global returns the pool holding tables that are not merchant-partitioned:
// the webhook log, whose events arrive before any merchant is known; settlement
// files and reconciliation breaks, which are per-provider back-office records;
// and the schema-migration bookkeeping.
//
// It is physical shard 0 by definition rather than by accident. Concentrating
// them makes shard 0 hotter than its peers, which is a real and accepted cost —
// the alternative, replicating mutable back-office state to every shard, needs
// cross-shard consistency for data that has no merchant to partition on.
//
// Reference data — fee schedules, FX rates — is a separate case. It is read
// inside shard transactions, so it lives on every shard and is replicated
// there, not fetched from here.
func (r *Router) Global() *DB { return r.shards[0] }

// Shard resolves the pool holding a shard key's rows.
func (r *Router) Shard(shardKey string) (*DB, error) {
	physical, err := r.mapping.Resolve(shardKey)
	if err != nil {
		return nil, err
	}
	return r.shards[physical], nil
}

// WithTx runs fn inside a transaction on the shard that owns shardKey.
func (r *Router) WithTx(ctx context.Context, shardKey string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	db, err := r.Shard(shardKey)
	if err != nil {
		return err
	}
	return db.WithTx(ctx, fn)
}

// Ping verifies every shard. A router that reports healthy while one of its
// databases is unreachable would route a fraction of merchants into failures
// that look like application bugs.
func (r *Router) Ping(ctx context.Context) error {
	for i, db := range r.shards {
		if err := db.Ping(ctx); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

func (r *Router) Close() {
	for _, db := range r.shards {
		db.Close()
	}
}
