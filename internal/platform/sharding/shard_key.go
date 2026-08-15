// Package sharding derives the shard key that partitions the payment data set.
//
// Only key derivation lives here for now; physical routing across multiple
// databases arrives in a later phase. The key itself is settled early and
// stored on every row, because backfilling a shard key across a populated
// ledger means rewriting every row while the service stays online.
package sharding

import (
	"fmt"
	"hash/fnv"
)

// LogicalShards is fixed at 64 and must never change.
//
// The standard escape hatch for resharding is a fixed logical count mapped onto
// a variable number of physical databases: growing capacity then becomes a
// mapping change rather than a rehash of every existing row. Changing this
// constant would relocate live data and break that property.
const LogicalShards = 64

// Shard keys are stored as a fixed-width string — s00 through s63 — rather than
// an integer. Fixed width keeps them sortable and range-scannable as text, and
// the prefix makes a stray shard key obvious in a log line or a query plan.
const (
	shardKeyPrefix = "s"
	shardKeyDigits = 2
)

// KeyForMerchant derives the shard key from a merchant identifier.
//
// Merchant is the partition dimension because merchant-scoped reads dominate
// this workload — balances, settlement, and reconciliation are all per-merchant
// — so partitioning this way keeps the common query inside one shard. The
// alternatives were rejected: user_id splits a merchant's own ledger across
// shards, and transaction_id gives even distribution but makes every
// merchant-level aggregate a scatter-gather.
//
// FNV-1a is used because it is in the standard library and its output is fixed
// forever; a hash whose results could change between Go releases would silently
// relocate existing merchants.
func KeyForMerchant(merchantID string) string {
	h := fnv.New64a()
	// Hash.Write never returns an error, per the hash.Hash contract.
	_, _ = h.Write([]byte(merchantID))
	return fmt.Sprintf("%s%0*d", shardKeyPrefix, shardKeyDigits, h.Sum64()%LogicalShards)
}
