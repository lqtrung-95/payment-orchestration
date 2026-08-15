package sharding

import (
	"fmt"
	"strconv"
	"strings"
)

// Mapping assigns each of the 64 logical shards to one physical database.
//
// The indirection is the whole point. Rows carry a logical shard key that never
// changes; the number of physical databases behind those 64 slots is a
// deployment decision that can change without touching a single row's key.
// Hashing merchants straight onto physical databases would make adding capacity
// a rehash of the entire data set, taken while the service is live and money is
// moving.
type Mapping struct {
	physical int
}

// NewMapping builds a mapping onto n physical databases.
//
// n is constrained to a power of two that divides LogicalShards so that every
// physical database owns an equal, contiguous run of logical shards. That is
// what makes growing capacity tractable: doubling n splits each existing range
// exactly in half, so the migration is "copy logical shards 32–63 to the new
// database" — a contiguous bulk move that can be verified by counting rows per
// shard key. An arbitrary n produces uneven ranges whose boundaries shift for
// shards that were not meant to move.
func NewMapping(n int) (Mapping, error) {
	if n < 1 || n > LogicalShards {
		return Mapping{}, fmt.Errorf("physical shard count must be between 1 and %d, got %d", LogicalShards, n)
	}
	if n&(n-1) != 0 {
		return Mapping{}, fmt.Errorf("physical shard count must be a power of two, got %d", n)
	}
	return Mapping{physical: n}, nil
}

// Physical returns the number of physical databases in this mapping.
func (m Mapping) Physical() int { return m.physical }

// PhysicalFor resolves a logical shard index to a physical database index.
func (m Mapping) PhysicalFor(logical int) (int, error) {
	if logical < 0 || logical >= LogicalShards {
		return 0, fmt.Errorf("logical shard %d out of range [0,%d)", logical, LogicalShards)
	}
	return logical / (LogicalShards / m.physical), nil
}

// Resolve maps a stored shard key straight to a physical database index.
func (m Mapping) Resolve(shardKey string) (int, error) {
	logical, err := LogicalIndex(shardKey)
	if err != nil {
		return 0, err
	}
	return m.PhysicalFor(logical)
}

// LogicalRange returns the inclusive range of logical shards owned by a
// physical database. Used by operational tooling that has to report or move
// what lives where, and by the tests that assert the ranges are contiguous.
func (m Mapping) LogicalRange(physical int) (lo, hi int, err error) {
	if physical < 0 || physical >= m.physical {
		return 0, 0, fmt.Errorf("physical shard %d out of range [0,%d)", physical, m.physical)
	}
	width := LogicalShards / m.physical
	return physical * width, (physical+1)*width - 1, nil
}

// LogicalIndex parses a stored shard key back into its logical index.
//
// Parsing rather than re-hashing is deliberate: the key on the row is the
// authority. Re-deriving it from the merchant would let a change in the hash
// function silently route a row's reads to a different database than the one
// holding it.
func LogicalIndex(shardKey string) (int, error) {
	digits, ok := strings.CutPrefix(shardKey, shardKeyPrefix)
	if !ok || len(digits) != shardKeyDigits {
		return 0, fmt.Errorf("malformed shard key %q: want %s followed by %d digits", shardKey, shardKeyPrefix, shardKeyDigits)
	}
	n, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("malformed shard key %q: %w", shardKey, err)
	}
	if n < 0 || n >= LogicalShards {
		return 0, fmt.Errorf("shard key %q is outside [0,%d)", shardKey, LogicalShards)
	}
	return n, nil
}
