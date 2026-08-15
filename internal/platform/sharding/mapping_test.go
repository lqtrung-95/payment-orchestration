package sharding

import (
	"fmt"
	"testing"
)

func TestNewMappingRejectsCountsItCannotPartitionEvenly(t *testing.T) {
	for _, n := range []int{0, -1, 3, 5, 6, 12, 65, 128} {
		if _, err := NewMapping(n); err == nil {
			t.Errorf("NewMapping(%d) was accepted; it cannot own equal contiguous ranges", n)
		}
	}
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64} {
		if _, err := NewMapping(n); err != nil {
			t.Errorf("NewMapping(%d): %v", n, err)
		}
	}
}

// Every logical shard belongs to exactly one physical database, and every
// physical database owns a contiguous run of them. Contiguity is what makes a
// capacity change a bulk copy of a range rather than a row-by-row scatter.
func TestMappingPartitionsEveryLogicalShardExactlyOnce(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64} {
		m, err := NewMapping(n)
		if err != nil {
			t.Fatalf("NewMapping(%d): %v", n, err)
		}

		owner := make([]int, LogicalShards)
		for i := range owner {
			owner[i] = -1
		}

		for p := 0; p < n; p++ {
			lo, hi, err := m.LogicalRange(p)
			if err != nil {
				t.Fatalf("n=%d LogicalRange(%d): %v", n, p, err)
			}
			for l := lo; l <= hi; l++ {
				if owner[l] != -1 {
					t.Fatalf("n=%d: logical shard %d claimed by both %d and %d", n, l, owner[l], p)
				}
				owner[l] = p
			}
		}

		for l, p := range owner {
			if p == -1 {
				t.Fatalf("n=%d: logical shard %d belongs to no physical database", n, l)
			}
			got, err := m.PhysicalFor(l)
			if err != nil {
				t.Fatalf("n=%d PhysicalFor(%d): %v", n, l, err)
			}
			if got != p {
				t.Errorf("n=%d: logical %d resolves to %d but sits in range of %d", n, l, got, p)
			}
		}
	}
}

// The property the whole indirection exists for: doubling the physical count
// splits each existing range exactly in half. No logical shard ever lands on a
// database that was not already responsible for half of where it came from, so
// growing capacity moves a contiguous half and leaves the other half untouched.
func TestDoublingPhysicalCountSplitsEachRangeInHalf(t *testing.T) {
	for n := 1; n < LogicalShards; n *= 2 {
		before, err := NewMapping(n)
		if err != nil {
			t.Fatalf("NewMapping(%d): %v", n, err)
		}
		after, err := NewMapping(n * 2)
		if err != nil {
			t.Fatalf("NewMapping(%d): %v", n*2, err)
		}

		for l := 0; l < LogicalShards; l++ {
			was, err := before.PhysicalFor(l)
			if err != nil {
				t.Fatal(err)
			}
			now, err := after.PhysicalFor(l)
			if err != nil {
				t.Fatal(err)
			}
			if now/2 != was {
				t.Fatalf("n=%d->%d: logical %d moved from physical %d to %d, which is not a split of its old range",
					n, n*2, l, was, now)
			}
		}
	}
}

// A merchant's stored key must not depend on how many databases are deployed.
// If it did, adding a database would rewrite every row's key while the service
// was live — which is exactly the migration the logical layer exists to avoid.
func TestShardKeyIsIndependentOfPhysicalCount(t *testing.T) {
	merchants := merchantPopulation(500)

	for _, merchant := range merchants {
		key := KeyForMerchant(merchant)
		if again := KeyForMerchant(merchant); again != key {
			t.Fatalf("%s: key changed between calls: %s then %s", merchant, key, again)
		}

		logical, err := LogicalIndex(key)
		if err != nil {
			t.Fatalf("%s: key %q does not parse: %v", merchant, key, err)
		}

		for _, n := range []int{1, 2, 4, 8, 16, 32, 64} {
			m, err := NewMapping(n)
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := m.Resolve(key)
			if err != nil {
				t.Fatalf("%s: resolve under n=%d: %v", merchant, n, err)
			}
			expected, err := m.PhysicalFor(logical)
			if err != nil {
				t.Fatal(err)
			}
			if resolved != expected {
				t.Errorf("%s: n=%d resolved to %d, want %d", merchant, n, resolved, expected)
			}
		}
	}
}

// A hash that piles most merchants onto a few shards would make the sharding
// decorative: one database would carry the load while the others idled. The
// bound is loose on purpose — this is a smoke test for a broken hash, not a
// claim about uniformity.
func TestMerchantsSpreadAcrossAllLogicalShards(t *testing.T) {
	const merchants = 5000

	counts := make([]int, LogicalShards)
	for _, merchant := range merchantPopulation(merchants) {
		logical, err := LogicalIndex(KeyForMerchant(merchant))
		if err != nil {
			t.Fatal(err)
		}
		counts[logical]++
	}

	perfect := merchants / LogicalShards
	for shard, got := range counts {
		if got == 0 {
			t.Errorf("logical shard %d received no merchants", shard)
		}
		if got > perfect*3 {
			t.Errorf("logical shard %d took %d merchants, more than triple the even share of %d",
				shard, got, perfect)
		}
	}
}

func TestLogicalIndexRejectsMalformedKeys(t *testing.T) {
	// "s64" and above matter most: they parse cleanly but name a shard that
	// does not exist, so accepting one would route to a pool index out of range.
	for _, key := range []string{"", "s", "0", "00", "x00", "s0", "s001", "sxx", "s64", "s99", "S00"} {
		if _, err := LogicalIndex(key); err == nil {
			t.Errorf("LogicalIndex(%q) was accepted", key)
		}
	}

	for l := 0; l < LogicalShards; l++ {
		key := fmt.Sprintf("s%02d", l)
		got, err := LogicalIndex(key)
		if err != nil {
			t.Fatalf("LogicalIndex(%q): %v", key, err)
		}
		if got != l {
			t.Errorf("LogicalIndex(%q) = %d, want %d", key, got, l)
		}
	}
}

func TestPhysicalForRejectsOutOfRangeLogicalShards(t *testing.T) {
	m, err := NewMapping(4)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []int{-1, LogicalShards, LogicalShards + 1} {
		if _, err := m.PhysicalFor(l); err == nil {
			t.Errorf("PhysicalFor(%d) was accepted", l)
		}
	}
	for _, p := range []int{-1, 4, 64} {
		if _, _, err := m.LogicalRange(p); err == nil {
			t.Errorf("LogicalRange(%d) was accepted", p)
		}
	}
}

func merchantPopulation(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("merchant-%d", i))
	}
	return out
}
