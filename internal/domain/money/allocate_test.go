package money

import (
	"errors"
	"math/rand"
	"testing"
)

func TestAllocateDistributesRemainder(t *testing.T) {
	tests := []struct {
		name   string
		amount int64
		ratios []int64
		want   []int64
	}{
		{"exact split", 100, []int64{1, 1}, []int64{50, 50}},
		// 100/3 truncates to 33 each, leaving 1 to distribute to the first part.
		{"indivisible thirds", 100, []int64{1, 1, 1}, []int64{34, 33, 33}},
		{"weighted", 100, []int64{3, 1}, []int64{75, 25}},
		{"weighted with remainder", 10, []int64{3, 1}, []int64{8, 2}},
		{"single part takes all", 999, []int64{7}, []int64{999}},
		{"zero ratio gets nothing", 100, []int64{1, 0, 1}, []int64{50, 0, 50}},
		{"zero amount", 0, []int64{1, 1, 1}, []int64{0, 0, 0}},
		// Negative amounts arise from refunds and reversals; the remainder is
		// distributed in the same direction as the amount.
		{"negative indivisible", -100, []int64{1, 1, 1}, []int64{-34, -33, -33}},
		{"negative weighted", -10, []int64{3, 1}, []int64{-8, -2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts, err := MustNew(tt.amount, "USD").Allocate(tt.ratios...)
			if err != nil {
				t.Fatalf("Allocate returned error: %v", err)
			}
			if len(parts) != len(tt.want) {
				t.Fatalf("got %d parts, want %d", len(parts), len(tt.want))
			}
			for i, want := range tt.want {
				if parts[i].Amount() != want {
					t.Errorf("part[%d] = %d, want %d", i, parts[i].Amount(), want)
				}
			}
		})
	}
}

func TestAllocateRejectsInvalidRatios(t *testing.T) {
	m := MustNew(100, "USD")

	if _, err := m.Allocate(); !errors.Is(err, ErrEmptyAllocation) {
		t.Errorf("Allocate() error = %v, want ErrEmptyAllocation", err)
	}
	if _, err := m.Allocate(1, -1); !errors.Is(err, ErrNegativeRatio) {
		t.Errorf("Allocate(1, -1) error = %v, want ErrNegativeRatio", err)
	}
	if _, err := m.Allocate(0, 0); !errors.Is(err, ErrZeroRatioTotal) {
		t.Errorf("Allocate(0, 0) error = %v, want ErrZeroRatioTotal", err)
	}
	if _, err := m.Split(0); !errors.Is(err, ErrEmptyAllocation) {
		t.Errorf("Split(0) error = %v, want ErrEmptyAllocation", err)
	}
}

// The property that matters: an allocation neither creates nor destroys a
// minor unit, for any amount and any ratios. This is the invariant that keeps
// fee splits from drifting the ledger out of balance one cent at a time.
func TestAllocateConservesTotal(t *testing.T) {
	rng := rand.New(rand.NewSource(20260811))

	for i := 0; i < 20000; i++ {
		// Bounded well away from the int64 limits so that the property under
		// test is conservation, not overflow handling, which is covered
		// separately.
		amount := rng.Int63n(2_000_000_001) - 1_000_000_000

		n := rng.Intn(8) + 1
		ratios := make([]int64, n)
		total := int64(0)
		for j := range ratios {
			ratios[j] = rng.Int63n(1000)
			total += ratios[j]
		}
		if total == 0 {
			ratios[0] = 1
		}

		original := MustNew(amount, "USD")
		parts, err := original.Allocate(ratios...)
		if err != nil {
			t.Fatalf("Allocate(%d, %v) returned error: %v", amount, ratios, err)
		}

		sum, err := Sum(parts...)
		if err != nil {
			t.Fatalf("Sum returned error: %v", err)
		}
		if !sum.Equal(original) {
			t.Fatalf("allocation of %d by %v summed to %d", amount, ratios, sum.Amount())
		}
	}
}

func TestSplitConservesTotal(t *testing.T) {
	for _, amount := range []int64{0, 1, 7, 100, 101, -1, -7, -101, 999999999} {
		for n := 1; n <= 13; n++ {
			parts, err := MustNew(amount, "USD").Split(n)
			if err != nil {
				t.Fatalf("Split(%d) on %d returned error: %v", n, amount, err)
			}
			sum, err := Sum(parts...)
			if err != nil {
				t.Fatalf("Sum returned error: %v", err)
			}
			if sum.Amount() != amount {
				t.Errorf("Split(%d) of %d summed to %d", n, amount, sum.Amount())
			}
		}
	}
}

// Parts differ by at most one minor unit, so no single party absorbs the whole
// rounding error of a large split.
func TestSplitPartsDifferByAtMostOneUnit(t *testing.T) {
	parts, err := MustNew(100, "USD").Split(7)
	if err != nil {
		t.Fatalf("Split returned error: %v", err)
	}

	minPart, maxPart := parts[0].Amount(), parts[0].Amount()
	for _, p := range parts[1:] {
		if p.Amount() < minPart {
			minPart = p.Amount()
		}
		if p.Amount() > maxPart {
			maxPart = p.Amount()
		}
	}
	if maxPart-minPart > 1 {
		t.Errorf("parts span %d..%d, want a spread of at most 1", minPart, maxPart)
	}
}

func TestSumRejectsMixedCurrencies(t *testing.T) {
	if _, err := Sum(MustNew(1, "USD"), MustNew(1, "EUR")); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sum across currencies error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := Sum(); !errors.Is(err, ErrEmptyAllocation) {
		t.Errorf("Sum() error = %v, want ErrEmptyAllocation", err)
	}
}
