package fx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/domain/fx"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
)

func rate(t *testing.T, base, quote money.Currency, nano int64) fx.Rate {
	t.Helper()

	r, err := fx.NewRate(base, quote, nano, "test", time.Unix(0, 0))
	if err != nil {
		t.Fatalf("NewRate(%s/%s, %d): %v", base, quote, nano, err)
	}
	return r
}

func TestConvertBetweenTwoDecimalCurrencies(t *testing.T) {
	// 100.00 EUR at 1.085 is 108.50 USD.
	got, err := rate(t, "EUR", "USD", 1_085_000_000).Convert(money.MustNew(10000, "EUR"))
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if got.Amount() != 10850 || got.Currency() != "USD" {
		t.Errorf("Convert = %d %s, want 10850 USD", got.Amount(), got.Currency())
	}
}

// The case that makes the exponent part of the arithmetic rather than a display
// concern. USD has two decimal places and JPY has none, so ignoring the
// difference is not a rounding error — it is wrong by a factor of a hundred.
func TestConvertAcrossDifferentCurrencyExponents(t *testing.T) {
	cases := []struct {
		name       string
		rate       fx.Rate
		amount     money.Money
		wantMinor  int64
		wantSymbol money.Currency
	}{
		{
			// 100.00 USD × 157.20 = 15,720 JPY, and JPY has no minor unit.
			name:       "two decimals to zero decimals",
			rate:       rate(t, "USD", "JPY", 157_200_000_000),
			amount:     money.MustNew(10000, "USD"),
			wantMinor:  15720,
			wantSymbol: "JPY",
		},
		{
			// 15,720 JPY back to USD at the reciprocal is 100.00 USD.
			name:       "zero decimals to two decimals",
			rate:       rate(t, "JPY", "USD", 6_361_323),
			amount:     money.MustNew(15720, "JPY"),
			wantMinor:  10000,
			wantSymbol: "USD",
		},
		{
			// 10.00 USD × 25,400 = 254,000 VND, also a zero-decimal currency.
			name:       "large zero-decimal quote",
			rate:       rate(t, "USD", "VND", 25_400_000_000_000),
			amount:     money.MustNew(1000, "USD"),
			wantMinor:  254000,
			wantSymbol: "VND",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.rate.Convert(tc.amount)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if got.Amount() != tc.wantMinor || got.Currency() != tc.wantSymbol {
				t.Errorf("Convert(%s) = %d %s, want %d %s",
					tc.amount, got.Amount(), got.Currency(), tc.wantMinor, tc.wantSymbol)
			}
		})
	}
}

// Half-up is biased: it rounds every exact tie in the same direction, so over
// many conversions the error accumulates and shows up as systematic drift
// against a settlement file. Half-even splits the ties.
func TestExactTiesRoundToEven(t *testing.T) {
	// A rate of exactly 0.5 puts every odd minor unit on an exact tie.
	half := rate(t, "EUR", "USD", 500_000_000)

	cases := []struct {
		in   int64
		want int64 // .5 rounds to the even neighbour
	}{
		{1, 0},  // 0.5 -> 0
		{3, 2},  // 1.5 -> 2
		{5, 2},  // 2.5 -> 2
		{7, 4},  // 3.5 -> 4
		{9, 4},  // 4.5 -> 4
		{11, 6}, // 5.5 -> 6
	}

	for _, tc := range cases {
		got, err := half.Convert(money.MustNew(tc.in, "EUR"))
		if err != nil {
			t.Fatalf("Convert(%d): %v", tc.in, err)
		}
		if got.Amount() != tc.want {
			t.Errorf("Convert(%d) = %d, want %d (half to even)", tc.in, got.Amount(), tc.want)
		}
	}
}

// Over a run of ties, half-even must not drift in either direction the way
// half-up would.
func TestTieRoundingIsUnbiasedAcrossManyConversions(t *testing.T) {
	half := rate(t, "EUR", "USD", 500_000_000)

	var converted, exactDoubled int64
	for i := int64(1); i <= 2000; i += 2 { // every odd input lands on a tie
		got, err := half.Convert(money.MustNew(i, "EUR"))
		if err != nil {
			t.Fatalf("Convert(%d): %v", i, err)
		}
		converted += got.Amount()
		exactDoubled += i // exact value is i/2, doubled to stay in integers
	}

	// Sum of exact values, doubled, versus the doubled sum of rounded values.
	drift := converted*2 - exactDoubled
	if drift != 0 {
		t.Errorf("rounding drift over 1000 ties = %d, want 0 — the rounding mode is biased", drift)
	}
}

func TestConvertRejectsAMismatchedCurrency(t *testing.T) {
	_, err := rate(t, "EUR", "USD", 1_085_000_000).Convert(money.MustNew(100, "GBP"))
	if !errors.Is(err, fx.ErrCurrencyMismatch) {
		t.Errorf("Convert with the wrong currency = %v, want ErrCurrencyMismatch", err)
	}
}

// A conversion whose result cannot be represented must fail loudly rather than
// wrapping into a plausible-looking small number.
func TestConvertDetectsOverflow(t *testing.T) {
	huge := rate(t, "USD", "VND", 25_400_000_000_000)
	_, err := huge.Convert(money.MustNew(1<<62, "USD"))
	if !errors.Is(err, money.ErrRoundingOverflow) {
		t.Errorf("Convert of an enormous amount = %v, want ErrRoundingOverflow", err)
	}
}

// Inverting and converting back should land on the original amount for values
// where the rate divides cleanly. This is what catches a reciprocal that was
// rounded twice.
func TestRoundTripThroughTheInverseRate(t *testing.T) {
	forward := rate(t, "EUR", "USD", 1_250_000_000) // 1.25, exactly invertible
	back, err := forward.Invert()
	if err != nil {
		t.Fatalf("Invert: %v", err)
	}
	if back.Base != "USD" || back.Quote != "EUR" {
		t.Fatalf("Invert produced %s/%s, want USD/EUR", back.Base, back.Quote)
	}

	original := money.MustNew(80000, "EUR") // 800.00
	usd, err := forward.Convert(original)
	if err != nil {
		t.Fatalf("Convert forward: %v", err)
	}
	eur, err := back.Convert(usd)
	if err != nil {
		t.Fatalf("Convert back: %v", err)
	}
	if eur.Amount() != original.Amount() {
		t.Errorf("round trip = %d, want %d", eur.Amount(), original.Amount())
	}
}

// The simulator must be reproducible: a reconciliation run over historical data
// has to give the same answer today as next week.
func TestSimulatedRatesAreDeterministicAndMove(t *testing.T) {
	p := fx.NewSimulatedProvider(200)
	ctx := context.Background()
	at := time.Date(2026, 3, 4, 9, 30, 0, 0, time.UTC)

	first, err := p.Quote(ctx, "EUR", "USD", at)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	again, err := p.Quote(ctx, "EUR", "USD", at)
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if first.Nano != again.Nano {
		t.Errorf("same instant produced %d then %d — rates are not reproducible", first.Nano, again.Nano)
	}

	// And it has to actually move, or fx_drift breaks could never be produced.
	later, err := p.Quote(ctx, "EUR", "USD", at.Add(6*time.Hour))
	if err != nil {
		t.Fatalf("Quote: %v", err)
	}
	if later.Nano == first.Nano {
		t.Error("rate did not move over six hours; settlement drift would be impossible to simulate")
	}
}

// An unconfigured pair is an error. A fabricated rate is worse than a failure:
// it produces a plausible number nobody can trace.
func TestUnknownPairIsRefused(t *testing.T) {
	_, err := fx.NewSimulatedProvider(100).Quote(context.Background(), "EUR", "CHF", time.Now())
	if !errors.Is(err, fx.ErrUnknownPair) {
		t.Errorf("Quote for an unconfigured pair = %v, want ErrUnknownPair", err)
	}
}

func TestExpiredLockRefusesToConvert(t *testing.T) {
	now := time.Now().UTC()
	lock, err := fx.NewLock(uuidFor(t), rate(t, "EUR", "USD", 1_085_000_000), now, time.Minute)
	if err != nil {
		t.Fatalf("NewLock: %v", err)
	}

	if _, err := lock.Convert(money.MustNew(10000, "EUR"), now.Add(30*time.Second)); err != nil {
		t.Fatalf("Convert inside the window: %v", err)
	}

	// Dead at the boundary, not one nanosecond after it.
	if _, err := lock.Convert(money.MustNew(10000, "EUR"), lock.ExpiresAt); !errors.Is(err, fx.ErrLockExpired) {
		t.Errorf("Convert at the expiry instant = %v, want ErrLockExpired", err)
	}
	if _, err := lock.Convert(money.MustNew(10000, "EUR"), now.Add(2*time.Minute)); !errors.Is(err, fx.ErrLockExpired) {
		t.Errorf("Convert after expiry = %v, want ErrLockExpired", err)
	}
}

func uuidFor(t *testing.T) uuid.UUID {
	t.Helper()
	return uuid.New()
}
