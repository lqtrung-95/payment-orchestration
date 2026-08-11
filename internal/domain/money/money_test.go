package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestNewRejectsMalformedCurrency(t *testing.T) {
	for _, code := range []Currency{"", "US", "USDD", "usd", "U5D", "US "} {
		if _, err := New(100, code); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("New(100, %q) error = %v, want ErrInvalidCurrency", code, err)
		}
	}
}

func TestAddSubRejectCurrencyMismatch(t *testing.T) {
	usd := MustNew(100, "USD")
	eur := MustNew(100, "EUR")

	if _, err := usd.Add(eur); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add across currencies error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := usd.Sub(eur); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub across currencies error = %v, want ErrCurrencyMismatch", err)
	}
}

// The zero value carries no currency. Arithmetic on it must fail loudly rather
// than behaving as though it were zero dollars, which would let an
// uninitialised struct field silently participate in a balance calculation.
func TestZeroValueIsNotUsable(t *testing.T) {
	var uninitialised Money

	if uninitialised.IsValid() {
		t.Error("zero-value Money reports IsValid() = true")
	}
	if _, err := uninitialised.Add(MustNew(100, "USD")); err == nil {
		t.Error("Add on zero-value Money succeeded, want error")
	}
	if _, err := uninitialised.Allocate(1, 1); err == nil {
		t.Error("Allocate on zero-value Money succeeded, want error")
	}
}

func TestAddDetectsOverflow(t *testing.T) {
	max := MustNew(math.MaxInt64, "USD")
	if _, err := max.Add(MustNew(1, "USD")); !errors.Is(err, ErrOverflow) {
		t.Errorf("MaxInt64 + 1 error = %v, want ErrOverflow", err)
	}

	min := MustNew(math.MinInt64, "USD")
	if _, err := min.Add(MustNew(-1, "USD")); !errors.Is(err, ErrOverflow) {
		t.Errorf("MinInt64 + -1 error = %v, want ErrOverflow", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("Neg(MinInt64) error = %v, want ErrOverflow", err)
	}
}

func TestMulDetectsOverflow(t *testing.T) {
	m := MustNew(math.MaxInt64/2+1, "USD")
	if _, err := m.Mul(2); !errors.Is(err, ErrOverflow) {
		t.Errorf("Mul overflow error = %v, want ErrOverflow", err)
	}

	got, err := MustNew(250, "USD").Mul(4)
	if err != nil {
		t.Fatalf("Mul returned error: %v", err)
	}
	if want := MustNew(1000, "USD"); !got.Equal(want) {
		t.Errorf("250 * 4 = %v, want %v", got, want)
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		money Money
		want  string
	}{
		{MustNew(1050, "USD"), "10.50 USD"},
		{MustNew(5, "USD"), "0.05 USD"},
		{MustNew(0, "USD"), "0.00 USD"},
		{MustNew(-1050, "USD"), "-10.50 USD"},
		// Zero-exponent currency: the amount is already whole units.
		{MustNew(50000, "VND"), "50000 VND"},
		{MustNew(1000, "JPY"), "1000 JPY"},
		// Three-decimal currency.
		{MustNew(1234, "KWD"), "1.234 KWD"},
	}

	for _, tt := range tests {
		if got := tt.money.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	original := MustNew(1050, "USD")

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if want := `{"amount":1050,"currency":"USD"}`; string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var decoded Money
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}
	if !decoded.Equal(original) {
		t.Errorf("round trip = %v, want %v", decoded, original)
	}
}

// A client sending a decimal amount has a bug. Failing at the boundary makes
// that visible immediately, rather than truncating to a value that only shows
// up as a one-minor-unit reconciliation break much later.
func TestUnmarshalRejectsNonInteger(t *testing.T) {
	for _, payload := range []string{
		`{"amount":10.50,"currency":"USD"}`,
		`{"amount":"1050","currency":"USD"}`,
		`{"amount":1050,"currency":"usd"}`,
		`{"amount":1050}`,
		`{"amount":1050,"currency":"USD","extra":true}`,
	} {
		var m Money
		if err := json.Unmarshal([]byte(payload), &m); err == nil {
			t.Errorf("Unmarshal(%s) succeeded, want error", payload)
		}
	}
}
