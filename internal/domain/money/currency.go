package money

import "fmt"

// Currency is an ISO-4217 alphabetic code, always uppercase.
type Currency string

// Validate reports whether the code is three uppercase ASCII letters.
//
// The check is structural rather than a lookup against the full ISO-4217 list:
// currencies are added and redenominated over time, and a hardcoded allowlist
// becomes a source of spurious rejections. Whether a given currency is
// *supported* is a routing and pricing question answered elsewhere.
func (c Currency) Validate() error {
	if len(c) != 3 {
		return fmt.Errorf("%w: %q is not three characters", ErrInvalidCurrency, string(c))
	}
	for i := 0; i < len(c); i++ {
		if ch := c[i]; ch < 'A' || ch > 'Z' {
			return fmt.Errorf("%w: %q must be uppercase A-Z", ErrInvalidCurrency, string(c))
		}
	}
	return nil
}

func (c Currency) String() string { return string(c) }

// exponents records currencies whose minor unit is not the usual 1/100.
//
// Getting this wrong is a display bug with real consequences: rendering a
// 50,000 VND charge as "500.00" understates it by two orders of magnitude.
//
// Arithmetic *within* a currency never consults this table — minor units are
// minor units, and adding two USD amounts does not care how they are printed.
// Arithmetic *between* currencies does: converting 100.00 USD to JPY has to
// account for two decimal places becoming none, and a conversion that skips it
// is wrong by a factor of a hundred. See fx.Rate.Convert, which is the only
// place that difference is applied.
//
// An unlisted currency is therefore safe for storage and same-currency maths,
// and is treated as two-decimal everywhere else.
var exponents = map[Currency]int{
	"BHD": 3, // Bahraini dinar
	"CLP": 0, // Chilean peso
	"IDR": 0, // Indonesian rupiah — commonly quoted without minor units
	"ISK": 0, // Icelandic krona
	"JOD": 3, // Jordanian dinar
	"JPY": 0, // Japanese yen
	"KRW": 0, // South Korean won
	"KWD": 3, // Kuwaiti dinar
	"OMR": 3, // Omani rial
	"TND": 3, // Tunisian dinar
	"VND": 0, // Vietnamese dong
}

const defaultExponent = 2

// Exponent returns the number of decimal places in this currency's minor unit.
// Used only for display; see the note on exponents.
func (c Currency) Exponent() int {
	if e, ok := exponents[c]; ok {
		return e
	}
	return defaultExponent
}
