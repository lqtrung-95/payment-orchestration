package money

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// wireMoney is the JSON representation: integer minor units plus a currency
// code. Amounts cross the API boundary as integers so that no client, and no
// intermediate proxy or logging pipeline, ever parses them as a float.
type wireMoney struct {
	Amount   int64    `json:"amount"`
	Currency Currency `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.currency.Validate(); err != nil {
		return nil, fmt.Errorf("marshal money: %w", err)
	}
	return json.Marshal(wireMoney{Amount: m.amount, Currency: m.currency})
}

// UnmarshalJSON rejects anything that is not an integer minor-unit amount.
// A decimal such as 10.50 fails here rather than being silently truncated,
// which surfaces a client integration bug at the boundary instead of as a
// one-cent reconciliation break weeks later.
func (m *Money) UnmarshalJSON(data []byte) error {
	var w wireMoney
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return fmt.Errorf("unmarshal money: %w", err)
	}
	if err := w.Currency.Validate(); err != nil {
		return fmt.Errorf("unmarshal money: %w", err)
	}
	m.amount = w.Amount
	m.currency = w.Currency
	return nil
}
