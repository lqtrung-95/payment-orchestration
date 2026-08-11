package middleware

import (
	"strings"
	"testing"
)

func TestSanitizeRequestID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty is rejected", "", ""},
		{"uuid passes through", "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d", "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"},
		{"opaque token passes through", "req_01HX8Z9ABCDEF", "req_01HX8Z9ABCDEF"},

		// A newline would let a caller forge additional log records, and a
		// header separator would let them inject a second response header.
		{"newline is rejected", "abc\ndef", ""},
		{"carriage return is rejected", "abc\r\ndef", ""},
		{"space is rejected", "abc def", ""},
		{"tab is rejected", "abc\tdef", ""},
		{"null byte is rejected", "abc\x00def", ""},
		{"non-ascii is rejected", "abcédef", ""},

		{"at length limit is accepted", strings.Repeat("a", maxInboundRequestIDLen), strings.Repeat("a", maxInboundRequestIDLen)},
		{"over length limit is rejected", strings.Repeat("a", maxInboundRequestIDLen+1), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeRequestID(tt.in); got != tt.want {
				t.Errorf("sanitizeRequestID(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
