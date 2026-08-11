// Package idempotency stores and arbitrates idempotency keys.
package idempotency

import (
	"crypto/sha256"
	"encoding/json"
)

// Fingerprint digests the request so a key replayed with a different body can
// be detected.
//
// The JSON body is canonicalised before hashing: it is decoded and re-encoded,
// which normalises whitespace and orders object keys (Go's encoder sorts map
// keys). Without that, a client that merely re-serialised its request on retry
// would be told the body changed, and a legitimate retry would be refused.
//
// A body that is not valid JSON is hashed verbatim. It cannot be canonicalised,
// and hashing the raw bytes is the conservative choice: at worst it produces a
// spurious mismatch, never a false match that would let a different request
// replay another's response.
func Fingerprint(method, path string, body []byte) []byte {
	h := sha256.New()
	// Hash.Write never returns an error, per the hash.Hash contract.
	_, _ = h.Write([]byte(method))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(path))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(canonicalizeJSON(body))
	return h.Sum(nil)
}

func canonicalizeJSON(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}

	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return body
	}

	canonical, err := json.Marshal(parsed)
	if err != nil {
		return body
	}
	return canonical
}
