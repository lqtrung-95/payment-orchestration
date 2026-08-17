// Package auth issues and verifies the API keys that identify a merchant.
//
// Before this existed the merchant was an unauthenticated header: any caller
// could claim to be anyone, and the tenant isolation enforced everywhere else
// in the system was only as real as a string somebody typed. That made every
// other guarantee conditional.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
)

const (
	// Prefix marks a string as one of our keys, so a leaked value is
	// recognisable in a log or a paste and can be revoked without first
	// working out what it is.
	Prefix = "pmt"

	// publicLen is how many bytes of the key are stored in clear for lookup.
	// Enough to be unique in practice, and useless on its own.
	publicLen = 6

	// secretLen is the part that is hashed. 32 bytes from crypto/rand puts
	// this well beyond guessing, which is what lets the stored digest be a
	// plain SHA-256 rather than a deliberately slow hash.
	secretLen = 32
)

var (
	// ErrMalformedKey means the presented string is not shaped like a key.
	// Reported the same way as a wrong key at the API boundary: telling a
	// caller which of the two it was is free information about the format.
	ErrMalformedKey = errors.New("malformed api key")

	// ErrKeyNotFound covers both an unknown prefix and a revoked key.
	ErrKeyNotFound = errors.New("api key not recognised")
)

// Key is a newly minted credential. The secret exists only here and in the
// response to whoever asked for it — it is never stored and cannot be recovered.
type Key struct {
	// Public identifies the row. Safe to log.
	Public string

	// Secret is the whole key, the thing a caller presents. Shown once.
	Secret string

	// Hash is what gets stored.
	Hash []byte
}

// Generate mints a key.
//
// The returned Secret is the only copy. That is the point of hashing rather
// than encrypting: a stolen database yields digests, and a digest cannot be
// presented to this API. The cost is that a lost key is regenerated rather than
// looked up, which is the correct trade for a credential that moves money.
func Generate() (Key, error) {
	publicBytes := make([]byte, publicLen)
	if _, err := rand.Read(publicBytes); err != nil {
		return Key{}, fmt.Errorf("generate key prefix: %w", err)
	}
	secretBytes := make([]byte, secretLen)
	if _, err := rand.Read(secretBytes); err != nil {
		return Key{}, fmt.Errorf("generate key secret: %w", err)
	}

	public := encode(publicBytes)
	secret := fmt.Sprintf("%s_%s_%s", Prefix, public, encode(secretBytes))

	return Key{Public: public, Secret: secret, Hash: Digest(secret)}, nil
}

// Digest hashes a whole presented key.
//
// SHA-256 because the input is high-entropy and machine-generated. A password
// hash would add milliseconds to every request in exchange for resistance to an
// attack — dictionary search — that does not apply to 32 random bytes.
func Digest(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// PublicPart extracts the lookup handle from a presented key without trusting
// any of it. Nothing is compared until the row it names has been found.
func PublicPart(presented string) (string, error) {
	parts := strings.Split(presented, "_")
	if len(parts) != 3 || parts[0] != Prefix || parts[1] == "" || parts[2] == "" {
		return "", ErrMalformedKey
	}
	return parts[1], nil
}

// Redact renders a key for display, showing only the public half.
//
// Used wherever a key might otherwise reach a log line. The secret is never
// printed after the moment it is issued.
func Redact(public string) string {
	return fmt.Sprintf("%s_%s_%s", Prefix, public, strings.Repeat("*", 8))
}

// encode produces lowercase base32 with no padding.
//
// Not base64url, which was the first attempt: its alphabet contains the
// underscore this format uses as a delimiter, so roughly one generated key in
// three came out with an extra separator inside its secret and would not parse.
// A tighter alphabet is worth more than the shorter string — base32 excludes
// both `_` and `-`, so the three parts can never be ambiguous, and the result
// still survives a header, an environment variable, and an unquoted shell
// argument.
func encode(b []byte) string {
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}
