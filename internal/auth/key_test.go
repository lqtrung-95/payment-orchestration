package auth_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/lequoctrung/payment-orchestrator/internal/auth"
)

func TestGeneratedKeysAreDistinct(t *testing.T) {
	const n = 500

	secrets := make(map[string]bool, n)
	publics := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		key, err := auth.Generate()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if secrets[key.Secret] {
			t.Fatal("two generated keys collided")
		}
		if publics[key.Public] {
			// A collision here would be worse than a duplicate secret: the
			// public part is the unique lookup handle, so a repeat makes one
			// merchant's key unusable.
			t.Fatal("two generated keys share a public part")
		}
		secrets[key.Secret] = true
		publics[key.Public] = true
	}
}

// The stored digest must not contain the secret, in any form. This is the
// property that makes a stolen database useless against the API.
func TestStoredDigestDoesNotContainTheSecret(t *testing.T) {
	key, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(key.Hash, []byte(key.Secret)) {
		t.Error("the digest contains the secret verbatim")
	}
	// The secret's own bytes must not appear either — a digest that happened to
	// be a slice of the input would pass the check above.
	parts := strings.Split(key.Secret, "_")
	if bytes.Contains(key.Hash, []byte(parts[len(parts)-1])) {
		t.Error("the digest contains the secret's random half")
	}
	if len(key.Hash) != 32 {
		t.Errorf("digest is %d bytes, want 32", len(key.Hash))
	}
}

func TestDigestIsStableAndSecretSpecific(t *testing.T) {
	a, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(auth.Digest(a.Secret), a.Hash) {
		t.Error("digesting the same secret twice produced different bytes")
	}
	if bytes.Equal(auth.Digest(a.Secret), auth.Digest(b.Secret)) {
		t.Error("two different secrets produced the same digest")
	}
	// A single character difference must change the digest completely; this is
	// what stops a near-miss from being a near-match.
	altered := a.Secret[:len(a.Secret)-1] + "x"
	if bytes.Equal(auth.Digest(altered), a.Hash) {
		t.Error("changing the last character left the digest unchanged")
	}
}

func TestPublicPartParsesOurKeysAndRejectsEverythingElse(t *testing.T) {
	key, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}

	got, err := auth.PublicPart(key.Secret)
	if err != nil {
		t.Fatalf("parsing a generated key: %v", err)
	}
	if got != key.Public {
		t.Errorf("parsed %q, want %q", got, key.Public)
	}

	for _, bad := range []string{
		"",
		"pmt",
		"pmt_",
		"pmt_only-two-parts",
		"pmt__missingpublic",
		"pmt_public_",
		"wrong_public_secret",
		"pmt_public_secret_extra",
		key.Public,
	} {
		if _, err := auth.PublicPart(bad); !errors.Is(err, auth.ErrMalformedKey) {
			t.Errorf("PublicPart(%q) = %v, want ErrMalformedKey", bad, err)
		}
	}
}

// Redact is what stands between a key and a log line, so it must never emit the
// half that authenticates.
func TestRedactShowsOnlyThePublicHalf(t *testing.T) {
	key, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}

	redacted := auth.Redact(key.Public)
	if strings.Contains(redacted, key.Secret) {
		t.Fatal("the redacted form contains the whole key")
	}

	secretHalf := strings.Split(key.Secret, "_")[2]
	if strings.Contains(redacted, secretHalf) {
		t.Fatal("the redacted form contains the secret half")
	}
	if !strings.Contains(redacted, key.Public) {
		t.Error("the redacted form does not identify which key it is")
	}
}
