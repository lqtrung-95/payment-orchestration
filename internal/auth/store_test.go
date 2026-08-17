package auth_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lequoctrung/payment-orchestrator/internal/auth"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/testsupport"
)

func newStore(t *testing.T) (*auth.Store, *postgres.DB) {
	t.Helper()

	db := testsupport.NewDB(t)
	if _, err := db.Pool().Exec(context.Background(), "TRUNCATE api_keys"); err != nil {
		t.Fatalf("truncate api_keys: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Pool().Exec(context.Background(), "TRUNCATE api_keys")
	})
	return auth.NewStore(), db
}

func issue(t *testing.T, store *auth.Store, db *postgres.DB, merchant string) auth.Key {
	t.Helper()

	key, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Issue(context.Background(), db.Pool(), merchant, "test", key); err != nil {
		t.Fatalf("issue: %v", err)
	}
	return key
}

func TestIssuedKeyAuthenticatesItsOwnMerchant(t *testing.T) {
	store, db := newStore(t)
	key := issue(t, store, db, "merchant-a")

	record, err := store.Verify(context.Background(), db, key.Secret)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if record.MerchantID != "merchant-a" {
		t.Errorf("authenticated as %q, want merchant-a", record.MerchantID)
	}
}

// The property the whole scheme rests on: the secret is not recoverable from
// the database. Checked against every text column rather than the one we expect
// it in, because a leak through a name or a label counts just as much.
func TestTheSecretIsNotStoredAnywhere(t *testing.T) {
	store, db := newStore(t)
	key := issue(t, store, db, "merchant-a")

	secretHalf := strings.Split(key.Secret, "_")[2]

	var matches int
	err := db.Pool().QueryRow(context.Background(), `
		SELECT count(*) FROM api_keys
		WHERE key_prefix LIKE '%' || $1 || '%'
		   OR name LIKE '%' || $1 || '%'
		   OR merchant_id LIKE '%' || $1 || '%'
		   OR encode(key_hash, 'escape') LIKE '%' || $1 || '%'`, secretHalf).Scan(&matches)
	if err != nil {
		t.Fatalf("scan for the secret: %v", err)
	}
	if matches != 0 {
		t.Error("the secret is recoverable from the stored row")
	}
}

// A guess with the right public half and the wrong secret must fail. This is
// the case a lookup-only implementation would wave through, and the reason the
// digest is compared at all.
func TestRightPrefixWithWrongSecretIsRefused(t *testing.T) {
	store, db := newStore(t)
	key := issue(t, store, db, "merchant-a")

	other, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}
	forged := auth.Prefix + "_" + key.Public + "_" + strings.Split(other.Secret, "_")[2]

	if _, err := store.Verify(context.Background(), db, forged); !errors.Is(err, auth.ErrKeyNotFound) {
		t.Errorf("a forged key was accepted or misreported: %v", err)
	}
}

func TestRevokedKeyStopsWorkingAndStaysRevoked(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	key := issue(t, store, db, "merchant-a")

	if _, err := store.Verify(ctx, db, key.Secret); err != nil {
		t.Fatalf("key should work before revocation: %v", err)
	}

	revoked, err := store.Revoke(ctx, db.Pool(), key.Public)
	if err != nil {
		t.Fatal(err)
	}
	if !revoked {
		t.Fatal("revoke reported no change")
	}

	if _, err := store.Verify(ctx, db, key.Secret); !errors.Is(err, auth.ErrKeyNotFound) {
		t.Errorf("a revoked key still authenticates: %v", err)
	}

	// Revoking again must report no change rather than a second success, so an
	// operator is never told they disabled something they did not.
	again, err := store.Revoke(ctx, db.Pool(), key.Public)
	if err != nil {
		t.Fatal(err)
	}
	if again {
		t.Error("revoking an already-revoked key reported a change")
	}

	// The row survives revocation: it is the record of which credential acted.
	var count int
	if err := db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM api_keys WHERE key_prefix = $1`, key.Public).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Error("revocation deleted the row instead of disabling it")
	}
}

func TestUnknownAndMalformedKeysAreRefused(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()
	issue(t, store, db, "merchant-a")

	unknown, err := auth.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Verify(ctx, db, unknown.Secret); !errors.Is(err, auth.ErrKeyNotFound) {
		t.Errorf("an unissued key was accepted: %v", err)
	}

	for _, bad := range []string{"", "garbage", "pmt_only_", "Bearer something"} {
		if _, err := store.Verify(ctx, db, bad); err == nil {
			t.Errorf("Verify(%q) succeeded", bad)
		}
	}
}

// One merchant's key must never resolve to another's, which is the entire point
// of tenant isolation now that the merchant is no longer caller-supplied.
func TestKeysDoNotCrossMerchants(t *testing.T) {
	store, db := newStore(t)
	ctx := context.Background()

	a := issue(t, store, db, "merchant-a")
	b := issue(t, store, db, "merchant-b")

	for _, tc := range []struct {
		key  auth.Key
		want string
	}{{a, "merchant-a"}, {b, "merchant-b"}} {
		record, err := store.Verify(ctx, db, tc.key.Secret)
		if err != nil {
			t.Fatalf("verify %s: %v", tc.want, err)
		}
		if record.MerchantID != tc.want {
			t.Errorf("key resolved to %q, want %q", record.MerchantID, tc.want)
		}
	}
}
