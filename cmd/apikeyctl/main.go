// Command apikeyctl issues, lists, and revokes the API keys merchants
// authenticate with.
//
//	apikeyctl issue  -merchant M [-name N]   mint a key and print it once
//	apikeyctl list   [-merchant M]           show keys without their secrets
//	apikeyctl revoke -key pmt_xxx            disable a key
//
// There is no command that reads a key back. Only the digest is stored, so a
// lost key is replaced rather than recovered — which is the property that makes
// a stolen database useless against this API.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lequoctrung/payment-orchestrator/internal/auth"
	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
)

const usage = `usage: apikeyctl <command> [flags]

  issue   -merchant M [-name N]   mint a key for a merchant, printed once
  list    [-merchant M]           list keys, without their secrets
  revoke  -key pmt_xxx_...        disable a key
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "apikeyctl:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("a subcommand is required")
	}

	switch os.Args[1] {
	case "issue":
		return issue(os.Args[2:])
	case "list":
		return list(os.Args[2:])
	case "revoke":
		return revoke(os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

// deps opens the global shard. Keys live there because authentication happens
// before the merchant — and therefore the shard — is known.
func deps() (context.Context, *postgres.Router, *auth.Store, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, nil, err
	}
	ctx := context.Background()
	router, err := postgres.NewRouter(ctx, cfg.Postgres, cfg.Postgres.ShardDSNs)
	if err != nil {
		return nil, nil, nil, err
	}
	return ctx, router, auth.NewStore(), nil
}

func issue(args []string) error {
	fs := flag.NewFlagSet("issue", flag.ExitOnError)
	merchant := fs.String("merchant", "", "merchant the key authenticates as")
	name := fs.String("name", "", "label, so an operator can tell keys apart later")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *merchant == "" {
		return errors.New("-merchant is required")
	}

	key, err := auth.Generate()
	if err != nil {
		return err
	}

	ctx, router, store, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	record, err := store.Issue(ctx, router.Global().Pool(), *merchant, *name, key)
	if err != nil {
		return err
	}

	// Printed to stdout, alone, with the warning on stderr — so piping the
	// command somewhere useful does not also capture the commentary.
	fmt.Fprintf(os.Stderr, "issued for %s (id %s)\n", record.MerchantID, record.ID)
	fmt.Fprintln(os.Stderr, "this is the only time the key is shown; it is stored hashed")
	fmt.Println(key.Secret)
	return nil
}

func list(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	merchant := fs.String("merchant", "", "restrict to one merchant")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, router, store, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	records, err := store.List(ctx, router.Global().Pool(), *merchant)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "KEY\tMERCHANT\tNAME\tCREATED\tLAST USED\tSTATUS")
	for _, r := range records {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			auth.Redact(r.Public), r.MerchantID, orDash(r.Name),
			r.CreatedAt.Format(time.RFC3339), stamp(r.LastUsedAt), status(r))
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\n%d key(s)\n", len(records))
	return nil
}

func revoke(args []string) error {
	fs := flag.NewFlagSet("revoke", flag.ExitOnError)
	key := fs.String("key", "", "the key, or just its public part")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" {
		return errors.New("-key is required")
	}

	// Accepts a whole key or only its public half, because whoever is revoking
	// in a hurry has whichever one they have.
	public := *key
	if parsed, err := auth.PublicPart(*key); err == nil {
		public = parsed
	} else if strings.Contains(*key, "_") {
		return fmt.Errorf("%w: expected %s_<public>_<secret> or just the public part", err, auth.Prefix)
	}

	ctx, router, store, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	revoked, err := store.Revoke(ctx, router.Global().Pool(), public)
	if err != nil {
		return err
	}
	if !revoked {
		// Reported rather than treated as success: "already revoked" and "no
		// such key" both mean the thing you meant to disable may still work.
		return fmt.Errorf("no active key with public part %q", public)
	}

	fmt.Printf("revoked %s\n", auth.Redact(public))
	return nil
}

func status(r *auth.Record) string {
	if r.RevokedAt != nil {
		return "revoked " + r.RevokedAt.Format(time.RFC3339)
	}
	return "active"
}

func stamp(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.Format(time.RFC3339)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
