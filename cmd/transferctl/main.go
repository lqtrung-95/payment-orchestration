// Command transferctl moves money between merchants and inspects transfers in
// flight.
//
// A transfer whose two merchants live on different databases has no single
// transaction available to it, so it runs as try-confirm-cancel. Every
// subcommand here talks to the same coordinator the service runs; `sweep` in
// particular is the manual form of the loop that resolves transfers whose
// coordinator died, and it is safe to run alongside it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/domain/money"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/sharding"
	"github.com/lequoctrung/payment-orchestrator/internal/tcc"
)

const usage = `usage: transferctl <command> [flags]

  send    -from M -to M -amount MINOR -currency C -key K   move funds between merchants
  show    -id ID                                           report a transfer's state
  shard   -merchant M [-quiet]                             report which database a merchant is on
  pending                                                  list transfers not yet resolved
  sweep                                                    resolve transfers whose coordinator stopped
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "transferctl:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("a subcommand is required")
	}

	switch os.Args[1] {
	case "send":
		return send(os.Args[2:])
	case "show":
		return show(os.Args[2:])
	case "shard":
		return shardOf(os.Args[2:])
	case "pending":
		return pending()
	case "sweep":
		return sweep()
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

// deps opens the router the coordinator routes through. Every command needs it,
// including the read-only ones: a transfer's two halves live on two databases
// and neither can be reported from the other.
func deps() (context.Context, *postgres.Router, *tcc.Coordinator, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, nil, err
	}

	ctx := context.Background()
	router, err := postgres.NewRouter(ctx, cfg.Postgres, cfg.Postgres.ShardDSNs)
	if err != nil {
		return nil, nil, nil, err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return ctx, router, tcc.NewCoordinator(router, tcc.DefaultConfig(), logger), nil
}

func send(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	from := fs.String("from", "", "source merchant")
	to := fs.String("to", "", "destination merchant")
	amount := fs.Int64("amount", 0, "amount in minor units")
	currency := fs.String("currency", "USD", "ISO currency code")
	key := fs.String("key", "", "idempotency key identifying this intent")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *from == "" || *to == "" || *amount <= 0 || *key == "" {
		return errors.New("-from, -to, -amount and -key are required")
	}

	value, err := money.New(*amount, money.Currency(*currency))
	if err != nil {
		return err
	}

	ctx, router, coordinator, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	transfer, err := coordinator.Transfer(ctx, tcc.TransferInput{
		SourceMerchant: *from,
		DestMerchant:   *to,
		Amount:         value,
		IdempotencyKey: *key,
	})
	if transfer != nil {
		printTransfer(router, transfer)
	}
	return err
}

func show(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	id := fs.String("id", "", "transfer id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	transferID, err := uuid.Parse(*id)
	if err != nil {
		return fmt.Errorf("-id must be a uuid: %w", err)
	}

	ctx, router, coordinator, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	transfer, err := coordinator.Get(ctx, transferID)
	if err != nil {
		return err
	}
	printTransfer(router, transfer)
	return nil
}

// shardOf answers the question sharding makes people ask first: which database
// holds this merchant. Needed often enough during an incident that guessing at
// it with a hash function in a scratch file is worse than a subcommand.
func shardOf(args []string) error {
	fs := flag.NewFlagSet("shard", flag.ExitOnError)
	merchant := fs.String("merchant", "", "merchant id")
	quiet := fs.Bool("quiet", false, "print `<logical-key> <database>` only, for scripts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *merchant == "" {
		return errors.New("-merchant is required")
	}

	key := sharding.KeyForMerchant(*merchant)

	// Resolved through a router rather than recomputed, so this reports where
	// reads actually go rather than where they ought to.
	ctx, router, _, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()
	_ = ctx

	physical, err := router.Mapping().Resolve(key)
	if err != nil {
		return err
	}

	if *quiet {
		fmt.Printf("%s %d\n", key, physical)
		return nil
	}

	lo, hi, err := router.Mapping().LogicalRange(physical)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", *merchant)
	fmt.Printf("  logical shard   %s\n", key)
	fmt.Printf("  database        %d of %d\n", physical, router.Mapping().Physical())
	fmt.Printf("  database owns   s%02d through s%02d\n", lo, hi)
	return nil
}

func pending() error {
	ctx, router, _, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	const query = `
		SELECT id, state, source_merchant, destination_merchant, amount_minor, currency,
		       timeout_at, attempts, COALESCE(last_error, '')
		FROM tcc_transfers
		WHERE state IN ('trying', 'confirming', 'cancelling')
		ORDER BY timeout_at`

	rows, err := router.Global().Pool().Query(ctx, query)
	if err != nil {
		return fmt.Errorf("list pending transfers: %w", err)
	}
	defer rows.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tSTATE\tFROM\tTO\tAMOUNT\tDEADLINE\tATTEMPTS\tLAST ERROR")

	found := 0
	for rows.Next() {
		var (
			id                uuid.UUID
			state, src, dst   string
			minor             int64
			currency, lastErr string
			deadline          any
			attempts          int
		)
		if err := rows.Scan(&id, &state, &src, &dst, &minor, &currency, &deadline, &attempts, &lastErr); err != nil {
			return fmt.Errorf("scan pending transfer: %w", err)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d %s\t%v\t%d\t%s\n",
			id, state, src, dst, minor, currency, deadline, attempts, lastErr)
		found++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\n%d transfer(s) in flight\n", found)
	return nil
}

func sweep() error {
	ctx, router, coordinator, err := deps()
	if err != nil {
		return err
	}
	defer router.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	resolved, err := tcc.NewSweeper(coordinator, 100, logger).Sweep(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("resolved %d stranded transfer(s)\n", resolved)
	return nil
}

// printTransfer reports which databases the two sides landed on, because that
// is the difference between a transfer that needed the protocol and one that
// merely used it.
func printTransfer(router *postgres.Router, t *tcc.Transfer) {
	sourceShard, _ := router.Mapping().Resolve(t.SourceShardKey)
	destShard, _ := router.Mapping().Resolve(t.DestShardKey)

	fmt.Printf("transfer %s\n", t.ID)
	fmt.Printf("  state        %s\n", t.State)
	fmt.Printf("  amount       %s\n", t.Amount)
	fmt.Printf("  from         %s  (logical %s, database %d)\n", t.SourceMerchant, t.SourceShardKey, sourceShard)
	fmt.Printf("  to           %s  (logical %s, database %d)\n", t.DestMerchant, t.DestShardKey, destShard)
	fmt.Printf("  cross-shard  %t\n", t.CrossShard())
	if t.LastError != "" {
		fmt.Printf("  last error   %s\n", t.LastError)
	}

	// Stated explicitly so the output is self-explanatory when the mapping has
	// one database and every transfer is trivially same-shard.
	if router.Mapping().Physical() == 1 {
		fmt.Printf("  note         one physical database is configured; %d logical shards map onto it\n",
			sharding.LogicalShards)
	}
}
