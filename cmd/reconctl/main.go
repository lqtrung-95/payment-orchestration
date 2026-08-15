// Command reconctl ingests settlement files, reconciles them against the
// ledger, and reports the breaks.
//
// Reconciliation proposes money movement, so every subcommand that decides
// something demands an actor. "Who closed this break, and why" is the first
// question asked when a discrepancy turns out to have been real.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/google/uuid"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
	"github.com/lequoctrung/payment-orchestrator/internal/recon"
	"github.com/lequoctrung/payment-orchestrator/internal/recon/breaks"
	fxstore "github.com/lequoctrung/payment-orchestrator/internal/store/fx"
)

const usage = `usage: reconctl <command> [flags]

  ingest     -provider P -file PATH        parse and store a settlement file
  run        -file-id ID -actor WHO        reconcile a stored file, report breaks
  breaks     [-status S] [-category C]     list breaks
  resolve    -id ID -actor WHO -reason R [-write-off]
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reconctl:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("a subcommand is required")
	}

	switch os.Args[1] {
	case "ingest":
		return ingest(os.Args[2:])
	case "run":
		return reconcile(os.Args[2:])
	case "breaks":
		return listBreaks(os.Args[2:])
	case "resolve":
		return resolve(os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

// deps wires what every subcommand needs.
type deps struct {
	db      *postgres.DB
	service *recon.Service
	repo    *recon.Repository
}

func open(ctx context.Context) (*deps, func(), error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, err
	}
	logger := telemetry.NewLogger(os.Stderr, cfg.Log.Level, cfg.Log.Format, false)

	db, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return nil, nil, err
	}

	service := recon.NewService(db,
		recon.NewRegistry(recon.NewSimulatorParser(cfg.PSP.DefaultProvider)),
		recon.NewRepository(), recon.NewLedgerReader(), fxstore.NewRepository(),
		recon.DefaultTolerances(), logger)

	return &deps{db: db, service: service, repo: recon.NewRepository()}, db.Close, nil
}

func ingest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	provider := fs.String("provider", "", "provider whose format the file is in")
	path := fs.String("file", "", "path to the settlement file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *provider == "" || *path == "" {
		return errors.New("-provider and -file are required")
	}

	ctx := context.Background()
	d, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	f, err := os.Open(*path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	file, isNew, err := d.service.Ingest(ctx, *provider, *path, f)
	if err != nil {
		return err
	}

	status := "already ingested"
	if isNew {
		status = "ingested"
	}
	fmt.Printf("%s: %s  %d rows  %s to %s\n", status, file.ID, file.RowCount,
		file.PeriodStart.Format("2006-01-02T15:04:05Z"), file.PeriodEnd.Format("2006-01-02T15:04:05Z"))
	return nil
}

func reconcile(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	fileID := fs.String("file-id", "", "settlement file to reconcile")
	actor := fs.String("actor", "", "who is running this")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *fileID == "" || *actor == "" {
		return errors.New("-file-id and -actor are required")
	}

	id, err := uuid.Parse(*fileID)
	if err != nil {
		return fmt.Errorf("file-id: %w", err)
	}

	ctx := context.Background()
	d, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	report, err := d.service.Reconcile(ctx, id, *actor)
	if err != nil {
		return err
	}

	fmt.Printf("run %s\n  matched %d\n  breaks  %d (%d new)\n\n",
		report.RunID, report.Matched, report.Total(), report.NewBreaks)

	if report.Total() > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(w, "  CATEGORY\tCOUNT\tAUTO-RESOLVABLE")
		for _, category := range breaks.All {
			if n := report.ByCategory[category]; n > 0 {
				_, _ = fmt.Fprintf(w, "  %s\t%d\t%t\n", category, n, category.AutoResolvable())
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}

	// Exposure is what is actually at stake, which a count of breaks does not
	// convey: one break worth six figures matters more than fifty worth pennies.
	if len(report.Exposure) > 0 {
		fmt.Println("\n  exposure")
		currencies := make([]string, 0, len(report.Exposure))
		for cur := range report.Exposure {
			currencies = append(currencies, cur)
		}
		sort.Strings(currencies)
		for _, cur := range currencies {
			fmt.Printf("    %s %d minor units\n", cur, report.Exposure[cur])
		}
	}
	return nil
}

func listBreaks(args []string) error {
	fs := flag.NewFlagSet("breaks", flag.ExitOnError)
	status := fs.String("status", "", "filter by status")
	category := fs.String("category", "", "filter by category")
	limit := fs.Int("limit", 50, "maximum rows")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	d, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	const query = `
		SELECT id, category, status, match_key,
		       COALESCE(delta_minor, 0), COALESCE(currency, ''), detail
		FROM recon_breaks
		WHERE ($1 = '' OR status::text = $1)
		  AND ($2 = '' OR category::text = $2)
		ORDER BY created_at DESC
		LIMIT $3`

	rows, err := d.db.Pool().Query(ctx, query, *status, *category, *limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tCATEGORY\tSTATUS\tDELTA\tKEY\tDETAIL")

	var n int
	for rows.Next() {
		var (
			id                      uuid.UUID
			cat, st, key, cur, note string
			delta                   int64
		)
		if err := rows.Scan(&id, &cat, &st, &key, &delta, &cur, &note); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%d %s\t%s\t%s\n", id, cat, st, delta, cur, key, note)
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\n%d breaks\n", n)
	return nil
}

func resolve(args []string) error {
	fs := flag.NewFlagSet("resolve", flag.ExitOnError)
	id := fs.String("id", "", "break to resolve")
	actor := fs.String("actor", "", "who is deciding")
	reason := fs.String("reason", "", "why")
	writeOff := fs.Bool("write-off", false, "record as written off rather than resolved")
	if err := fs.Parse(args); err != nil {
		return err
	}
	// Attribution is mandatory, and the database enforces it too. A break closed
	// with no name and no reason is indistinguishable from one quietly deleted.
	if *id == "" || *actor == "" || *reason == "" {
		return errors.New("-id, -actor and -reason are all required")
	}

	breakID, err := uuid.Parse(*id)
	if err != nil {
		return fmt.Errorf("id: %w", err)
	}

	ctx := context.Background()
	d, closeDB, err := open(ctx)
	if err != nil {
		return err
	}
	defer closeDB()

	status := breaks.StatusResolved
	if *writeOff {
		status = breaks.StatusWrittenOff
	}

	if err := d.repo.Resolve(ctx, d.db.Pool(), breakID, breaks.Resolution{
		Status: status, Actor: *actor, Note: *reason,
	}, nil); err != nil {
		return err
	}

	fmt.Printf("break %s marked %s by %s\n", breakID, status, *actor)
	return nil
}
