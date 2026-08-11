// Command webhookctl inspects and replays the raw webhook log.
//
// The log exists so that a mapping mistake is recoverable: the exact bytes a
// provider sent are kept, and can be re-read through corrected logic. That is
// only worth anything if someone can actually re-read them, which is what this
// command is for.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"

	"github.com/lequoctrung/payment-orchestrator/internal/config"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/postgres"
	"github.com/lequoctrung/payment-orchestrator/internal/platform/telemetry"
	txstore "github.com/lequoctrung/payment-orchestrator/internal/store/transaction"
	"github.com/lequoctrung/payment-orchestrator/internal/webhook"
	webhookproviders "github.com/lequoctrung/payment-orchestrator/internal/webhook/providers"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "webhookctl:", err)
		os.Exit(1)
	}
}

const usage = `usage: webhookctl replay [-v]

  replay   re-evaluate every stored event against current state and report what
           would change. Writes nothing.
`

func run() error {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("a subcommand is required")
	}

	switch os.Args[1] {
	case "replay":
		return replay(os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}

func replay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	verbose := fs.Bool("v", false, "list every event, not only the ones that would change")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := telemetry.NewLogger(os.Stderr, cfg.Log.Level, cfg.Log.Format, false)
	ctx := context.Background()

	db, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()

	registry := webhook.NewRegistry(
		webhookproviders.NewSimulator(cfg.Webhook.Provider, cfg.Webhook.Secret),
	)
	processor := webhook.NewProcessor(db, registry, webhook.NewRepository(),
		txstore.NewRepository(), logger)

	report, err := processor.Replay(ctx, db.Pool())
	if err != nil {
		return err
	}

	// Writes are buffered by the tabwriter; Flush below reports any real error.
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tREFERENCE\tSEQ\tRECORDED\tREPLAY\tNOTE")
	for _, e := range report.Entries {
		if !*verbose && !e.Changed() {
			continue
		}
		_, _ = fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\t%s\n",
			e.RawEventID, e.Reference, e.Sequence, e.RecordedOutcome, e.ReplayOutcome, e.Note)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\n%d events, %d would change state\n", len(report.Entries), report.Changed)

	// A convergent log replays to nothing. Anything else means applying the same
	// event twice has an effect, which would make replay a corruption tool
	// rather than a recovery one — so it is an error, not a remark.
	if report.Changed > 0 {
		return fmt.Errorf("%d events would change state on replay; the log is not convergent", report.Changed)
	}

	logger.Info("replay clean", slog.Int("events", len(report.Entries)))
	return nil
}
