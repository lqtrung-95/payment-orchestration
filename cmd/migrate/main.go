// Command migrate applies or reverts database migrations.
//
//	migrate up          apply all pending migrations
//	migrate down        revert the most recent migration
//	migrate version     print current version and dirty state
//	migrate force <n>   clear the dirty flag at version n
//
// Every command runs against every physical shard listed in
// POSTGRES_SHARD_DSNS, falling back to POSTGRES_DSN when it is unset. The whole
// schema exists on every shard: merchant-partitioned tables hold that shard's
// merchants, reference tables are replicated, and the back-office tables are
// only written on shard 0. A shard left a version behind is a shard whose
// merchants fail on the first query touching the new column, so a partial
// success is reported as a failure.
//
// Migrations are embedded in the binary, so this command needs no access to the
// repository checkout.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/lequoctrung/payment-orchestrator/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected a command: up, down, version, force")
	}

	dsns, err := shardDSNs()
	if err != nil {
		return err
	}

	// Every shard is attempted even after one fails, and the failures are
	// reported together. Stopping at the first leaves the operator discovering
	// the remaining shards one rerun at a time.
	var failures []string
	for i, dsn := range dsns {
		label := fmt.Sprintf("shard %d", i)
		if err := runOne(args, dsn, label); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", label, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d of %d shards failed:\n  %s",
			len(failures), len(dsns), strings.Join(failures, "\n  "))
	}
	return nil
}

// shardDSNs resolves the databases to migrate. POSTGRES_SHARD_DSNS wins when
// set; POSTGRES_DSN alone means a single unsharded database.
func shardDSNs() ([]string, error) {
	if raw := strings.TrimSpace(os.Getenv("POSTGRES_SHARD_DSNS")); raw != "" {
		var out []string
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return nil, errors.New("POSTGRES_DSN or POSTGRES_SHARD_DSNS is required")
	}
	return []string{dsn}, nil
}

func runOne(args []string, dsn, label string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("load embedded migrations: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return fmt.Errorf("open migrator: %w", err)
	}
	defer func() {
		// Both returned errors are reported together: a source close failure and
		// a database close failure have different causes and hiding either one
		// makes a stuck migration harder to diagnose.
		if srcErr, dbErr := m.Close(); srcErr != nil || dbErr != nil {
			fmt.Fprintf(os.Stderr, "migrate: %s: close: source=%v database=%v\n", label, srcErr, dbErr)
		}
	}()

	switch args[0] {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("apply migrations: %w", err)
		}
		return printVersion(m, label+": migrations applied")

	case "down":
		// Steps(-1) rather than Down(): reverting every migration in one command
		// is rarely what anyone intends, and on a payment schema it is
		// catastrophic if run against the wrong DSN.
		if err := m.Steps(-1); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("revert migration: %w", err)
		}
		return printVersion(m, label+": migration reverted")

	case "version":
		return printVersion(m, label+": current version")

	case "force":
		if len(args) < 2 {
			return errors.New("force requires a version argument")
		}
		v, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("force version must be an integer: %w", err)
		}
		if err := m.Force(v); err != nil {
			return fmt.Errorf("force version: %w", err)
		}
		return printVersion(m, label+": version forced")

	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printVersion(m *migrate.Migrate, label string) error {
	v, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		fmt.Printf("%s: no migrations applied\n", label)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read version: %w", err)
	}
	fmt.Printf("%s: version=%d dirty=%t\n", label, v, dirty)
	return nil
}
