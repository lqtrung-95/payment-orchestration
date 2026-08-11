// Command migrate applies or reverts database migrations.
//
//	migrate up          apply all pending migrations
//	migrate down        revert the most recent migration
//	migrate version     print current version and dirty state
//	migrate force <n>   clear the dirty flag at version n
//
// Migrations are embedded in the binary, so this command needs no access to the
// repository checkout.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

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

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return errors.New("POSTGRES_DSN is required")
	}

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
			fmt.Fprintf(os.Stderr, "migrate: close: source=%v database=%v\n", srcErr, dbErr)
		}
	}()

	switch args[0] {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("apply migrations: %w", err)
		}
		return printVersion(m, "migrations applied")

	case "down":
		// Steps(-1) rather than Down(): reverting every migration in one command
		// is rarely what anyone intends, and on a payment schema it is
		// catastrophic if run against the wrong DSN.
		if err := m.Steps(-1); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("revert migration: %w", err)
		}
		return printVersion(m, "migration reverted")

	case "version":
		return printVersion(m, "current version")

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
		return printVersion(m, "version forced")

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
