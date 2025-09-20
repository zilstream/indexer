package database

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
)

const migrationsDir = "migrations"

// RunMigrations applies embedded SQL migrations in order based on filename prefixes.
// Migration files are applied once and tracked in the schema_migrations table.
func RunMigrations(ctx context.Context, cfg *config.DatabaseConfig, logger zerolog.Logger) error {
	connConfig, err := pgx.ParseConfig(cfg.ConnectionString())
	if err != nil {
		return fmt.Errorf("parse connection string: %w", err)
	}
	// Use simple protocol so multi-statement files work without manual splitting.
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("connect database for migrations: %w", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	entries, err := migrationsFS.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version := strings.TrimSuffix(entry.Name(), ".sql")
		applied, err := hasMigration(ctx, conn, version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		if err := applyMigration(ctx, conn, entry, version, logger); err != nil {
			return err
		}
	}

	return nil
}

func hasMigration(ctx context.Context, conn *pgx.Conn, version string) (bool, error) {
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
		version,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check migration %s: %w", version, err)
	}
	return exists, nil
}

func applyMigration(ctx context.Context, conn *pgx.Conn, entry fs.DirEntry, version string, logger zerolog.Logger) error {
	contents, err := migrationsFS.ReadFile(fmt.Sprintf("%s/%s", migrationsDir, entry.Name()))
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}

	script := strings.TrimSpace(string(contents))

	noTx := false
	for _, line := range strings.Split(script, "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "-- +no-transaction") {
			noTx = true
			break
		}
	}

	if noTx {
		statements := splitSQLStatements(script)
		for _, stmt := range statements {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply migration %s: %w", version, err)
			}
		}

		if _, err := conn.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`,
			version,
		); err != nil {
			return fmt.Errorf("record migration %s: %w", version, err)
		}

		logger.Info().Str("migration", version).Msg("Applied migration (no transaction)")
		return nil
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if script != "" {
		if _, err := tx.Exec(ctx, script); err != nil {
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version) VALUES ($1)`,
		version,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}

	logger.Info().Str("migration", version).Msg("Applied migration")
	return nil
}

func splitSQLStatements(script string) []string {
	lines := strings.Split(script, "\n")
	var b strings.Builder
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	cleaned := b.String()
	parts := strings.Split(cleaned, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		statements = append(statements, stmt)
	}

	return statements
}
