package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
)

type Database struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

func New(ctx context.Context, cfg *config.DatabaseConfig, logger zerolog.Logger) (*Database, error) {
	connString := cfg.ConnectionString()
	
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure pool settings
	poolConfig.MaxConns = cfg.MaxConnections
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Minute * 30

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("database", cfg.Name).
		Msg("Connected to database")

	return &Database{
		pool:   pool,
		logger: logger,
	}, nil
}

func (db *Database) Close() {
	db.pool.Close()
	db.logger.Info().Msg("Database connection closed")
}

func (db *Database) Pool() *pgxpool.Pool {
	return db.pool
}

// Transaction executes a function within a database transaction
func (db *Database) Transaction(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				db.logger.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetLastBlockNumber returns the last indexed block number
func (db *Database) GetLastBlockNumber(ctx context.Context) (uint64, error) {
	var blockNumber uint64
	query := `SELECT last_block_number FROM indexer_state WHERE chain_id = $1`
	
	err := db.pool.QueryRow(ctx, query, 32769).Scan(&blockNumber)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get last block number: %w", err)
	}

	return blockNumber, nil
}

// UpdateLastBlockNumber updates the last indexed block number
func (db *Database) UpdateLastBlockNumber(ctx context.Context, blockNumber uint64, blockHash string) error {
	query := `
		UPDATE indexer_state 
		SET last_block_number = $1, 
		    last_block_hash = $2,
		    updated_at = NOW()
		WHERE chain_id = $3`
	
	_, err := db.pool.Exec(ctx, query, blockNumber, blockHash, 32769)
	if err != nil {
		return fmt.Errorf("failed to update last block number: %w", err)
	}

	return nil
}