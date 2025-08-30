package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
)

var ErrNotFound = errors.New("not found")

type Database struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// Alias for external packages
type DB = Database

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

// GetLastBlock returns the last indexed block
func (db *Database) GetLastBlock(ctx context.Context) (*Block, error) {
	var block Block
	query := `
		SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, base_fee_per_gas
		FROM blocks
		ORDER BY number DESC
		LIMIT 1`
	
	err := db.pool.QueryRow(ctx, query).Scan(
		&block.Number,
		&block.Hash,
		&block.ParentHash,
		&block.Timestamp,
		&block.GasLimit,
		&block.GasUsed,
		&block.BaseFeePerGas,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to get last block: %w", err)
	}

	return &block, nil
}

// FindMissingBlocks finds gaps in block numbers between start and end
func (db *Database) FindMissingBlocks(ctx context.Context, start, end uint64) ([]uint64, error) {
	query := `
		WITH RECURSIVE expected_blocks AS (
			SELECT $1::BIGINT as block_num
			UNION ALL
			SELECT block_num + 1
			FROM expected_blocks
			WHERE block_num < $2
		)
		SELECT e.block_num
		FROM expected_blocks e
		LEFT JOIN blocks b ON e.block_num = b.number
		WHERE b.number IS NULL
		ORDER BY e.block_num`
	
	rows, err := db.pool.Query(ctx, query, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to find missing blocks: %w", err)
	}
	defer rows.Close()
	
	var missing []uint64
	for rows.Next() {
		var blockNum uint64
		if err := rows.Scan(&blockNum); err != nil {
			return nil, err
		}
		missing = append(missing, blockNum)
	}
	
	return missing, nil
}

// ValidateBlockSequence checks if blocks are properly linked by parent hash
func (db *Database) ValidateBlockSequence(ctx context.Context, start, end uint64) error {
	query := `
		SELECT b1.number, b1.hash, b2.parent_hash
		FROM blocks b1
		LEFT JOIN blocks b2 ON b1.number + 1 = b2.number
		WHERE b1.number >= $1 AND b1.number < $2
			AND b2.parent_hash IS NOT NULL
			AND b1.hash != b2.parent_hash
		ORDER BY b1.number`
	
	rows, err := db.pool.Query(ctx, query, start, end)
	if err != nil {
		return fmt.Errorf("failed to validate block sequence: %w", err)
	}
	defer rows.Close()
	
	var mismatches []string
	for rows.Next() {
		var blockNum uint64
		var blockHash, nextParentHash string
		if err := rows.Scan(&blockNum, &blockHash, &nextParentHash); err != nil {
			return err
		}
		mismatches = append(mismatches, fmt.Sprintf("block %d hash %s != next block parent %s", 
			blockNum, blockHash, nextParentHash))
	}
	
	if len(mismatches) > 0 {
		return fmt.Errorf("block sequence validation failed: %v", mismatches)
	}
	
	return nil
}