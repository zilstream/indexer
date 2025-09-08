package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// BulkWriter provides high-performance PostgreSQL COPY-based bulk inserts
type BulkWriter struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// NewBulkWriter creates a new bulk writer optimized for fast sync
func NewBulkWriter(pool *pgxpool.Pool, logger zerolog.Logger) *BulkWriter {
	return &BulkWriter{
		pool:   pool,
		logger: logger.With().Str("component", "bulk_writer").Logger(),
	}
}

// WriteBatchBulk writes multiple blocks using PostgreSQL COPY for maximum performance
func (w *BulkWriter) WriteBatchBulk(
	ctx context.Context,
	blocks []*Block,
	transactionsByBlock map[uint64][]*Transaction,
	eventLogsByBlock map[uint64][]*EventLog,
) error {
	if len(blocks) == 0 {
		return nil
	}

	start := time.Now()
	totalTxs := 0
	totalLogs := 0
	
	// Count totals for metrics
	for _, block := range blocks {
		if txs, ok := transactionsByBlock[block.Number]; ok {
			totalTxs += len(txs)
		}
		if logs, ok := eventLogsByBlock[block.Number]; ok {
			totalLogs += len(logs)
		}
	}

	w.logger.Debug().
		Int("blocks", len(blocks)).
		Int("transactions", totalTxs).
		Int("event_logs", totalLogs).
		Msg("Starting bulk write")

	// Use a single transaction for all operations
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin bulk transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Bulk insert blocks using COPY
	if err := w.bulkInsertBlocks(ctx, tx, blocks); err != nil {
		return fmt.Errorf("failed to bulk insert blocks: %w", err)
	}

	// 2. Bulk insert transactions using COPY
	if totalTxs > 0 {
		allTxs := make([]*Transaction, 0, totalTxs)
		for _, block := range blocks {
			if txs, ok := transactionsByBlock[block.Number]; ok {
				allTxs = append(allTxs, txs...)
			}
		}
		if err := w.bulkInsertTransactions(ctx, tx, allTxs); err != nil {
			return fmt.Errorf("failed to bulk insert transactions: %w", err)
		}
	}

	// 3. Bulk insert event logs using COPY
	if totalLogs > 0 {
		allLogs := make([]*EventLog, 0, totalLogs)
		for _, block := range blocks {
			if logs, ok := eventLogsByBlock[block.Number]; ok {
				allLogs = append(allLogs, logs...)
			}
		}
		if err := w.bulkInsertEventLogs(ctx, tx, allLogs); err != nil {
			return fmt.Errorf("failed to bulk insert event logs: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit bulk transaction: %w", err)
	}

	elapsed := time.Since(start)
	blocksPerSec := float64(len(blocks)) / elapsed.Seconds()
	
	w.logger.Info().
		Int("blocks", len(blocks)).
		Int("transactions", totalTxs).
		Int("event_logs", totalLogs).
		Dur("elapsed", elapsed).
		Float64("blocks_per_sec", blocksPerSec).
		Msg("Bulk write completed")

	return nil
}

// bulkInsertBlocks uses COPY to insert blocks in bulk
func (w *BulkWriter) bulkInsertBlocks(ctx context.Context, tx pgx.Tx, blocks []*Block) error {
	if len(blocks) == 0 {
		return nil
	}

	// Create a temporary table first (for upsert behavior)
	_, err := tx.Exec(ctx, `
		CREATE TEMPORARY TABLE temp_blocks (
			number BIGINT,
			hash TEXT,
			parent_hash TEXT,
			timestamp BIGINT,
			gas_limit BIGINT,
			gas_used BIGINT,
			base_fee_per_gas NUMERIC,
			transaction_count INTEGER,
			created_at TIMESTAMPTZ
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp blocks table: %w", err)
	}

	// Use COPY to insert into temp table
	copySource := &blockCopySource{blocks: blocks}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"temp_blocks"}, 
		[]string{"number", "hash", "parent_hash", "timestamp", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "created_at"},
		copySource)
	if err != nil {
		return fmt.Errorf("failed to copy blocks: %w", err)
	}

	// Insert from temp table with conflict handling
	_, err = tx.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used, base_fee_per_gas, transaction_count, created_at)
		SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, base_fee_per_gas, transaction_count, created_at
		FROM temp_blocks
		ON CONFLICT (number) DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			timestamp = EXCLUDED.timestamp,
			gas_limit = EXCLUDED.gas_limit,
			gas_used = EXCLUDED.gas_used,
			base_fee_per_gas = EXCLUDED.base_fee_per_gas,
			transaction_count = EXCLUDED.transaction_count
	`)
	if err != nil {
		return fmt.Errorf("failed to upsert blocks from temp table: %w", err)
	}

	return nil
}

// bulkInsertTransactions uses COPY to insert transactions in bulk
func (w *BulkWriter) bulkInsertTransactions(ctx context.Context, tx pgx.Tx, transactions []*Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// Create temporary table
	_, err := tx.Exec(ctx, `
		CREATE TEMPORARY TABLE temp_transactions (
			hash TEXT,
			block_number BIGINT,
			transaction_index INTEGER,
			from_address TEXT,
			to_address TEXT,
			value NUMERIC,
			gas_limit BIGINT,
			gas_price NUMERIC,
			gas_used BIGINT,
			nonce BIGINT,
			input TEXT,
			transaction_type INTEGER,
			status INTEGER,
			created_at TIMESTAMPTZ
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp transactions table: %w", err)
	}

	// Use COPY to insert into temp table
	copySource := &transactionCopySource{transactions: transactions}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"temp_transactions"},
		[]string{"hash", "block_number", "transaction_index", "from_address", "to_address", "value", "gas_limit", "gas_price", "gas_used", "nonce", "input", "transaction_type", "status", "created_at"},
		copySource)
	if err != nil {
		return fmt.Errorf("failed to copy transactions: %w", err)
	}

	// Insert from temp table with conflict handling
	_, err = tx.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, transaction_index, from_address, to_address, value, gas_limit, gas_price, gas_used, nonce, input, transaction_type, status, created_at)
		SELECT hash, block_number, transaction_index, from_address, to_address, value, gas_limit, gas_price, gas_used, nonce, input, transaction_type, status, created_at
		FROM temp_transactions
		ON CONFLICT (hash) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			transaction_index = EXCLUDED.transaction_index
	`)
	if err != nil {
		return fmt.Errorf("failed to upsert transactions from temp table: %w", err)
	}

	return nil
}

// bulkInsertEventLogs uses COPY to insert event logs in bulk
func (w *BulkWriter) bulkInsertEventLogs(ctx context.Context, tx pgx.Tx, eventLogs []*EventLog) error {
	if len(eventLogs) == 0 {
		return nil
	}

	// Create temporary table
	_, err := tx.Exec(ctx, `
		CREATE TEMPORARY TABLE temp_event_logs (
			block_number BIGINT,
			block_hash TEXT,
			transaction_hash TEXT,
			transaction_index INTEGER,
			log_index INTEGER,
			address TEXT,
			topics JSONB,
			data TEXT,
			removed BOOLEAN,
			created_at TIMESTAMPTZ
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp event_logs table: %w", err)
	}

	// Use COPY to insert into temp table
	copySource := &eventLogCopySource{eventLogs: eventLogs}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"temp_event_logs"},
		[]string{"block_number", "block_hash", "transaction_hash", "transaction_index", "log_index", "address", "topics", "data", "removed", "created_at"},
		copySource)
	if err != nil {
		return fmt.Errorf("failed to copy event logs: %w", err)
	}

	// Insert from temp table with conflict handling
	_, err = tx.Exec(ctx, `
		INSERT INTO event_logs (block_number, block_hash, transaction_hash, transaction_index, log_index, address, topics, data, removed, created_at)
		SELECT block_number, block_hash, transaction_hash, transaction_index, log_index, address, topics, data, removed, created_at
		FROM temp_event_logs
		ON CONFLICT (block_number, transaction_index, log_index) DO UPDATE SET
			block_hash = EXCLUDED.block_hash,
			transaction_hash = EXCLUDED.transaction_hash
	`)
	if err != nil {
		return fmt.Errorf("failed to upsert event logs from temp table: %w", err)
	}

	return nil
}

// blockCopySource implements pgx.CopyFromSource for blocks
type blockCopySource struct {
	blocks []*Block
	idx    int
}

func (s *blockCopySource) Next() bool {
	s.idx++
	return s.idx <= len(s.blocks)
}

func (s *blockCopySource) Values() ([]interface{}, error) {
	if s.idx > len(s.blocks) || s.idx <= 0 {
		return nil, fmt.Errorf("invalid index")
	}
	
	block := s.blocks[s.idx-1]
	
	// Convert *big.Int to string for PostgreSQL COPY
	var baseFeePerGas interface{} = nil
	if block.BaseFeePerGas != nil {
		baseFeePerGas = block.BaseFeePerGas.String()
	}
	
	return []interface{}{
		block.Number,
		block.Hash,
		block.ParentHash,
		block.Timestamp,
		block.GasLimit,
		block.GasUsed,
		baseFeePerGas,
		block.TransactionCount,
		block.CreatedAt,
	}, nil
}

func (s *blockCopySource) Err() error {
	return nil
}

// transactionCopySource implements pgx.CopyFromSource for transactions
type transactionCopySource struct {
	transactions []*Transaction
	idx          int
}

func (s *transactionCopySource) Next() bool {
	s.idx++
	return s.idx <= len(s.transactions)
}

func (s *transactionCopySource) Values() ([]interface{}, error) {
	if s.idx > len(s.transactions) || s.idx <= 0 {
		return nil, fmt.Errorf("invalid index")
	}
	
	tx := s.transactions[s.idx-1]
	
	// Convert *big.Int to string for PostgreSQL COPY
	var value interface{} = nil
	if tx.Value != nil {
		value = tx.Value.String()
	}
	
	var gasPrice interface{} = nil
	if tx.GasPrice != nil {
		gasPrice = tx.GasPrice.String()
	}
	
	return []interface{}{
		tx.Hash,
		tx.BlockNumber,
		tx.TransactionIndex,
		tx.FromAddress,
		tx.ToAddress,
		value,
		tx.GasLimit,
		gasPrice,
		tx.GasUsed,
		tx.Nonce,
		tx.Input,
		tx.TransactionType,
		tx.Status,
		tx.CreatedAt,
	}, nil
}

func (s *transactionCopySource) Err() error {
	return nil
}

// eventLogCopySource implements pgx.CopyFromSource for event logs
type eventLogCopySource struct {
	eventLogs []*EventLog
	idx       int
}

func (s *eventLogCopySource) Next() bool {
	s.idx++
	return s.idx <= len(s.eventLogs)
}

func (s *eventLogCopySource) Values() ([]interface{}, error) {
	if s.idx > len(s.eventLogs) || s.idx <= 0 {
		return nil, fmt.Errorf("invalid index")
	}
	
	log := s.eventLogs[s.idx-1]
	
	// Convert topics slice to JSON string for JSONB field
	var topicsJSON interface{} = nil
	if log.Topics != nil {
		topicsBytes, err := json.Marshal(log.Topics)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal topics to JSON: %w", err)
		}
		topicsJSON = string(topicsBytes)
	} else {
		topicsJSON = "[]" // Empty JSON array
	}
	
	return []interface{}{
		log.BlockNumber,
		log.BlockHash,
		log.TransactionHash,
		log.TransactionIndex,
		log.LogIndex,
		log.Address,
		topicsJSON,
		log.Data,
		log.Removed,
		log.CreatedAt,
	}, nil
}

func (s *eventLogCopySource) Err() error {
	return nil
}