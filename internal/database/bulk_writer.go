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

// bulkInsertBlocks uses UNNEST arrays for high-performance bulk inserts
func (w *BulkWriter) bulkInsertBlocks(ctx context.Context, tx pgx.Tx, blocks []*Block) error {
	if len(blocks) == 0 {
		return nil
	}

	// For very large batches, use COPY approach for best performance
	if len(blocks) > 1000 {
		return w.bulkInsertBlocksCopy(ctx, tx, blocks)
	}

	// For smaller batches, use UNNEST for better performance than temp tables
	return w.bulkInsertBlocksUnnest(ctx, tx, blocks)
}

// bulkInsertBlocksCopy uses COPY method for very large batches
func (w *BulkWriter) bulkInsertBlocksCopy(ctx context.Context, tx pgx.Tx, blocks []*Block) error {
	// Use the existing COPY-based approach for very large batches
	copySource := &blockCopySource{blocks: blocks}

	// Direct COPY to main table with conflict handling via prepared statement
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"blocks"},
		[]string{"number", "hash", "parent_hash", "timestamp", "gas_limit", "gas_used", "base_fee_per_gas", "transaction_count", "created_at"},
		copySource)

	if err != nil {
		// Fallback to UNNEST method if COPY fails due to conflicts
		return w.bulkInsertBlocksUnnest(ctx, tx, blocks)
	}

	return nil
}

// bulkInsertBlocksUnnest uses UNNEST arrays for medium-sized batches
func (w *BulkWriter) bulkInsertBlocksUnnest(ctx context.Context, tx pgx.Tx, blocks []*Block) error {
	// Prepare data arrays
	numbers := make([]int64, len(blocks))
	hashes := make([]string, len(blocks))
	parentHashes := make([]string, len(blocks))
	timestamps := make([]int64, len(blocks))
	gasLimits := make([]int64, len(blocks))
	gasUseds := make([]int64, len(blocks))
	baseFees := make([]interface{}, len(blocks))
	txCounts := make([]int32, len(blocks))
	createdAts := make([]time.Time, len(blocks))

	for i, block := range blocks {
		numbers[i] = int64(block.Number)
		hashes[i] = block.Hash
		parentHashes[i] = block.ParentHash
		timestamps[i] = block.Timestamp
		gasLimits[i] = int64(block.GasLimit)
		gasUseds[i] = int64(block.GasUsed)
		if block.BaseFeePerGas != nil {
			baseFees[i] = block.BaseFeePerGas.String()
		} else {
			baseFees[i] = nil
		}
		txCounts[i] = int32(block.TransactionCount)
		createdAts[i] = block.CreatedAt
	}

	query := `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used, base_fee_per_gas, transaction_count, created_at)
		SELECT * FROM UNNEST($1::BIGINT[], $2::TEXT[], $3::TEXT[], $4::BIGINT[], $5::BIGINT[], $6::BIGINT[], $7::NUMERIC[], $8::INTEGER[], $9::TIMESTAMPTZ[])
		ON CONFLICT (number) DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			timestamp = EXCLUDED.timestamp,
			gas_limit = EXCLUDED.gas_limit,
			gas_used = EXCLUDED.gas_used,
			base_fee_per_gas = EXCLUDED.base_fee_per_gas,
			transaction_count = EXCLUDED.transaction_count`

	_, err := tx.Exec(ctx, query, numbers, hashes, parentHashes, timestamps, gasLimits, gasUseds, baseFees, txCounts, createdAts)
	if err != nil {
		return fmt.Errorf("failed to bulk insert blocks using UNNEST: %w", err)
	}

	return nil
}

// bulkInsertTransactions uses optimized bulk insert methods
func (w *BulkWriter) bulkInsertTransactions(ctx context.Context, tx pgx.Tx, transactions []*Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// For very large batches, use COPY approach for best performance
	if len(transactions) > 2000 {
		return w.bulkInsertTransactionsCopy(ctx, tx, transactions)
	}

	// For smaller batches, use UNNEST for better performance than temp tables
	return w.bulkInsertTransactionsUnnest(ctx, tx, transactions)
}

// bulkInsertTransactionsCopy uses COPY method for very large transaction batches
func (w *BulkWriter) bulkInsertTransactionsCopy(ctx context.Context, tx pgx.Tx, transactions []*Transaction) error {
	copySource := &transactionCopySource{transactions: transactions}

	_, err := tx.CopyFrom(ctx, pgx.Identifier{"transactions"},
		[]string{"hash", "block_number", "transaction_index", "from_address", "to_address", "value", "gas_limit", "gas_price", "gas_used", "nonce", "input", "transaction_type", "status", "created_at"},
		copySource)

	if err != nil {
		// Fallback to UNNEST method if COPY fails due to conflicts
		return w.bulkInsertTransactionsUnnest(ctx, tx, transactions)
	}

	return nil
}

// bulkInsertTransactionsUnnest uses UNNEST arrays for medium-sized transaction batches
func (w *BulkWriter) bulkInsertTransactionsUnnest(ctx context.Context, tx pgx.Tx, transactions []*Transaction) error {
	// Deduplicate transactions by hash to prevent "ON CONFLICT DO UPDATE command cannot affect row a second time" error
	seen := make(map[string]bool)
	dedupedTxs := make([]*Transaction, 0, len(transactions))
	for _, txn := range transactions {
		if !seen[txn.Hash] {
			seen[txn.Hash] = true
			dedupedTxs = append(dedupedTxs, txn)
		}
	}

	if len(dedupedTxs) != len(transactions) {
		w.logger.Warn().
			Int("original", len(transactions)).
			Int("deduped", len(dedupedTxs)).
			Msg("Removed duplicate transactions from batch")
	}

	// Prepare data arrays
	hashes := make([]string, len(dedupedTxs))
	blockNumbers := make([]int64, len(dedupedTxs))
	txIndexes := make([]int32, len(dedupedTxs))
	fromAddrs := make([]string, len(dedupedTxs))
	toAddrs := make([]interface{}, len(dedupedTxs))
	values := make([]interface{}, len(dedupedTxs))
	gasLimits := make([]int64, len(dedupedTxs))
	gasPrices := make([]interface{}, len(dedupedTxs))
	gasUseds := make([]int64, len(dedupedTxs))
	nonces := make([]int64, len(dedupedTxs))
	inputs := make([]string, len(dedupedTxs))
	txTypes := make([]int32, len(dedupedTxs))
	statuses := make([]int32, len(dedupedTxs))
	createdAts := make([]time.Time, len(dedupedTxs))

	for i, txn := range dedupedTxs {
		hashes[i] = txn.Hash
		blockNumbers[i] = int64(txn.BlockNumber)
		txIndexes[i] = int32(txn.TransactionIndex)
		fromAddrs[i] = txn.FromAddress
		if txn.ToAddress != nil {
			toAddrs[i] = *txn.ToAddress
		} else {
			toAddrs[i] = nil
		}
		if txn.Value != nil {
			values[i] = txn.Value.String()
		} else {
			values[i] = nil
		}
		gasLimits[i] = int64(txn.GasLimit)
		if txn.GasPrice != nil {
			gasPrices[i] = txn.GasPrice.String()
		} else {
			gasPrices[i] = nil
		}
		gasUseds[i] = int64(txn.GasUsed)
		nonces[i] = int64(txn.Nonce)
		inputs[i] = txn.Input
		txTypes[i] = int32(txn.TransactionType)
		statuses[i] = int32(txn.Status)
		createdAts[i] = txn.CreatedAt
	}

	query := `
		INSERT INTO transactions (hash, block_number, transaction_index, from_address, to_address, value, gas_limit, gas_price, gas_used, nonce, input, transaction_type, status, created_at)
		SELECT * FROM UNNEST($1::TEXT[], $2::BIGINT[], $3::INTEGER[], $4::TEXT[], $5::TEXT[], $6::NUMERIC[], $7::BIGINT[], $8::NUMERIC[], $9::BIGINT[], $10::BIGINT[], $11::TEXT[], $12::INTEGER[], $13::INTEGER[], $14::TIMESTAMPTZ[])
		ON CONFLICT (hash) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			transaction_index = EXCLUDED.transaction_index`

	_, err := tx.Exec(ctx, query, hashes, blockNumbers, txIndexes, fromAddrs, toAddrs, values, gasLimits, gasPrices, gasUseds, nonces, inputs, txTypes, statuses, createdAts)
	if err != nil {
		return fmt.Errorf("failed to bulk insert transactions using UNNEST: %w", err)
	}

	return nil
}

// bulkInsertEventLogs uses optimized bulk insert methods
func (w *BulkWriter) bulkInsertEventLogs(ctx context.Context, tx pgx.Tx, eventLogs []*EventLog) error {
	if len(eventLogs) == 0 {
		return nil
	}

	// For very large batches, use COPY approach for best performance
	if len(eventLogs) > 5000 {
		return w.bulkInsertEventLogsCopy(ctx, tx, eventLogs)
	}

	// For smaller batches, use UNNEST for better performance than temp tables
	return w.bulkInsertEventLogsUnnest(ctx, tx, eventLogs)
}

// bulkInsertEventLogsCopy uses COPY method for very large event log batches
func (w *BulkWriter) bulkInsertEventLogsCopy(ctx context.Context, tx pgx.Tx, eventLogs []*EventLog) error {
	copySource := &eventLogCopySource{eventLogs: eventLogs}

	_, err := tx.CopyFrom(ctx, pgx.Identifier{"event_logs"},
		[]string{"block_number", "block_hash", "transaction_hash", "transaction_index", "log_index", "address", "topics", "data", "removed", "created_at"},
		copySource)

	if err != nil {
		// Fallback to UNNEST method if COPY fails due to conflicts
		return w.bulkInsertEventLogsUnnest(ctx, tx, eventLogs)
	}

	return nil
}

// bulkInsertEventLogsUnnest uses UNNEST arrays for medium-sized event log batches
func (w *BulkWriter) bulkInsertEventLogsUnnest(ctx context.Context, tx pgx.Tx, eventLogs []*EventLog) error {
	// Prepare data arrays
	blockNumbers := make([]int64, len(eventLogs))
	blockHashes := make([]string, len(eventLogs))
	txHashes := make([]string, len(eventLogs))
	txIndexes := make([]int32, len(eventLogs))
	logIndexes := make([]int32, len(eventLogs))
	addresses := make([]string, len(eventLogs))
	topics := make([]interface{}, len(eventLogs))
	data := make([]string, len(eventLogs))
	removed := make([]bool, len(eventLogs))
	createdAts := make([]time.Time, len(eventLogs))

	for i, log := range eventLogs {
		blockNumbers[i] = int64(log.BlockNumber)
		blockHashes[i] = log.BlockHash
		txHashes[i] = log.TransactionHash
		txIndexes[i] = int32(log.TransactionIndex)
		logIndexes[i] = int32(log.LogIndex)
		addresses[i] = log.Address

		// Convert topics to JSON string for JSONB
		if log.Topics != nil {
			topicsBytes, err := json.Marshal(log.Topics)
			if err != nil {
				return fmt.Errorf("failed to marshal topics to JSON: %w", err)
			}
			topics[i] = string(topicsBytes)
		} else {
			topics[i] = "[]" // Empty JSON array
		}

		data[i] = log.Data
		removed[i] = log.Removed
		createdAts[i] = log.CreatedAt
	}

	query := `
		INSERT INTO event_logs (block_number, block_hash, transaction_hash, transaction_index, log_index, address, topics, data, removed, created_at)
		SELECT * FROM UNNEST($1::BIGINT[], $2::TEXT[], $3::TEXT[], $4::INTEGER[], $5::INTEGER[], $6::TEXT[], $7::JSONB[], $8::TEXT[], $9::BOOLEAN[], $10::TIMESTAMPTZ[])
		ON CONFLICT (block_number, transaction_index, log_index) DO UPDATE SET
			block_hash = EXCLUDED.block_hash,
			transaction_hash = EXCLUDED.transaction_hash`

	_, err := tx.Exec(ctx, query, blockNumbers, blockHashes, txHashes, txIndexes, logIndexes, addresses, topics, data, removed, createdAts)
	if err != nil {
		return fmt.Errorf("failed to bulk insert event logs using UNNEST: %w", err)
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