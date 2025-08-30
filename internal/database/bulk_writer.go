package database

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// BulkWriter provides high-performance bulk write operations using PostgreSQL COPY
type BulkWriter struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
	
	// Buffering
	blockBuffer       []*Block
	transactionBuffer []*Transaction
	bufferSize        int
	mu                sync.Mutex
	
	// Metrics
	totalBlocks       int64
	totalTransactions int64
	totalCopyTime     time.Duration
}

// NewBulkWriter creates a new bulk writer
func NewBulkWriter(pool *pgxpool.Pool, bufferSize int, logger zerolog.Logger) *BulkWriter {
	return &BulkWriter{
		pool:              pool,
		logger:            logger,
		bufferSize:        bufferSize,
		blockBuffer:       make([]*Block, 0, bufferSize),
		transactionBuffer: make([]*Transaction, 0, bufferSize*10), // Assume avg 10 tx per block
	}
}

// AddBlock adds a block to the buffer
func (w *BulkWriter) AddBlock(block *Block) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.blockBuffer = append(w.blockBuffer, block)
	
	// Auto-flush if buffer is full
	if len(w.blockBuffer) >= w.bufferSize {
		return w.flushBlocksLocked(context.Background())
	}
	
	return nil
}

// AddTransaction adds a transaction to the buffer
func (w *BulkWriter) AddTransaction(tx *Transaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.transactionBuffer = append(w.transactionBuffer, tx)
	
	// Auto-flush if buffer is full
	if len(w.transactionBuffer) >= w.bufferSize*10 {
		return w.flushTransactionsLocked(context.Background())
	}
	
	return nil
}

// Flush flushes all buffers
func (w *BulkWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// Flush blocks first (transactions have foreign key dependency)
	if err := w.flushBlocksLocked(ctx); err != nil {
		return err
	}
	
	// Then flush transactions
	if err := w.flushTransactionsLocked(ctx); err != nil {
		return err
	}
	
	return nil
}

// flushBlocksLocked flushes the block buffer (must be called with lock held)
func (w *BulkWriter) flushBlocksLocked(ctx context.Context) error {
	if len(w.blockBuffer) == 0 {
		return nil
	}
	
	start := time.Now()
	
	// Use COPY for bulk insert
	copyCount, err := w.pool.CopyFrom(
		ctx,
		pgx.Identifier{"blocks"},
		[]string{
			"number",
			"hash",
			"parent_hash",
			"timestamp",
			"gas_limit",
			"gas_used",
			"base_fee_per_gas",
			"transaction_count",
			"created_at",
		},
		pgx.CopyFromSlice(len(w.blockBuffer), func(i int) ([]interface{}, error) {
			b := w.blockBuffer[i]
			// Handle nil BaseFeePerGas
			var baseFee interface{}
			if b.BaseFeePerGas != nil {
				baseFee = b.BaseFeePerGas.String() // Convert to string for numeric column
			}
			return []interface{}{
				b.Number,
				b.Hash,
				b.ParentHash,
				b.Timestamp,
				b.GasLimit,
				b.GasUsed,
				baseFee,
				b.TransactionCount,
				b.CreatedAt,
			}, nil
		}),
	)
	
	if err != nil {
		// Handle conflicts by falling back to upsert
		if strings.Contains(err.Error(), "duplicate key") {
			return w.upsertBlocksLocked(ctx)
		}
		return fmt.Errorf("failed to copy blocks: %w", err)
	}
	
	elapsed := time.Since(start)
	w.totalCopyTime += elapsed
	w.totalBlocks += copyCount
	
	w.logger.Debug().
		Int64("count", copyCount).
		Dur("elapsed", elapsed).
		Float64("blocks_per_sec", float64(copyCount)/elapsed.Seconds()).
		Msg("Bulk inserted blocks")
	
	// Clear buffer
	w.blockBuffer = w.blockBuffer[:0]
	
	return nil
}

// upsertBlocksLocked performs upsert for blocks that already exist
func (w *BulkWriter) upsertBlocksLocked(ctx context.Context) error {
	if len(w.blockBuffer) == 0 {
		return nil
	}
	
	// Build bulk upsert query
	query := `
		INSERT INTO blocks (
			number, hash, parent_hash, timestamp, 
			gas_limit, gas_used, base_fee_per_gas, 
			transaction_count, created_at
		) VALUES `
	
	values := make([]interface{}, 0, len(w.blockBuffer)*9)
	placeholders := make([]string, 0, len(w.blockBuffer))
	
	for i, b := range w.blockBuffer {
		base := i * 9
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
		))
		
		values = append(values,
			b.Number,
			b.Hash,
			b.ParentHash,
			b.Timestamp,
			b.GasLimit,
			b.GasUsed,
			b.BaseFeePerGas,
			b.TransactionCount,
			b.CreatedAt,
		)
	}
	
	query += strings.Join(placeholders, ", ")
	query += ` ON CONFLICT (number) DO UPDATE SET
		hash = EXCLUDED.hash,
		parent_hash = EXCLUDED.parent_hash,
		timestamp = EXCLUDED.timestamp,
		gas_limit = EXCLUDED.gas_limit,
		gas_used = EXCLUDED.gas_used,
		base_fee_per_gas = EXCLUDED.base_fee_per_gas,
		transaction_count = EXCLUDED.transaction_count`
	
	_, err := w.pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to upsert blocks: %w", err)
	}
	
	w.logger.Debug().
		Int("count", len(w.blockBuffer)).
		Msg("Upserted blocks")
	
	// Clear buffer
	w.blockBuffer = w.blockBuffer[:0]
	
	return nil
}

// flushTransactionsLocked flushes the transaction buffer (must be called with lock held)
func (w *BulkWriter) flushTransactionsLocked(ctx context.Context) error {
	if len(w.transactionBuffer) == 0 {
		return nil
	}
	
	start := time.Now()
	
	// Use COPY for bulk insert
	copyCount, err := w.pool.CopyFrom(
		ctx,
		pgx.Identifier{"transactions"},
		[]string{
			"hash",
			"block_number",
			"transaction_index",
			"from_address",
			"to_address",
			"value",
			"gas_price",
			"gas_limit",
			"gas_used",
			"nonce",
			"input",
			"status",
			"transaction_type",
			"original_type_hex",
			"created_at",
		},
		pgx.CopyFromSlice(len(w.transactionBuffer), func(i int) ([]interface{}, error) {
			tx := w.transactionBuffer[i]
			// Handle big.Int values
			var value, gasPrice interface{}
			if tx.Value != nil {
				value = tx.Value.String()
			}
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
				gasPrice,
				tx.GasLimit,
				tx.GasUsed,
				tx.Nonce,
				tx.Input,
				tx.Status,
				tx.TransactionType,
				tx.OriginalTypeHex,
				tx.CreatedAt,
			}, nil
		}),
	)
	
	if err != nil {
		// Handle conflicts by falling back to upsert
		if strings.Contains(err.Error(), "duplicate key") {
			return w.upsertTransactionsLocked(ctx)
		}
		return fmt.Errorf("failed to copy transactions: %w", err)
	}
	
	elapsed := time.Since(start)
	w.totalCopyTime += elapsed
	w.totalTransactions += copyCount
	
	w.logger.Debug().
		Int64("count", copyCount).
		Dur("elapsed", elapsed).
		Float64("tx_per_sec", float64(copyCount)/elapsed.Seconds()).
		Msg("Bulk inserted transactions")
	
	// Clear buffer
	w.transactionBuffer = w.transactionBuffer[:0]
	
	return nil
}

// upsertTransactionsLocked performs upsert for transactions that already exist
func (w *BulkWriter) upsertTransactionsLocked(ctx context.Context) error {
	if len(w.transactionBuffer) == 0 {
		return nil
	}
	
	// For large batches, split into smaller chunks to avoid query size limits
	chunkSize := 100
	for i := 0; i < len(w.transactionBuffer); i += chunkSize {
		end := i + chunkSize
		if end > len(w.transactionBuffer) {
			end = len(w.transactionBuffer)
		}
		
		chunk := w.transactionBuffer[i:end]
		if err := w.upsertTransactionChunk(ctx, chunk); err != nil {
			return err
		}
	}
	
	// Clear buffer
	w.transactionBuffer = w.transactionBuffer[:0]
	
	return nil
}

// upsertTransactionChunk upserts a chunk of transactions
func (w *BulkWriter) upsertTransactionChunk(ctx context.Context, txs []*Transaction) error {
	// Build bulk upsert query
	query := `
		INSERT INTO transactions (
			hash, block_number, transaction_index, from_address, to_address,
			value, gas_price, gas_limit, gas_used, nonce, input, status,
			transaction_type, original_type_hex, created_at
		) VALUES `
	
	values := make([]interface{}, 0, len(txs)*15)
	placeholders := make([]string, 0, len(txs))
	
	for i, tx := range txs {
		base := i * 15
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
			base+9, base+10, base+11, base+12, base+13, base+14, base+15,
		))
		
		values = append(values,
			tx.Hash,
			tx.BlockNumber,
			tx.TransactionIndex,
			tx.FromAddress,
			tx.ToAddress,
			tx.Value,
			tx.GasPrice,
			tx.GasLimit,
			tx.GasUsed,
			tx.Nonce,
			tx.Input,
			tx.Status,
			tx.TransactionType,
			tx.OriginalTypeHex,
			tx.CreatedAt,
		)
	}
	
	query += strings.Join(placeholders, ", ")
	query += ` ON CONFLICT (hash) DO UPDATE SET
		block_number = EXCLUDED.block_number,
		transaction_index = EXCLUDED.transaction_index,
		from_address = EXCLUDED.from_address,
		to_address = EXCLUDED.to_address,
		value = EXCLUDED.value,
		gas_price = EXCLUDED.gas_price,
		gas_limit = EXCLUDED.gas_limit,
		gas_used = EXCLUDED.gas_used,
		status = EXCLUDED.status,
		transaction_type = EXCLUDED.transaction_type,
		original_type_hex = EXCLUDED.original_type_hex`
	
	_, err := w.pool.Exec(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to upsert transaction chunk: %w", err)
	}
	
	return nil
}

// GetMetrics returns writer metrics
func (w *BulkWriter) GetMetrics() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	avgCopyTime := time.Duration(0)
	if w.totalBlocks+w.totalTransactions > 0 {
		avgCopyTime = w.totalCopyTime / time.Duration(w.totalBlocks+w.totalTransactions)
	}
	
	return map[string]interface{}{
		"total_blocks":       w.totalBlocks,
		"total_transactions": w.totalTransactions,
		"total_copy_time":    w.totalCopyTime.String(),
		"avg_copy_time":      avgCopyTime.String(),
		"blocks_buffered":    len(w.blockBuffer),
		"tx_buffered":        len(w.transactionBuffer),
	}
}

// BulkInsertBlocks performs a bulk insert of blocks using COPY
func BulkInsertBlocks(ctx context.Context, pool *pgxpool.Pool, blocks []*Block) error {
	if len(blocks) == 0 {
		return nil
	}
	
	copyCount, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"blocks"},
		[]string{
			"number",
			"hash", 
			"parent_hash",
			"timestamp",
			"gas_limit",
			"gas_used",
			"base_fee_per_gas",
			"transaction_count",
			"created_at",
		},
		pgx.CopyFromSlice(len(blocks), func(i int) ([]interface{}, error) {
			b := blocks[i]
			return []interface{}{
				b.Number,
				b.Hash,
				b.ParentHash,
				b.Timestamp,
				b.GasLimit,
				b.GasUsed,
				b.BaseFeePerGas,
				b.TransactionCount,
				time.Now(),
			}, nil
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to bulk insert %d blocks: %w", len(blocks), err)
	}
	
	if copyCount != int64(len(blocks)) {
		return fmt.Errorf("expected to insert %d blocks, but inserted %d", len(blocks), copyCount)
	}
	
	return nil
}

// BulkInsertTransactions performs a bulk insert of transactions using COPY
func BulkInsertTransactions(ctx context.Context, pool *pgxpool.Pool, transactions []*Transaction) error {
	if len(transactions) == 0 {
		return nil
	}
	
	copyCount, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"transactions"},
		[]string{
			"hash",
			"block_number",
			"transaction_index",
			"from_address",
			"to_address",
			"value",
			"gas_price",
			"gas_limit",
			"gas_used",
			"nonce",
			"input",
			"status",
			"transaction_type",
			"original_type_hex",
			"created_at",
		},
		pgx.CopyFromSlice(len(transactions), func(i int) ([]interface{}, error) {
			tx := transactions[i]
			return []interface{}{
				tx.Hash,
				tx.BlockNumber,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.Value,
				tx.GasPrice,
				tx.GasLimit,
				tx.GasUsed,
				tx.Nonce,
				tx.Input,
				tx.Status,
				tx.TransactionType,
				tx.OriginalTypeHex,
				time.Now(),
			}, nil
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to bulk insert %d transactions: %w", len(transactions), err)
	}
	
	if copyCount != int64(len(transactions)) {
		return fmt.Errorf("expected to insert %d transactions, but inserted %d", len(transactions), copyCount)
	}
	
	return nil
}