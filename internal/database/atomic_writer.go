package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// AtomicBlockWriter handles atomic writes of blocks with their transactions and event logs
type AtomicBlockWriter struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// NewAtomicBlockWriter creates a new atomic block writer
func NewAtomicBlockWriter(pool *pgxpool.Pool, logger zerolog.Logger) *AtomicBlockWriter {
	return &AtomicBlockWriter{
		pool:   pool,
		logger: logger.With().Str("component", "atomic_writer").Logger(),
	}
}

// WriteBlock atomically writes a block with its transactions and event logs
func (w *AtomicBlockWriter) WriteBlock(
	ctx context.Context,
	block *Block,
	transactions []*Transaction,
	eventLogs []*EventLog,
) error {
	// Start transaction
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert block first (parent for foreign keys)
	err = w.insertBlock(ctx, tx, block)
	if err != nil {
		return fmt.Errorf("failed to insert block: %w", err)
	}

	// Insert transactions (depend on block)
	for _, txn := range transactions {
		err = w.insertTransaction(ctx, tx, txn)
		if err != nil {
			return fmt.Errorf("failed to insert transaction %s: %w", txn.Hash, err)
		}
	}

	// Insert event logs (depend on transaction)
	for _, log := range eventLogs {
		err = w.insertEventLog(ctx, tx, log)
		if err != nil {
			return fmt.Errorf("failed to insert event log: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	w.logger.Debug().
		Uint64("block", block.Number).
		Int("transactions", len(transactions)).
		Int("events", len(eventLogs)).
		Msg("Block written atomically")

	return nil
}

// WriteBatch atomically writes multiple blocks in a single transaction
func (w *AtomicBlockWriter) WriteBatch(
	ctx context.Context,
	blocks []*Block,
	transactionsByBlock map[uint64][]*Transaction,
	eventLogsByBlock map[uint64][]*EventLog,
) error {
	if len(blocks) == 0 {
		return nil
	}

	start := time.Now()
	
	// Start transaction
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Process each block in order
	for _, block := range blocks {
		// Insert block
		err = w.insertBlock(ctx, tx, block)
		if err != nil {
			return fmt.Errorf("failed to insert block %d: %w", block.Number, err)
		}

		// Insert transactions for this block
		if txns, ok := transactionsByBlock[block.Number]; ok {
			for _, txn := range txns {
				err = w.insertTransaction(ctx, tx, txn)
				if err != nil {
					return fmt.Errorf("failed to insert transaction %s: %w", txn.Hash, err)
				}
			}
		}

		// Insert event logs for this block
		if logs, ok := eventLogsByBlock[block.Number]; ok {
			for _, log := range logs {
				err = w.insertEventLog(ctx, tx, log)
				if err != nil {
					return fmt.Errorf("failed to insert event log: %w", err)
				}
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	elapsed := time.Since(start)
	w.logger.Info().
		Int("blocks", len(blocks)).
		Dur("elapsed", elapsed).
		Float64("blocks_per_sec", float64(len(blocks))/elapsed.Seconds()).
		Msg("Batch written atomically")

	return nil
}

func (w *AtomicBlockWriter) insertBlock(ctx context.Context, tx pgx.Tx, block *Block) error {
	query := `
		INSERT INTO blocks (
			number, hash, parent_hash, timestamp,
			gas_limit, gas_used, base_fee_per_gas,
			transaction_count, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		ON CONFLICT (number) DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			timestamp = EXCLUDED.timestamp,
			gas_limit = EXCLUDED.gas_limit,
			gas_used = EXCLUDED.gas_used,
			base_fee_per_gas = EXCLUDED.base_fee_per_gas,
			transaction_count = EXCLUDED.transaction_count`

	_, err := tx.Exec(ctx, query,
		block.Number,
		block.Hash,
		block.ParentHash,
		block.Timestamp,
		block.GasLimit,
		block.GasUsed,
		block.BaseFeePerGas,
		block.TransactionCount,
		block.CreatedAt,
	)
	return err
}

func (w *AtomicBlockWriter) insertTransaction(ctx context.Context, tx pgx.Tx, txn *Transaction) error {
	query := `
		INSERT INTO transactions (
			hash, block_number, transaction_index,
			from_address, to_address, value,
			gas_limit, gas_price, gas_used,
			nonce, input, transaction_type,
			status, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14
		)
		ON CONFLICT (hash) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			transaction_index = EXCLUDED.transaction_index`

	_, err := tx.Exec(ctx, query,
		txn.Hash,
		txn.BlockNumber,
		txn.TransactionIndex,
		txn.FromAddress,
		txn.ToAddress,
		txn.Value,
		txn.GasLimit,
		txn.GasPrice,
		txn.GasUsed,
		txn.Nonce,
		txn.Input,
		txn.TransactionType,
		txn.Status,
		txn.CreatedAt,
	)
	return err
}

func (w *AtomicBlockWriter) insertEventLog(ctx context.Context, tx pgx.Tx, log *EventLog) error {
	query := `
		INSERT INTO event_logs (
			block_number, block_hash, transaction_hash,
			transaction_index, log_index, address,
			topics, data, removed, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (block_number, transaction_index, log_index) DO UPDATE SET
			block_hash = EXCLUDED.block_hash,
			transaction_hash = EXCLUDED.transaction_hash`

	_, err := tx.Exec(ctx, query,
		log.BlockNumber,
		log.BlockHash,
		log.TransactionHash,
		log.TransactionIndex,
		log.LogIndex,
		log.Address,
		log.Topics,
		log.Data,
		log.Removed,
		log.CreatedAt,
	)
	return err
}