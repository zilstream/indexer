package processor

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// BlockProcessor handles the processing of blocks
type BlockProcessor struct {
	rpcClient      *rpc.Client
	db             *database.Database
	blockRepo      *database.BlockRepository
	eventProcessor *EventProcessor
	logger         zerolog.Logger
}

// NewBlockProcessor creates a new block processor
func NewBlockProcessor(rpcClient *rpc.Client, db *database.Database, logger zerolog.Logger) *BlockProcessor {
	return &BlockProcessor{
		rpcClient:      rpcClient,
		db:             db,
		blockRepo:      database.NewBlockRepository(db),
		eventProcessor: NewEventProcessor(rpcClient, db, logger),
		logger:         logger,
	}
}

// ProcessBlock processes a single block with its transactions and events
func (p *BlockProcessor) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	// Create a transaction processor for this operation
	txProcessor := NewTransactionProcessor(p.rpcClient, p.db, p.logger)
	
	// Use the full processing method
	return p.ProcessBlockWithTransactions(ctx, blockNumber, txProcessor)
}

// ProcessBlockWithTransactions processes a block and its transactions together
func (p *BlockProcessor) ProcessBlockWithTransactions(ctx context.Context, blockNumber uint64, txProcessor *TransactionProcessor) error {
	// Fetch block from RPC
	block, err := p.rpcClient.GetBlockWithTransactions(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch block %d: %w", blockNumber, err)
	}

	// Use transaction to ensure atomicity
	err = p.db.Transaction(ctx, func(tx pgx.Tx) error {
		// Convert and insert block
		dbBlock := p.convertBlock(block)
		if err := p.insertBlockTx(ctx, tx, dbBlock); err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}

		// Process transactions if any
		var receipts []*types.Receipt
		if len(block.Transactions()) > 0 {
			receipts, err = p.rpcClient.GetBlockReceipts(ctx, blockNumber)
			if err != nil {
				return fmt.Errorf("failed to get block receipts: %w", err)
			}

			// Get transaction metadata if available (for Zilliqa pre-EVM transactions)
			metadata := p.rpcClient.GetTransactionMetadata(block.Hash().Hex())
			
			transactions := txProcessor.convertTransactionsWithMeta(block, receipts, metadata)
			if err := txProcessor.insertBatchTx(ctx, tx, transactions); err != nil {
				return fmt.Errorf("failed to insert transactions: %w", err)
			}
			
			// Process event logs from receipts
			if err := p.eventProcessor.ProcessBlockLogs(ctx, block, receipts); err != nil {
				return fmt.Errorf("failed to process event logs: %w", err)
			}
		}

		// Update indexer state
		if err := p.updateLastBlockTx(ctx, tx, blockNumber, block.Hash().Hex()); err != nil {
			return fmt.Errorf("failed to update last block: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	p.logger.Info().
		Uint64("number", blockNumber).
		Str("hash", block.Hash().Hex()).
		Int("transactions", len(block.Transactions())).
		Uint64("gas_used", block.GasUsed()).
		Msg("Block and transactions processed")

	return nil
}

// convertBlock converts an Ethereum block to database model
func (p *BlockProcessor) convertBlock(block *types.Block) *database.Block {
	return &database.Block{
		Number:           block.NumberU64(),
		Hash:             block.Hash().Hex(),
		ParentHash:       block.ParentHash().Hex(),
		Timestamp:        int64(block.Time()),
		GasLimit:         block.GasLimit(),
		GasUsed:          block.GasUsed(),
		TransactionCount: len(block.Transactions()),
	}
}

// insertBlockTx inserts a block within a transaction
func (p *BlockProcessor) insertBlockTx(ctx context.Context, tx pgx.Tx, block *database.Block) error {
	query := `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used, transaction_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (number) DO NOTHING`

	_, err := tx.Exec(ctx, query,
		block.Number,
		block.Hash,
		block.ParentHash,
		block.Timestamp,
		block.GasLimit,
		block.GasUsed,
		block.TransactionCount,
	)

	return err
}

// updateLastBlockTx updates the last block within a transaction
func (p *BlockProcessor) updateLastBlockTx(ctx context.Context, tx pgx.Tx, blockNumber uint64, blockHash string) error {
	query := `
		UPDATE indexer_state 
		SET last_block_number = $1, 
		    last_block_hash = $2,
		    updated_at = NOW()
		WHERE chain_id = $3`

	_, err := tx.Exec(ctx, query, blockNumber, blockHash, 32769)
	return err
}