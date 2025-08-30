package processor

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// Indexer is the main indexer that coordinates block processing
type Indexer struct {
	config    *config.Config
	rpcClient *rpc.Client
	db        *database.Database
	
	blockProcessor *BlockProcessor
	txProcessor    *TransactionProcessor
	
	logger    zerolog.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewIndexer creates a new indexer instance
func NewIndexer(cfg *config.Config, logger zerolog.Logger) (*Indexer, error) {
	// Create RPC client
	rpcClient, err := rpc.NewClient(cfg.Chain.RPCEndpoint, cfg.Chain.ChainID, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	// Create database connection
	db, err := database.New(context.Background(), &cfg.Database, logger)
	if err != nil {
		rpcClient.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Create processors
	blockProcessor := NewBlockProcessor(rpcClient, db, logger)
	txProcessor := NewTransactionProcessor(rpcClient, db, logger)

	ctx, cancel := context.WithCancel(context.Background())

	return &Indexer{
		config:         cfg,
		rpcClient:      rpcClient,
		db:             db,
		blockProcessor: blockProcessor,
		txProcessor:    txProcessor,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// Start starts the indexer
func (i *Indexer) Start() error {
	i.logger.Info().Msg("Starting indexer")

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start sync loop in a goroutine
	i.wg.Add(1)
	go i.syncLoop()

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		i.logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
	case <-i.ctx.Done():
		i.logger.Info().Msg("Context cancelled")
	}

	// Graceful shutdown
	i.Stop()
	return nil
}

// Stop stops the indexer gracefully
func (i *Indexer) Stop() {
	i.logger.Info().Msg("Stopping indexer")
	
	// Cancel context to stop all goroutines
	i.cancel()
	
	// Wait for all goroutines to finish
	i.wg.Wait()
	
	// Close connections
	i.rpcClient.Close()
	i.db.Close()
	
	i.logger.Info().Msg("Indexer stopped")
}

// syncLoop is the main synchronization loop
func (i *Indexer) syncLoop() {
	defer i.wg.Done()

	// Get last indexed block
	lastBlock, err := i.db.GetLastBlockNumber(i.ctx)
	if err != nil {
		i.logger.Error().Err(err).Msg("Failed to get last block number")
		return
	}

	// If starting from scratch and start_block is 0, get latest block
	if lastBlock == 0 && i.config.Chain.StartBlock == 0 {
		latestBlock, err := i.rpcClient.GetLatestBlockNumber(i.ctx)
		if err != nil {
			i.logger.Error().Err(err).Msg("Failed to get latest block number")
			return
		}
		lastBlock = latestBlock
		i.logger.Info().Uint64("block", lastBlock).Msg("Starting from latest block")
	} else if lastBlock == 0 && i.config.Chain.StartBlock > 0 {
		lastBlock = i.config.Chain.StartBlock - 1
		i.logger.Info().Uint64("block", i.config.Chain.StartBlock).Msg("Starting from configured block")
	}

	// Main sync loop
	consecutiveErrors := 0
	maxConsecutiveErrors := 10

	for {
		select {
		case <-i.ctx.Done():
			i.logger.Info().Msg("Sync loop stopped")
			return
		default:
			// Get the latest block number
			latestBlock, err := i.rpcClient.GetLatestBlockNumber(i.ctx)
			if err != nil {
				i.logger.Error().Err(err).Msg("Failed to get latest block number")
				consecutiveErrors++
				if consecutiveErrors >= maxConsecutiveErrors {
					i.logger.Error().Msg("Too many consecutive errors, stopping sync")
					return
				}
				time.Sleep(5 * time.Second)
				continue
			}

			// Check if we're caught up
			if lastBlock >= latestBlock {
				i.logger.Debug().
					Uint64("current", lastBlock).
					Uint64("latest", latestBlock).
					Msg("Caught up with chain")
				time.Sleep(i.config.Chain.BlockTime)
				continue
			}

			// Process next block
			nextBlock := lastBlock + 1
			
			startTime := time.Now()
			err = i.processBlock(nextBlock)
			processingTime := time.Since(startTime)

			if err != nil {
				i.logger.Error().
					Err(err).
					Uint64("block", nextBlock).
					Dur("duration", processingTime).
					Msg("Failed to process block")
				
				consecutiveErrors++
				if consecutiveErrors >= maxConsecutiveErrors {
					i.logger.Error().Msg("Too many consecutive errors, stopping sync")
					return
				}
				
				// Wait before retrying
				time.Sleep(5 * time.Second)
				continue
			}

			// Reset error counter on success
			consecutiveErrors = 0

			// Log progress
			lag := latestBlock - nextBlock
			i.logger.Info().
				Uint64("block", nextBlock).
				Uint64("lag", lag).
				Dur("duration", processingTime).
				Msg("Block processed")

			// Update last block
			lastBlock = nextBlock

			// If we're far behind, don't sleep
			if lag > 100 {
				continue
			}

			// Small delay to avoid overwhelming the RPC
			if lag > 10 {
				time.Sleep(100 * time.Millisecond)
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

// processBlock processes a single block with its transactions
func (i *Indexer) processBlock(blockNumber uint64) error {
	ctx, cancel := context.WithTimeout(i.ctx, 30*time.Second)
	defer cancel()

	// Process block and transactions together in a transaction
	return i.blockProcessor.ProcessBlockWithTransactions(ctx, blockNumber, i.txProcessor)
}

// GetStatus returns the current indexer status
func (i *Indexer) GetStatus(ctx context.Context) (map[string]interface{}, error) {
	lastBlock, err := i.db.GetLastBlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	latestBlock, err := i.rpcClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"last_indexed_block": lastBlock,
		"latest_block":       latestBlock,
		"lag":                latestBlock - lastBlock,
		"syncing":            lastBlock < latestBlock,
		"connected":          i.rpcClient.IsConnected(ctx),
	}, nil
}