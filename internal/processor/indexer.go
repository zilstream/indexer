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
	"github.com/zilstream/indexer/internal/api"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
	syncMgr "github.com/zilstream/indexer/internal/sync"
)

// Indexer is the main indexer that coordinates block processing
type Indexer struct {
	config    *config.Config
	rpcClient *rpc.Client
	db        *database.Database
	
	blockProcessor *BlockProcessor
	txProcessor    *TransactionProcessor
	syncManager    *syncMgr.Manager
	healthServer   *api.HealthServer
	
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
	
	// Create sync manager
	syncConfig := syncMgr.Config{
		BatchSize:  cfg.Processor.BatchSize,
		MaxWorkers: cfg.Processor.Workers,
		RetryDelay: 5 * time.Second,
		MaxRetries: 3,
		StartBlock: cfg.Chain.StartBlock,
	}
	syncManager := syncMgr.NewManager(db, rpcClient, blockProcessor, logger, syncConfig)
	
	// Create health server
	healthServer := api.NewHealthServer(db, rpcClient, syncManager, logger, fmt.Sprintf("%d", cfg.Server.MetricsPort))

	ctx, cancel := context.WithCancel(context.Background())

	return &Indexer{
		config:         cfg,
		rpcClient:      rpcClient,
		db:             db,
		blockProcessor: blockProcessor,
		txProcessor:    txProcessor,
		syncManager:    syncManager,
		healthServer:   healthServer,
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

	// Start health server
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		if err := i.healthServer.Start(); err != nil {
			i.logger.Error().Err(err).Msg("Health server error")
		}
	}()

	// Check if we should use fast sync
	if err := i.checkAndRunFastSync(); err != nil {
		i.logger.Error().Err(err).Msg("Fast sync check failed")
		// Continue with normal sync even if fast sync fails
	}

	// Start normal sync manager for ongoing blocks
	i.wg.Add(1)
	go func() {
		defer i.wg.Done()
		if err := i.syncManager.Start(i.ctx); err != nil {
			i.logger.Error().Err(err).Msg("Sync manager error")
		}
	}()

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


// checkAndRunFastSync checks if fast sync should be used and runs it if needed
func (i *Indexer) checkAndRunFastSync() error {
	// Check if fast sync is enabled
	if !i.config.Processor.FastSync.Enabled {
		i.logger.Info().Msg("Fast sync is disabled in configuration")
		return nil
	}
	
	ctx := context.Background()
	
	// Get current chain tip
	latestBlock, err := i.rpcClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}
	
	// Get last indexed block
	lastIndexedBlock, err := i.db.GetLastBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get last indexed block: %w", err)
	}
	
	// Determine start block
	startBlock := lastIndexedBlock
	if startBlock == 0 && i.config.Chain.StartBlock > 0 {
		startBlock = i.config.Chain.StartBlock
		// If we're starting from a specific block, update the database so we don't try to sync earlier blocks
		if err := i.db.UpdateLastBlockNumber(ctx, startBlock-1, ""); err != nil {
			i.logger.Warn().Err(err).Uint64("block", startBlock-1).Msg("Failed to set initial block number")
		}
	}
	if startBlock == 0 {
		startBlock = 1 // Start from block 1 if not specified
	}
	
	// Calculate how far behind we are
	blocksToSync := latestBlock - startBlock
	
	// Use fast sync if we're more than threshold blocks behind
	fastSyncThreshold := uint64(i.config.Processor.FastSync.Threshold)
	
	if blocksToSync > fastSyncThreshold {
		i.logger.Info().
			Uint64("start_block", startBlock).
			Uint64("latest_block", latestBlock).
			Uint64("blocks_to_sync", blocksToSync).
			Msg("Large gap detected, using fast sync for historical blocks")
		
		// Calculate where to stop fast sync (leave last 1000 blocks for normal sync)
		const recentBlockBuffer = 1000
		fastSyncEndBlock := latestBlock - recentBlockBuffer
		if fastSyncEndBlock <= startBlock {
			// Not enough blocks to warrant fast sync
			return nil
		}
		
		// Configure fast sync from config
		fastSyncConfig := syncMgr.FastSyncConfig{
			BatchSize:         i.config.Processor.FastSync.BatchSize,
			WorkerCount:       i.config.Processor.FastSync.Workers,
			BufferSize:        i.config.Processor.FastSync.BufferSize,
			SkipReceipts:      i.config.Processor.FastSync.SkipReceipts,
			SkipReceiptsBelow: latestBlock - uint64(i.config.Processor.FastSync.SkipReceiptsBelow),
			RequestsPerSecond: i.config.Processor.FastSync.RequestsPerSecond,
			OptimizeBatchSize: true,
			LogBlockRange:     5000,
		}
		
		// Create fast sync instance
		fastSync := syncMgr.NewFastSync(
			fastSyncConfig,
			i.db,
			i.rpcClient,
			i.logger,
		)
		defer fastSync.Close()
		
		// Run fast sync
		i.logger.Info().
			Uint64("start", startBlock).
			Uint64("end", fastSyncEndBlock).
			Uint64("total", fastSyncEndBlock-startBlock+1).
			Msg("Starting fast sync for historical blocks")
		
		startTime := time.Now()
		if err := fastSync.SyncRange(ctx, startBlock, fastSyncEndBlock); err != nil {
			return fmt.Errorf("fast sync failed: %w", err)
		}
		
		elapsed := time.Since(startTime)
		blocksPerSec := float64(fastSyncEndBlock-startBlock+1) / elapsed.Seconds()
		
		i.logger.Info().
			Uint64("blocks_synced", fastSyncEndBlock-startBlock+1).
			Dur("elapsed", elapsed).
			Float64("blocks_per_sec", blocksPerSec).
			Msg("Fast sync completed successfully")
		
		// Update last indexed block for normal sync to continue
		if err := i.db.UpdateLastBlockNumber(ctx, fastSyncEndBlock, ""); err != nil {
			return fmt.Errorf("failed to update last block after fast sync: %w", err)
		}
		
		i.logger.Info().
			Uint64("continuing_from", fastSyncEndBlock+1).
			Msg("Switching to normal sync for recent blocks")
	} else if blocksToSync > 0 {
		i.logger.Info().
			Uint64("blocks_behind", blocksToSync).
			Msg("Gap is small, using normal sync")
	} else {
		i.logger.Info().Msg("Already caught up with chain")
	}
	
	return nil
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