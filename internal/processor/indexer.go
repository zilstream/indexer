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

	// Start sync manager
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