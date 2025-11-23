package processor

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/api"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/core"
	"github.com/zilstream/indexer/internal/modules/loader"
	"github.com/zilstream/indexer/internal/modules/uniswapv2"
	"github.com/zilstream/indexer/internal/modules/uniswapv3"
	"github.com/zilstream/indexer/internal/prices"
	"github.com/zilstream/indexer/internal/realtime"
	"github.com/zilstream/indexer/internal/rpc"
	"github.com/zilstream/indexer/internal/sync"
)

// Indexer is the main indexer that coordinates block processing
type Indexer struct {
	config         *config.Config
	rpcClient      *rpc.Client
	db             *database.Database
	moduleRegistry *core.ModuleRegistry
	pricePoller    *prices.Poller
	publisher      *realtime.Publisher
	logger         zerolog.Logger
	shutdown       chan struct{}
}

// NewIndexer creates a new indexer instance
func NewIndexer(cfg *config.Config, logger zerolog.Logger) (*Indexer, error) {
	ctx := context.Background()

	// Extract RPC endpoint from config
	rpcEndpoint := cfg.Chain.RPCEndpoint
	if rpcEndpoint == "" {
		return nil, fmt.Errorf("RPC endpoint not configured")
	}

	// Connect to RPC
	rpcClient, err := rpc.NewClient(rpcEndpoint, int64(cfg.Chain.ChainID), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	// Connect to database
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if cfg.Bootstrap.ZILPrices.AutoLoad {
		if _, err := prices.EnsureHistoricalPrices(ctx, db.Pool(), prices.LoaderConfig{
			CSVPath:   cfg.Bootstrap.ZILPrices.CSVPath,
			Source:    cfg.Bootstrap.ZILPrices.Source,
			BatchSize: cfg.Bootstrap.ZILPrices.BatchSize,
		}, logger); err != nil {
			db.Close()
			return nil, fmt.Errorf("bootstrap ZIL prices: %w", err)
		}
	}

	// Create module registry
	moduleRegistry := core.NewModuleRegistry(db, logger)

	// Create price poller
	pricePoller := prices.NewPoller(db.Pool(), prices.PollerConfig{
		Interval: cfg.Processor.PricePollInterval,
	}, logger)

	// Create realtime publisher if enabled
	var publisher *realtime.Publisher
	if cfg.Centrifugo.Enabled {
		publisherConfig := realtime.PublishConfig{
			APIURL: cfg.Centrifugo.APIURL,
			APIKey: cfg.Centrifugo.APIKey,
		}
		publisher = realtime.NewPublisher(publisherConfig, db.Pool(), logger)
		logger.Info().Msg("Realtime publisher enabled")
	} else {
		logger.Info().Msg("Realtime publisher disabled")
	}

	return &Indexer{
		config:         cfg,
		rpcClient:      rpcClient,
		db:             db,
		moduleRegistry: moduleRegistry,
		pricePoller:    pricePoller,
		publisher:      publisher,
		logger:         logger.With().Str("component", "indexer").Logger(),
		shutdown:       make(chan struct{}),
	}, nil
}

// Start starts the indexer
func (i *Indexer) Start(ctx context.Context) error {
	i.logger.Info().Msg("Starting indexer")

	// Initialize modules
	if err := i.initializeModules(ctx); err != nil {
		return fmt.Errorf("failed to initialize modules: %w", err)
	}

	// Recover recent blocks for modules after restart (prevents data loss from interrupted deployments)
	if err := i.recoverModuleData(ctx); err != nil {
		i.logger.Warn().Err(err).Msg("Module recovery failed, continuing anyway")
	}

	// Start health server
	healthServer := api.NewHealthServer(i, i.logger)
	go func() {
		port := fmt.Sprintf(":%d", i.config.Server.MetricsPort)
		i.logger.Info().Str("port", port).Msg("Starting health server")
		if err := healthServer.Start(port); err != nil {
			i.logger.Error().Err(err).Msg("Health server error")
		}
	}()

	// Start price poller early (before sync to avoid blocking)
	i.logger.Info().Msg("Starting price poller")
	if i.pricePoller != nil {
		go func() {
			if err := i.pricePoller.Start(ctx); err != nil && err != context.Canceled {
				i.logger.Error().Err(err).Msg("Price poller error")
			}
		}()
		time.Sleep(100 * time.Millisecond)
		i.logger.Info().Msg("Price poller started")
	} else {
		i.logger.Warn().Msg("Price poller is nil, skipping")
	}

	// Start initial sync if needed
	if err := i.syncOnce(ctx); err != nil {
		i.logger.Error().Err(err).Msg("Initial sync failed")
	}

	// Start continuous sync
	go i.continuousSync(ctx)

	// Wait for shutdown
	i.waitForShutdown()

	return nil
}

// continuousSync runs the continuous sync loop
func (i *Indexer) continuousSync(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-i.shutdown:
			return
		case <-ticker.C:
			if err := i.syncOnce(ctx); err != nil {
				i.logger.Error().Err(err).Msg("Sync error")
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// syncOnce performs one sync iteration
func (i *Indexer) syncOnce(ctx context.Context) error {
	// Get current state
	lastBlock, err := i.db.GetLastBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get last block: %w", err)
	}

	latestBlock, err := i.rpcClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	// Determine start block
	startBlock := lastBlock + 1
	if lastBlock == 0 && i.config.Chain.StartBlock > 0 {
		startBlock = uint64(i.config.Chain.StartBlock)
		i.logger.Info().Uint64("start_block", startBlock).Msg("Starting from configured block")

		// Set initial block number
		if err := i.db.UpdateLastBlockNumber(ctx, startBlock-1, ""); err != nil {
			i.logger.Warn().Err(err).Uint64("block", startBlock-1).Msg("Failed to set initial block number")
		}
	}
	if startBlock == 0 {
		startBlock = 1 // Start from block 1 if not specified
	}

	// Calculate how far behind we are
	if latestBlock <= startBlock {
		return nil // Already caught up
	}

	blocksToSync := latestBlock - startBlock

	if blocksToSync > 0 {
		// Create unified sync config
		syncConfig := sync.UnifiedSyncConfig{
			MaxBatchSize:      i.config.Processor.BatchSize,
			MaxRetries:        i.config.Processor.MaxRetries,
			RetryDelay:        i.config.Processor.RetryDelay,
			RequestsPerSecond: i.config.Processor.RequestsPerSecond,
		}

		// Create unified sync instance
		unifiedSync := sync.NewUnifiedSync(
			i.db,
			i.rpcClient,
			i.moduleRegistry,
			syncConfig,
			i.logger,
		)
		if i.publisher != nil {
			unifiedSync.SetPublisher(i.publisher)
		}
		defer unifiedSync.Close()

		// Determine end block (don't go all the way to latest for safety)
		endBlock := latestBlock
		if blocksToSync > 100 {
			// Leave a small buffer for very recent blocks
			endBlock = latestBlock - 10
		}

		// Run sync
		if err := unifiedSync.SyncRange(ctx, startBlock, endBlock); err != nil {
			return fmt.Errorf("sync failed: %w", err)
		}
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
		"lastIndexedBlock": lastBlock,
		"latestBlock":      latestBlock,
		"blocksIndexing":   latestBlock - lastBlock,
		"synced":           lastBlock >= latestBlock,
		"chainId":          i.config.Chain.ChainID,
	}, nil
}

// waitForShutdown waits for shutdown signal
func (i *Indexer) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	i.logger.Info().Msg("Shutdown signal received")

	close(i.shutdown)

	i.logger.Info().Msg("Indexer stopped")
}

// initializeModules initializes the module system
func (i *Indexer) initializeModules(ctx context.Context) error {
	i.logger.Info().Msg("Initializing modules")

	// Load manifests from directory
	manifestLoader := loader.NewManifestLoader(i.logger)
	manifests, err := manifestLoader.LoadFromDirectory("manifests")
	if err != nil {
		return fmt.Errorf("failed to load manifests: %w", err)
	}

	i.logger.Info().Int("count", len(manifests)).Msg("Loaded manifests")

	// Initialize each module based on manifest
	// Create shared price provider
	priceProvider := prices.NewPostgresProvider(i.db.Pool(), 10_000)
	for _, manifest := range manifests {
		// Instantiate module based on manifest name
		var module core.Module
		switch strings.ToLower(manifest.Name) {
		case "uniswap-v2":
			m, err := uniswapv2.NewUniswapV2Module(i.logger)
			if err != nil {
				i.logger.Error().Err(err).Str("module", manifest.Name).Msg("Failed to create UniswapV2 module")
				continue
			}
			m.SetPriceProvider(priceProvider)
			if i.publisher != nil {
				m.SetPublisher(i.publisher)
			}
			module = m
		case "uniswap-v3":
			m, err := uniswapv3.NewUniswapV3Module(i.logger)
			if err != nil {
				i.logger.Error().Err(err).Str("module", manifest.Name).Msg("Failed to create UniswapV3 module")
				continue
			}
			m.SetPriceProvider(priceProvider)
			if i.publisher != nil {
				m.SetPublisher(i.publisher)
			}
			module = m
		default:
			// Fallback: try v2
			m, err := uniswapv2.NewUniswapV2Module(i.logger)
			if err != nil {
				i.logger.Error().Err(err).Str("module", manifest.Name).Msg("Failed to create default module")
				continue
			}
			m.SetPriceProvider(priceProvider)
			module = m
		}

		// Initialize module with database
		if err := module.Initialize(ctx, i.db); err != nil {
			i.logger.Error().Err(err).Str("module", manifest.Name).Msg("Failed to initialize module")
			continue
		}

		// Register module
		if err := i.moduleRegistry.RegisterModule(module); err != nil {
			i.logger.Error().Err(err).Str("module", manifest.Name).Msg("Failed to register module")
			continue
		}

		i.logger.Info().Str("module", manifest.Name).Str("version", manifest.Version).Msg("Module registered")
	}

	// Start module registry
	i.moduleRegistry.Start()

	i.logger.Info().
		Int("modules", len(manifests)).
		Msg("Module system initialized")

	return nil
}

// recoverModuleData reprocesses recent blocks to ensure module data consistency after restart
func (i *Indexer) recoverModuleData(ctx context.Context) error {
	// Get last indexed block
	lastBlock, err := i.db.GetLastBlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get last block: %w", err)
	}

	if lastBlock == 0 {
		i.logger.Info().Msg("No blocks indexed yet, skipping module recovery")
		return nil
	}

	// Reprocess last 10 blocks to ensure module data is complete
	// This handles cases where core data was committed but module processing was interrupted
	numBlocksToRecover := uint64(10)
	if lastBlock < numBlocksToRecover {
		numBlocksToRecover = lastBlock
	}

	i.logger.Info().
		Uint64("last_block", lastBlock).
		Uint64("recovery_blocks", numBlocksToRecover).
		Msg("Starting module data recovery after restart")

	if err := i.moduleRegistry.RecoverRecentBlocks(ctx, lastBlock, numBlocksToRecover); err != nil {
		return fmt.Errorf("module recovery failed: %w", err)
	}

	return nil
}

// Close closes the indexer
func (i *Indexer) Close() error {
	close(i.shutdown)

	if i.moduleRegistry != nil {
		i.moduleRegistry.Stop()
	}

	i.db.Close()
	return nil
}
