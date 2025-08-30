package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
	"github.com/zilstream/indexer/internal/sync"
)

func main() {
	// Parse command-line flags
	var (
		configPath        string
		startBlock        uint64
		endBlock          uint64
		batchSize         int
		workers           int
		bufferSize        int
		skipReceipts      bool
		skipReceiptsBelow uint64
		rateLimit         int
		optimizeBatch     bool
	)
	
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.Uint64Var(&startBlock, "start", 0, "Start block number")
	flag.Uint64Var(&endBlock, "end", 0, "End block number (0 = latest)")
	flag.IntVar(&batchSize, "batch", 20, "Blocks per batch")
	flag.IntVar(&workers, "workers", 10, "Number of parallel workers")
	flag.IntVar(&bufferSize, "buffer", 1000, "Bulk write buffer size")
	flag.BoolVar(&skipReceipts, "skip-receipts", true, "Skip fetching receipts")
	flag.Uint64Var(&skipReceiptsBelow, "skip-receipts-below", 8000000, "Skip receipts for blocks below this number")
	flag.IntVar(&rateLimit, "rate", 50, "Requests per second")
	flag.BoolVar(&optimizeBatch, "optimize", true, "Auto-optimize batch size")
	flag.Parse()
	
	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	
	// Set up logger
	logger := setupLogger(cfg.Logging)
	
	// Log startup information
	logger.Info().
		Str("version", "0.1.0").
		Uint64("start_block", startBlock).
		Uint64("end_block", endBlock).
		Int("batch_size", batchSize).
		Int("workers", workers).
		Bool("skip_receipts", skipReceipts).
		Msg("Starting Fast Sync")
	
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info().Msg("Received shutdown signal")
		cancel()
	}()
	
	// Connect to database
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()
	
	// Create RPC client
	rpcClient, err := rpc.NewClient(cfg.Chain.RPCEndpoint, cfg.Chain.ChainID, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create RPC client")
	}
	defer rpcClient.Close()
	
	// Get end block if not specified
	if endBlock == 0 {
		latestBlock, err := rpcClient.GetLatestBlockNumber(ctx)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to get latest block")
		}
		endBlock = latestBlock
		logger.Info().Uint64("end_block", endBlock).Msg("Using latest block as end")
	}
	
	// Validate range
	if startBlock > endBlock {
		logger.Fatal().
			Uint64("start", startBlock).
			Uint64("end", endBlock).
			Msg("Invalid range: start > end")
	}
	
	// Calculate total blocks
	totalBlocks := endBlock - startBlock + 1
	logger.Info().
		Uint64("total_blocks", totalBlocks).
		Float64("estimated_minutes", float64(totalBlocks)/60000).
		Msg("Sync range calculated")
	
	// Create fast sync config
	fastSyncConfig := sync.FastSyncConfig{
		BatchSize:         batchSize,
		WorkerCount:       workers,
		BufferSize:        bufferSize,
		SkipReceipts:      skipReceipts,
		SkipReceiptsBelow: skipReceiptsBelow,
		RequestsPerSecond: rateLimit,
		OptimizeBatchSize: optimizeBatch,
		LogBlockRange:     5000,
	}
	
	// Create fast sync coordinator
	fastSync := sync.NewFastSync(fastSyncConfig, db, rpcClient, logger)
	defer fastSync.Close()
	
	// Start fast sync
	startTime := time.Now()
	err = fastSync.SyncRange(ctx, startBlock, endBlock)
	elapsed := time.Since(startTime)
	
	if err != nil {
		if err == context.Canceled {
			logger.Info().Msg("Fast sync cancelled by user")
		} else {
			logger.Error().Err(err).Msg("Fast sync failed")
			os.Exit(1)
		}
	} else {
		blocksPerSec := float64(totalBlocks) / elapsed.Seconds()
		logger.Info().
			Uint64("blocks_synced", totalBlocks).
			Dur("total_time", elapsed).
			Float64("blocks_per_second", blocksPerSec).
			Msg("Fast sync completed successfully!")
	}
}

func setupLogger(cfg config.LoggingConfig) zerolog.Logger {
	// Set time format
	zerolog.TimeFieldFormat = time.RFC3339Nano
	
	// Parse log level
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	
	// Configure output format
	var logger zerolog.Logger
	if cfg.Format == "console" {
		output := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "15:04:05.000",
		}
		logger = zerolog.New(output).Level(level).With().Timestamp().Logger()
	} else {
		logger = zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
	}
	
	return logger
}