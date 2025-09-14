package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/core"
	"github.com/zilstream/indexer/internal/modules/loader"
	"github.com/zilstream/indexer/internal/modules/uniswapv2"
	"github.com/zilstream/indexer/internal/modules/uniswapv3"
	"github.com/zilstream/indexer/internal/prices"
)

func main() {
	var (
		configPath string
		moduleName string
		fromBlock  uint64
		toBlock    uint64
	)

	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.StringVar(&moduleName, "module", "", "Module name to backfill (e.g. uniswap-v2, uniswap-v3)")
	flag.Uint64Var(&fromBlock, "from", 0, "Starting block (optional)")
	flag.Uint64Var(&toBlock, "to", 0, "Ending block (optional)")
	flag.Parse()

	if moduleName == "" {
		fmt.Fprintln(os.Stderr, "--module is required (uniswap-v2 or uniswap-v3)")
		os.Exit(1)
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).
		Level(zerolog.InfoLevel).
		With().Timestamp().Logger()
	log.Logger = logger

	ctx := context.Background()

	// Connect to database
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	// Determine block range from event_logs if not provided
	if fromBlock == 0 || toBlock == 0 {
		var minBlock, maxBlock uint64
		row := db.Pool().QueryRow(ctx, "SELECT COALESCE(MIN(block_number),0), COALESCE(MAX(block_number),0) FROM event_logs")
		if err := row.Scan(&minBlock, &maxBlock); err != nil {
			logger.Fatal().Err(err).Msg("Failed to get block range from event_logs")
		}
		if fromBlock == 0 { fromBlock = minBlock }
		if toBlock == 0 { toBlock = maxBlock }
	}
	if fromBlock == 0 || toBlock == 0 || fromBlock > toBlock {
		logger.Fatal().Uint64("from", fromBlock).Uint64("to", toBlock).Msg("Invalid block range")
	}

	// Load manifests to align module configuration
	manifestLoader := loader.NewManifestLoader(logger)
	manifests, err := manifestLoader.LoadFromDirectory("manifests")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load manifests")
	}
	var manifest *core.Manifest
	for _, m := range manifests {
		if m.Name == moduleName {
			manifest = m
			break
		}
	}
	if manifest == nil {
		logger.Fatal().Str("module", moduleName).Msg("Module not found in manifests")
	}

	// Instantiate module
	var module core.Module
	switch moduleName {
	case "uniswap-v2":
		m, err := uniswapv2.NewUniswapV2Module(logger)
		if err != nil { logger.Fatal().Err(err).Msg("Failed to create UniswapV2 module") }
		m.SetPriceProvider(prices.NewPostgresProvider(db.Pool(), 10_000))
		module = m
	case "uniswap-v3":
		m, err := uniswapv3.NewUniswapV3Module(logger)
		if err != nil { logger.Fatal().Err(err).Msg("Failed to create UniswapV3 module") }
		prov := prices.NewPostgresProvider(db.Pool(), 10_000)
		m.SetPriceProvider(prov)
		// Price router is constructed during Initialize when price provider is set
		module = m
	default:
		logger.Fatal().Str("module", moduleName).Msg("Unsupported module")
	}

	// Initialize module and run backfill directly (synchronous)
	if err := module.Initialize(ctx, db); err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize module")
	}

	logger.Info().Str("module", moduleName).Uint64("from", fromBlock).Uint64("to", toBlock).Msg("Starting backfill")
	if err := module.Backfill(ctx, fromBlock, toBlock); err != nil {
		logger.Fatal().Err(err).Msg("Backfill failed")
	}
	logger.Info().Msg("Backfill completed successfully")
}
