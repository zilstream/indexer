package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/core"
	"github.com/zilstream/indexer/internal/modules/loader"
	"github.com/zilstream/indexer/internal/modules/uniswapv2"
)

func main() {
	var (
		configPath string
		moduleName string
		fromBlock  uint64
		toBlock    uint64
	)

	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.StringVar(&moduleName, "module", "", "Module name to backfill")
	flag.Uint64Var(&fromBlock, "from", 0, "Starting block")
	flag.Uint64Var(&toBlock, "to", 0, "Ending block")
	flag.Parse()

	if moduleName == "" {
		fmt.Fprintf(os.Stderr, "Module name is required\n")
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
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	// Connect to database
	db, err := database.NewDatabase(cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	ctx := context.Background()

	// Create module registry
	registry := core.NewModuleRegistry(db, logger)

	// Load and register the module
	manifestLoader := loader.NewManifestLoader(logger)
	manifests, err := manifestLoader.LoadFromDirectory("manifests")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load manifests")
	}

	for _, manifest := range manifests {
		if manifest.Name == moduleName {
			// Create the module based on type
			var module core.Module
			switch manifest.Type {
			case "uniswap-v2":
				module, err = uniswapv2.NewUniswapV2Module(logger)
				if err != nil {
					logger.Fatal().Err(err).Msg("Failed to create module")
				}
			default:
				logger.Fatal().Str("type", manifest.Type).Msg("Unknown module type")
			}

			// Initialize and register
			if err := module.Initialize(ctx, db); err != nil {
				logger.Fatal().Err(err).Msg("Failed to initialize module")
			}

			if err := registry.RegisterModule(module); err != nil {
				logger.Fatal().Err(err).Msg("Failed to register module")
			}

			// If no range specified, get from database
			if fromBlock == 0 || toBlock == 0 {
				var minBlock, maxBlock uint64
				row := db.Pool().QueryRow(ctx, "SELECT MIN(block_number), MAX(block_number) FROM event_logs")
				if err := row.Scan(&minBlock, &maxBlock); err != nil {
					logger.Fatal().Err(err).Msg("Failed to get block range")
				}
				if fromBlock == 0 {
					fromBlock = minBlock
				}
				if toBlock == 0 {
					toBlock = maxBlock
				}
			}

			logger.Info().
				Str("module", moduleName).
				Uint64("from", fromBlock).
				Uint64("to", toBlock).
				Msg("Starting backfill")

			// Trigger the backfill
			if err := registry.TriggerBackfill(moduleName, fromBlock, toBlock); err != nil {
				logger.Fatal().Err(err).Msg("Backfill failed")
			}

			logger.Info().Msg("Backfill triggered successfully")
			return
		}
	}

	logger.Fatal().Str("module", moduleName).Msg("Module not found in manifests")
}