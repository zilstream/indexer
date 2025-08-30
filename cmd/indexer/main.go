package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/processor"
)

func main() {
	// Parse command-line flags
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
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
		Str("config", configPath).
		Msg("Starting Zilstream Indexer")

	// Create and start indexer
	indexer, err := processor.NewIndexer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create indexer")
	}

	// Start indexer (blocks until shutdown)
	if err := indexer.Start(); err != nil {
		logger.Fatal().Err(err).Msg("Indexer failed")
	}

	logger.Info().Msg("Indexer shutdown complete")
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
		logger = zerolog.New(output).Level(level).With().Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stdout).Level(level).With().Timestamp().Caller().Logger()
	}

	return logger
}