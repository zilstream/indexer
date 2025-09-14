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
	"github.com/zilstream/indexer/internal/api"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/processor"
)

func main() {
	var configPath string
	var role string
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.StringVar(&role, "role", "both", "Role to run: api | worker | both")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	logger := setupLogger(cfg.Logging)
	logger.Info().Str("version", "0.1.0").Str("config", configPath).Str("role", role).Msg("Starting Zilstream Server")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Database pool
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect database")
	}
	defer db.Close()

	// Build API server
	apiServer := api.NewAPIServer(db.Pool(), logger)

	switch role {
	case "api":
		startAPI(ctx, apiServer, cfg.Server.Port, logger)
	case "worker":
		startWorker(ctx, cfg, logger)
	case "both":
		if cfg.Server.RunIndexer {
			go startWorker(ctx, cfg, logger)
		}
		startAPI(ctx, apiServer, cfg.Server.Port, logger)
	default:
		logger.Fatal().Str("role", role).Msg("invalid role, use api|worker|both")
	}
}

func startAPI(ctx context.Context, s *api.APIServer, port int, logger zerolog.Logger) {
	addr := fmt.Sprintf(":%d", port)
	if err := s.Start(ctx, addr); err != nil {
		logger.Fatal().Err(err).Msg("API server failed")
	}
}

func startWorker(ctx context.Context, cfg *config.Config, logger zerolog.Logger) {
	indexer, err := processor.NewIndexer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create indexer")
	}
	if err := indexer.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Indexer failed")
	}
}

func setupLogger(cfg config.LoggingConfig) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil { level = zerolog.InfoLevel }
	var logger zerolog.Logger
	if cfg.Format == "console" {
		output := zerolog.ConsoleWriter{ Out: os.Stdout, TimeFormat: "15:04:05.000" }
		logger = zerolog.New(output).Level(level).With().Timestamp().Caller().Logger()
	} else {
		logger = zerolog.New(os.Stdout).Level(level).With().Timestamp().Caller().Logger()
	}
	return logger
}
