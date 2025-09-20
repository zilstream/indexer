package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/prices"
)

func main() {
	var (
		configPath string
		csvPath    string
		source     string
		batchSize  int
	)
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.StringVar(&csvPath, "csv", "data/zilliqa_historical_prices.csv", "Path to ZIL/USD historical CSV")
	flag.StringVar(&source, "source", "bootstrap_csv", "Source label stored in DB")
	flag.IntVar(&batchSize, "batch-size", 50_000, "Number of minute rows to insert per transaction")
	flag.Parse()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	inserted, err := prices.LoadHistoricalPrices(ctx, db.Pool(), prices.LoaderConfig{
		CSVPath:   csvPath,
		Source:    source,
		BatchSize: batchSize,
	}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load ZIL/USD prices")
	}

	logger.Info().Int64("rows_inserted", inserted).Msg("CSV import finished")
}
