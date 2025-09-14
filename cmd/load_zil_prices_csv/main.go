package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
)

// csv columns: timeOpen;timeClose;timeHigh;timeLow;priceOpen;priceHigh;priceLow;priceClose;volume
// times are epoch milliseconds; decimal comma for prices and volume

func main() {
	var (
		configPath string
		csvPath    string
		source     string
	)
	flag.StringVar(&configPath, "config", "config.yaml", "Path to configuration file")
	flag.StringVar(&csvPath, "csv", "data/zilliqa_historical_prices.csv", "Path to ZIL/USD historical CSV")
	flag.StringVar(&source, "source", "bootstrap_csv", "Source label stored in DB")
	flag.Parse()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	// Connect DB
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	f, err := os.Open(csvPath)
	if err != nil {
		logger.Fatal().Err(err).Str("path", csvPath).Msg("Failed to open CSV")
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = ';'
	reader.FieldsPerRecord = -1

	// read header
	if _, err := reader.Read(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to read CSV header")
	}

	batchSize := 50_000 // Increased batch size for better performance
	rows := make([]struct{ ts time.Time; price *big.Rat }, 0, batchSize)
	insertBatch := func() error {
		if len(rows) == 0 { return nil }

		startTime := time.Now()

		// Use high-performance UNNEST bulk insert instead of individual INSERTs
		timestamps := make([]time.Time, len(rows))
		prices := make([]string, len(rows)) // Convert to string for PostgreSQL NUMERIC
		sources := make([]string, len(rows))

		for i, r := range rows {
			timestamps[i] = r.ts
			// Convert big.Rat to decimal string for PostgreSQL
			prices[i] = r.price.FloatString(18) // 18 decimal places precision
			sources[i] = source
		}

		// Use a transaction for better performance and atomicity
		tx, err := db.Pool().Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		// Single bulk INSERT with UNNEST - much faster than individual INSERTs
		_, err = tx.Exec(ctx, `
			INSERT INTO prices_zil_usd_minute (ts, price, source)
			SELECT ts, price::NUMERIC, source
			FROM UNNEST($1::TIMESTAMPTZ[], $2::TEXT[], $3::TEXT[]) AS t(ts, price, source)
			ON CONFLICT (ts, source) DO NOTHING
		`, timestamps, prices, sources)

		if err != nil {
			return fmt.Errorf("bulk insert failed: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		elapsed := time.Since(startTime)
		logger.Info().
			Int("batch_size", len(rows)).
			Dur("elapsed", elapsed).
			Float64("records_per_sec", float64(len(rows))/elapsed.Seconds()).
			Msg("Batch inserted successfully")

		rows = rows[:0]
		return nil
	}

	var imported int64
	var csvLinesRead int64
	importStartTime := time.Now()

	logger.Info().
		Int("batch_size", batchSize).
		Str("source", source).
		Msg("Starting CSV import with optimized bulk inserts")

	for {
		rec, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" { break }
			logger.Fatal().Err(err).Msg("CSV read error")
		}

		csvLinesRead++

		if len(rec) < 8 {
			logger.Warn().Interface("rec", rec).Msg("Skipping short row")
			continue
		}

		// timeClose is at index 1; priceClose at index 7
		tCloseMs, _ := strconv.ParseInt(rec[1], 10, 64)
		priceCloseStr := strings.ReplaceAll(rec[7], ",", ".")

		// Parse price as big.Rat for proper numeric handling
		priceRat := new(big.Rat)
		if _, ok := priceRat.SetString(priceCloseStr); !ok {
			logger.Warn().Str("price", priceCloseStr).Msg("Invalid price, skipping row")
			continue
		}

		// forward-fill the entire day into minutes using the close price
		openMs, _ := strconv.ParseInt(rec[0], 10, 64)
		start := time.UnixMilli(openMs).UTC().Truncate(time.Minute)
		end := time.UnixMilli(tCloseMs).UTC().Truncate(time.Minute)

		for t := start; !t.After(end); t = t.Add(time.Minute) {
			rows = append(rows, struct{ ts time.Time; price *big.Rat }{ts: t, price: priceRat})

			if len(rows) >= batchSize {
				if err := insertBatch(); err != nil {
					logger.Fatal().Err(err).Msg("DB insert batch failed")
				}

				imported += int64(batchSize)
				elapsed := time.Since(importStartTime)
				overallRate := float64(imported) / elapsed.Seconds()

				// Log progress every batch
				logger.Info().
					Int64("csv_lines_read", csvLinesRead).
					Int64("imported", imported).
					Dur("elapsed", elapsed).
					Float64("overall_records_per_sec", overallRate).
					Time("current_minute", t).
					Msg("Bulk import progress")
			}
		}
	}
	// flush final batch
	finalBatchSize := len(rows)
	if err := insertBatch(); err != nil {
		logger.Fatal().Err(err).Msg("DB insert final batch failed")
	}
	imported += int64(finalBatchSize)

	totalElapsed := time.Since(importStartTime)
	overallRate := float64(imported) / totalElapsed.Seconds()

	logger.Info().
		Int64("csv_lines_read", csvLinesRead).
		Int64("imported_total", imported).
		Int("final_batch_size", finalBatchSize).
		Dur("total_elapsed", totalElapsed).
		Float64("overall_records_per_sec", overallRate).
		Msg("CSV import completed successfully")
}
