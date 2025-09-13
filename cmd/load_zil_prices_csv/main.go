package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
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

	batchSize := 10_000
	rows := make([]struct{ ts time.Time; price string }, 0, batchSize)
	insertBatch := func() error {
		if len(rows) == 0 { return nil }
		// build copy via batched INSERT to avoid adding new deps; use UNNEST
		// For simplicity, use one INSERT per row; acceptable for initial import sizes. Optimize later if needed.
		for _, r := range rows {
			_, err := db.Pool().Exec(ctx, `
				INSERT INTO prices_zil_usd_minute (ts, price, source)
				VALUES ($1, $2, $3)
				ON CONFLICT (ts, source) DO NOTHING
			`, r.ts, r.price, source)
			if err != nil { return err }
		}
		rows = rows[:0]
		return nil
	}

	var imported int64
	for {
		rec, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" { break }
			logger.Fatal().Err(err).Msg("CSV read error")
		}
		if len(rec) < 8 {
			logger.Warn().Interface("rec", rec).Msg("Skipping short row")
			continue
		}
		// timeClose is at index 1; priceClose at index 7
		tCloseMs, _ := strconv.ParseInt(rec[1], 10, 64)
		priceCloseStr := strings.ReplaceAll(rec[7], ",", ".")
		// parse as float to validate
		if _, err := strconv.ParseFloat(priceCloseStr, 64); err != nil {
			logger.Warn().Str("price", priceCloseStr).Msg("Invalid price, skipping row")
			continue
		}
		// forward-fill the entire day into minutes using the close price
		openMs, _ := strconv.ParseInt(rec[0], 10, 64)
		start := time.UnixMilli(openMs).UTC().Truncate(time.Minute)
		end := time.UnixMilli(tCloseMs).UTC().Truncate(time.Minute)
		for t := start; !t.After(end); t = t.Add(time.Minute) {
			rows = append(rows, struct{ ts time.Time; price string }{ts: t, price: priceCloseStr})
			if len(rows) >= batchSize {
				if err := insertBatch(); err != nil {
					logger.Fatal().Err(err).Msg("DB insert batch failed")
				}
				imported += int64(batchSize)
				logger.Info().Int64("imported", imported).Time("through_minute", t).Msg("Imported rows")
			}
		}
	}
	// flush
	if err := insertBatch(); err != nil {
		logger.Fatal().Err(err).Msg("DB insert final batch failed")
	}
	imported += int64(len(rows))
	logger.Info().Int64("imported_total", imported).Msg("Import finished")
}
