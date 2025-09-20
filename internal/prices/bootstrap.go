package prices

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

const (
	defaultBatchSize   = 50_000
	undefinedTableCode = "42P01"
)

// LoaderConfig controls how the ZIL price bootstrap operates.
type LoaderConfig struct {
	CSVPath   string
	Source    string
	BatchSize int
}

// EnsureHistoricalPrices loads historical ZIL/USD prices when the target table is empty.
// It returns true when data was inserted.
func EnsureHistoricalPrices(ctx context.Context, pool *pgxpool.Pool, cfg LoaderConfig, logger zerolog.Logger) (bool, error) {
	var hasRows bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM prices_zil_usd_minute LIMIT 1)`).Scan(&hasRows); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == undefinedTableCode {
			return false, fmt.Errorf("price table missing; run migrations first: %w", err)
		}
		return false, fmt.Errorf("check prices table state: %w", err)
	}

	if hasRows {
		logger.Info().Msg("ZIL/USD price table already populated; skipping bootstrap")
		return false, nil
	}

	inserted, err := LoadHistoricalPrices(ctx, pool, cfg, logger)
	if err != nil {
		return false, err
	}

	if inserted == 0 {
		logger.Warn().Msg("No ZIL/USD prices imported; table remains empty")
		return false, nil
	}

	logger.Info().Int64("rows_inserted", inserted).Msg("Bootstrapped ZIL/USD prices from CSV")
	return true, nil
}

// LoadHistoricalPrices imports the bootstrap CSV into prices_zil_usd_minute.
func LoadHistoricalPrices(ctx context.Context, pool *pgxpool.Pool, cfg LoaderConfig, logger zerolog.Logger) (int64, error) {
	var (
		sourceReader  io.Reader
		closer        io.Closer
		csvPathForLog string
	)

	if cfg.CSVPath != "" {
		f, err := os.Open(cfg.CSVPath)
		if err == nil {
			sourceReader = f
			closer = f
			csvPathForLog = cfg.CSVPath
		} else if os.IsNotExist(err) && len(embeddedPriceCSV) > 0 {
			logger.Warn().Str("path", cfg.CSVPath).Msg("CSV file not found; using embedded bootstrap data")
		} else {
			return 0, fmt.Errorf("open csv %s: %w", cfg.CSVPath, err)
		}
	}

	if sourceReader == nil {
		if len(embeddedPriceCSV) == 0 {
			return 0, errors.New("no bootstrap CSV available")
		}
		sourceReader = bytes.NewReader(embeddedPriceCSV)
		csvPathForLog = "embedded"
	}

	if closer != nil {
		defer closer.Close()
	}

	if cfg.Source == "" {
		cfg.Source = "bootstrap_csv"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}

	reader := csv.NewReader(bufio.NewReader(sourceReader))
	reader.Comma = ';'
	reader.FieldsPerRecord = -1

	if _, err := reader.Read(); err != nil {
		return 0, fmt.Errorf("read csv header: %w", err)
	}

	type priceRow struct {
		ts    time.Time
		price *big.Rat
	}

	rows := make([]priceRow, 0, cfg.BatchSize)
	insertBatch := func() (int, error) {
		if len(rows) == 0 {
			return 0, nil
		}

		timestamps := make([]time.Time, len(rows))
		prices := make([]string, len(rows))
		sources := make([]string, len(rows))
		for i, r := range rows {
			timestamps[i] = r.ts
			prices[i] = r.price.FloatString(18)
			sources[i] = cfg.Source
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return 0, fmt.Errorf("begin tx: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		if _, err := tx.Exec(ctx, `
            INSERT INTO prices_zil_usd_minute (ts, price, source)
            SELECT ts, price::NUMERIC, source
            FROM UNNEST($1::TIMESTAMPTZ[], $2::TEXT[], $3::TEXT[]) AS t(ts, price, source)
            ON CONFLICT (ts, source) DO NOTHING
        `, timestamps, prices, sources); err != nil {
			return 0, fmt.Errorf("bulk insert failed: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit tx: %w", err)
		}

		inserted := len(rows)
		rows = rows[:0]
		return inserted, nil
	}

	var totalInserted int64
	var csvLines int64
	importStart := time.Now()

	logger.Info().
		Str("csv", csvPathForLog).
		Str("source", cfg.Source).
		Int("batch_size", cfg.BatchSize).
		Msg("Starting ZIL/USD price import")

	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return totalInserted, fmt.Errorf("csv read error: %w", err)
		}

		csvLines++

		if len(record) < 8 {
			logger.Warn().Interface("record", record).Msg("Skipping short CSV row")
			continue
		}

		tCloseMs, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			logger.Warn().Str("value", record[1]).Msg("Invalid close timestamp; skipping row")
			continue
		}
		openMs, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			logger.Warn().Str("value", record[0]).Msg("Invalid open timestamp; skipping row")
			continue
		}

		priceCloseStr := strings.ReplaceAll(record[7], ",", ".")
		priceRat := new(big.Rat)
		if _, ok := priceRat.SetString(priceCloseStr); !ok {
			logger.Warn().Str("price", priceCloseStr).Msg("Invalid price; skipping row")
			continue
		}

		startMinute := time.UnixMilli(openMs).UTC().Truncate(time.Minute)
		endMinute := time.UnixMilli(tCloseMs).UTC().Truncate(time.Minute)
		for t := startMinute; !t.After(endMinute); t = t.Add(time.Minute) {
			if ctx.Err() != nil {
				return totalInserted, ctx.Err()
			}

			rows = append(rows, priceRow{ts: t, price: priceRat})
			if len(rows) >= cfg.BatchSize {
				inserted, err := insertBatch()
				if err != nil {
					return totalInserted, err
				}
				totalInserted += int64(inserted)

				elapsed := time.Since(importStart)
				if elapsed > 0 {
					logger.Info().
						Int64("csv_lines_read", csvLines).
						Int64("rows_inserted", totalInserted).
						Dur("elapsed", elapsed).
						Float64("rows_per_second", float64(totalInserted)/elapsed.Seconds()).
						Time("current_minute", t).
						Msg("ZIL/USD price import progress")
				}
			}
		}
	}

	inserted, err := insertBatch()
	if err != nil {
		return totalInserted, err
	}
	totalInserted += int64(inserted)

	elapsed := time.Since(importStart)
	rate := 0.0
	if elapsed > 0 {
		rate = float64(totalInserted) / elapsed.Seconds()
	}

	logger.Info().
		Int64("csv_lines_read", csvLines).
		Int64("rows_inserted", totalInserted).
		Dur("elapsed", elapsed).
		Float64("rows_per_second", rate).
		Msg("ZIL/USD price import completed")

	return totalInserted, nil
}
