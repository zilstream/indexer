package prices

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

const (
	defaultPollInterval = 5 * time.Minute
	pollerSource        = "coingecko"
)

type Poller struct {
	pool     *pgxpool.Pool
	client   *CoinGeckoClient
	interval time.Duration
	logger   zerolog.Logger
}

type PollerConfig struct {
	Interval time.Duration
}

func NewPoller(pool *pgxpool.Pool, cfg PollerConfig, logger zerolog.Logger) *Poller {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultPollInterval
	}

	return &Poller{
		pool:     pool,
		client:   NewCoinGeckoClient(),
		interval: cfg.Interval,
		logger:   logger.With().Str("component", "price_poller").Logger(),
	}
}

func (p *Poller) Start(ctx context.Context) error {
	p.logger.Info().Dur("interval", p.interval).Msg("Starting ZIL/USD price poller")
	
	// Immediate poll on startup in a goroutine to avoid blocking
	go func() {
		if err := p.pollOnce(context.Background()); err != nil {
			p.logger.Error().Err(err).Msg("Initial price poll failed")
		}
	}()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info().Msg("Price poller shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := p.pollOnce(ctx); err != nil {
				p.logger.Error().Err(err).Msg("Failed to poll ZIL price")
			}
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context) error {
	startTime := time.Now()

	p.logger.Debug().Msg("Fetching ZIL/USD price from CoinGecko")

	price, err := p.client.FetchZILUSD(ctx)
	if err != nil {
		return fmt.Errorf("fetch price from CoinGecko: %w", err)
	}

	p.logger.Debug().Float64("price", price).Msg("Received price from CoinGecko")

	ts := time.Now().UTC().Truncate(time.Minute)

	if err := p.storePrice(ctx, ts, price); err != nil {
		return fmt.Errorf("store price: %w", err)
	}

	p.logger.Info().
		Float64("price_usd", price).
		Time("timestamp", ts).
		Dur("fetch_duration", time.Since(startTime)).
		Msg("Successfully polled and stored ZIL/USD price")

	return nil
}

func (p *Poller) storePrice(ctx context.Context, ts time.Time, price float64) error {
	query := `
		INSERT INTO prices_zil_usd_minute (ts, price, source)
		VALUES ($1, $2, $3)
		ON CONFLICT (ts, source) DO UPDATE
		SET price = EXCLUDED.price, inserted_at = NOW()
	`

	p.logger.Debug().
		Time("ts", ts).
		Float64("price", price).
		Str("source", pollerSource).
		Msg("Storing price in database")

	result, err := p.pool.Exec(ctx, query, ts, price, pollerSource)
	if err != nil {
		return fmt.Errorf("insert price: %w", err)
	}

	p.logger.Debug().
		Int64("rows_affected", result.RowsAffected()).
		Msg("Price stored successfully")

	return nil
}
