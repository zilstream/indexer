package scheduler

import (
	"context"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
)

type TokenMetricsScheduler struct {
	db        *pgxpool.Pool
	scheduler gocron.Scheduler
	logger    zerolog.Logger
}

func NewTokenMetricsScheduler(db *pgxpool.Pool, logger zerolog.Logger) (*TokenMetricsScheduler, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, err
	}

	return &TokenMetricsScheduler{
		db:        db,
		scheduler: s,
		logger:    logger.With().Str("component", "token-metrics-scheduler").Logger(),
	}, nil
}

func (s *TokenMetricsScheduler) Start(ctx context.Context) error {
	// Schedule token metrics update every 5 minutes
	_, err := s.scheduler.NewJob(
		gocron.DurationJob(5*time.Minute),
		gocron.NewTask(s.updateAllTokenMetrics, ctx),
		gocron.WithName("update-token-metrics"),
	)
	if err != nil {
		return err
	}

	// Schedule pair metrics update every 5 minutes
	_, err = s.scheduler.NewJob(
		gocron.DurationJob(5*time.Minute),
		gocron.NewTask(s.updateAllPairMetrics, ctx),
		gocron.WithName("update-pair-metrics"),
	)
	if err != nil {
		return err
	}

	s.logger.Info().Msg("Token and pair metrics scheduler started (runs every 5 minutes)")
	s.scheduler.Start()

	// Run immediately on startup
	go s.updateAllTokenMetrics(ctx)
	go s.updateAllPairMetrics(ctx)

	return nil
}

func (s *TokenMetricsScheduler) Stop() {
	s.logger.Info().Msg("Stopping token metrics scheduler")
	if err := s.scheduler.Shutdown(); err != nil {
		s.logger.Error().Err(err).Msg("Error shutting down scheduler")
	}
}

func (s *TokenMetricsScheduler) updateAllTokenMetrics(ctx context.Context) {
	s.logger.Info().Msg("Starting token metrics update")
	start := time.Now()

	// Get all token addresses
	rows, err := s.db.Query(ctx, "SELECT address FROM tokens")
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to query tokens")
		return
	}

	var tokens []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			s.logger.Error().Err(err).Msg("Failed to scan token address")
			continue
		}
		tokens = append(tokens, addr)
	}
	rows.Close()

	s.logger.Info().Int("count", len(tokens)).Msg("Updating token metrics")

	// Update each token
	successCount := 0
	for _, addr := range tokens {
		if err := database.UpdateTokenMetrics(ctx, s.db, addr); err != nil {
			s.logger.Error().Err(err).Str("token", addr).Msg("Failed to update token metrics")
			continue
		}
		successCount++
	}

	s.logger.Info().
		Int("success", successCount).
		Int("failed", len(tokens)-successCount).
		Int("total", len(tokens)).
		Dur("duration", time.Since(start)).
		Msg("Token metrics update completed")
}

func (s *TokenMetricsScheduler) updateAllPairMetrics(ctx context.Context) {
	s.logger.Info().Msg("Starting pair metrics update")
	start := time.Now()

	// Get all pairs from both V2 and V3
	rows, err := s.db.Query(ctx, "SELECT protocol, address FROM dex_pools")
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to query pairs")
		return
	}

	type pairInfo struct {
		protocol string
		address  string
	}
	var pairs []pairInfo
	for rows.Next() {
		var p pairInfo
		if err := rows.Scan(&p.protocol, &p.address); err != nil {
			s.logger.Error().Err(err).Msg("Failed to scan pair")
			continue
		}
		pairs = append(pairs, p)
	}
	rows.Close()

	s.logger.Info().Int("count", len(pairs)).Msg("Updating pair metrics")

	// Update each pair
	successCount := 0
	for _, p := range pairs {
		if err := database.UpdatePairMetrics(ctx, s.db, p.address, p.protocol); err != nil {
			s.logger.Error().Err(err).Str("pair", p.address).Str("protocol", p.protocol).Msg("Failed to update pair metrics")
			continue
		}
		successCount++
	}

	s.logger.Info().
		Int("success", successCount).
		Int("failed", len(pairs)-successCount).
		Int("total", len(pairs)).
		Dur("duration", time.Since(start)).
		Msg("Pair metrics update completed")
}
