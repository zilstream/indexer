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

	s.logger.Info().Msg("Token metrics scheduler started (runs every 5 minutes)")
	s.scheduler.Start()

	// Run immediately on startup
	go s.updateAllTokenMetrics(ctx)

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
