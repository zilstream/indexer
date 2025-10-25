package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	tokenAddress := flag.String("token", "", "specific token address to update (if empty, updates all)")
	flag.Parse()

	// Setup logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}

	// Connect to database
	ctx := context.Background()
	db, err := database.New(ctx, &cfg.Database, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	log.Info().Msg("Updating token metrics...")

	if *tokenAddress != "" {
		// Update specific token
		log.Info().Str("token", *tokenAddress).Msg("Updating single token")
		if err := database.UpdateTokenMetrics(ctx, db.Pool(), *tokenAddress); err != nil {
			log.Fatal().Err(err).Str("token", *tokenAddress).Msg("Failed to update token")
		}
		log.Info().Str("token", *tokenAddress).Msg("Token updated successfully")
	} else {
		// Update all tokens
		log.Info().Msg("Updating all tokens")
		
		// Get all token addresses
		rows, err := db.Pool().Query(ctx, "SELECT address FROM tokens")
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to query tokens")
		}
		
		var tokens []string
		for rows.Next() {
			var addr string
			if err := rows.Scan(&addr); err != nil {
				log.Error().Err(err).Msg("Failed to scan token address")
				continue
			}
			tokens = append(tokens, addr)
		}
		rows.Close()
		
		log.Info().Int("count", len(tokens)).Msg("Found tokens to update")
		
		// Update each token
		successCount := 0
		for i, addr := range tokens {
			if err := database.UpdateTokenMetrics(ctx, db.Pool(), addr); err != nil {
				log.Error().Err(err).Str("token", addr).Msg("Failed to update token")
				continue
			}
			successCount++
			if (i+1)%10 == 0 {
				log.Info().Int("processed", i+1).Int("total", len(tokens)).Msg("Progress")
			}
		}
		
		log.Info().
			Int("success", successCount).
			Int("failed", len(tokens)-successCount).
			Int("total", len(tokens)).
			Msg("Finished updating tokens")
	}

	fmt.Println("✓ Token metrics updated successfully")
}
