package main

import (
	"context"
	"fmt"
	"log"

	"os"
	
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/config"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/uniswapv2"
)

func main() {
	// Load config
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Setup logger
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	ctx := context.Background()

	// Connect to database
	db, err := database.New(ctx, &cfg.Database, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create uniswap module
	module, err := uniswapv2.NewUniswapV2Module(logger)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize module
	if err := module.Initialize(ctx, db); err != nil {
		log.Fatal(err)
	}

	// Check current state
	fmt.Println("Checking current state...")
	
	var pairCount int
	row := db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM uniswap_v2_pairs")
	row.Scan(&pairCount)
	fmt.Printf("Current pairs: %d\n", pairCount)

	var eventCount int
	row = db.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM event_logs 
		WHERE topics->>0 = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
	`)
	row.Scan(&eventCount)
	fmt.Printf("PairCreated events in DB: %d\n", eventCount)

	// Run backfill for the specific block range
	fmt.Println("\nRunning backfill from block 3251550 to 3251560...")
	if err := module.Backfill(ctx, 3251550, 3251560); err != nil {
		log.Fatal("Backfill failed:", err)
	}

	// Check results
	row = db.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM uniswap_v2_pairs")
	row.Scan(&pairCount)
	fmt.Printf("\nPairs after backfill: %d\n", pairCount)

	// List pairs
	if pairCount > 0 {
		rows, err := db.Pool().Query(ctx, `
			SELECT address, token0, token1, created_at_block 
			FROM uniswap_v2_pairs
		`)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		fmt.Println("\nCreated pairs:")
		for rows.Next() {
			var pairAddr, token0, token1 string
			var createdBlock int64
			rows.Scan(&pairAddr, &token0, &token1, &createdBlock)
			fmt.Printf("  Pair: %s (token0: %s, token1: %s, block: %d)\n", pairAddr, token0, token1, createdBlock)
		}
	}
}