package database

import (
	"context"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
)

// BlockRepository handles block-related database operations
type BlockRepository struct {
	db *Database
}

func NewBlockRepository(db *Database) *BlockRepository {
	return &BlockRepository{db: db}
}

// Insert inserts a single block into the database
func (r *BlockRepository) Insert(ctx context.Context, block *Block) error {
	query := `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used, transaction_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (number) DO NOTHING`

	_, err := r.db.pool.Exec(ctx, query,
		block.Number,
		block.Hash,
		block.ParentHash,
		block.Timestamp,
		block.GasLimit,
		block.GasUsed,
		block.TransactionCount,
	)

	if err != nil {
		return fmt.Errorf("failed to insert block: %w", err)
	}

	return nil
}

// InsertBatch inserts multiple blocks in a single batch
func (r *BlockRepository) InsertBatch(ctx context.Context, blocks []*Block) error {
	if len(blocks) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used, transaction_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (number) DO NOTHING`

	for _, block := range blocks {
		batch.Queue(query,
			block.Number,
			block.Hash,
			block.ParentHash,
			block.Timestamp,
			block.GasLimit,
			block.GasUsed,
			block.TransactionCount,
		)
	}

	br := r.db.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert block %d: %w", blocks[i].Number, err)
		}
	}

	return nil
}

// GetByNumber retrieves a block by its number
func (r *BlockRepository) GetByNumber(ctx context.Context, number uint64) (*Block, error) {
	var block Block
	query := `
		SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, transaction_count, created_at
		FROM blocks
		WHERE number = $1`

	err := r.db.pool.QueryRow(ctx, query, number).Scan(
		&block.Number,
		&block.Hash,
		&block.ParentHash,
		&block.Timestamp,
		&block.GasLimit,
		&block.GasUsed,
		&block.TransactionCount,
		&block.CreatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block by number: %w", err)
	}

	return &block, nil
}

// TransactionRepository handles transaction-related database operations
type TransactionRepository struct {
	db *Database
}

func NewTransactionRepository(db *Database) *TransactionRepository {
	return &TransactionRepository{db: db}
}

// InsertBatch inserts multiple transactions in a single batch
func (r *TransactionRepository) InsertBatch(ctx context.Context, transactions []*Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO transactions (
			hash, block_number, transaction_index, from_address, to_address,
			value, gas_price, gas_limit, gas_used, nonce, input, status,
			transaction_type, original_type_hex
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (hash) DO NOTHING`

	for _, tx := range transactions {
		batch.Queue(query,
			tx.Hash,
			tx.BlockNumber,
			tx.TransactionIndex,
			tx.FromAddress,
			tx.ToAddress,
			BigIntToNumeric(tx.Value),
			BigIntToNumeric(tx.GasPrice),
			tx.GasLimit,
			tx.GasUsed,
			tx.Nonce,
			tx.Input,
			tx.Status,
			tx.TransactionType,
			tx.OriginalTypeHex,
		)
	}

	br := r.db.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert transaction %s: %w", transactions[i].Hash, err)
		}
	}

	return nil
}

// GetByHash retrieves a transaction by its hash
func (r *TransactionRepository) GetByHash(ctx context.Context, hash string) (*Transaction, error) {
	var tx Transaction
	var valueStr, gasPriceStr *string

	query := `
		SELECT hash, block_number, transaction_index, from_address, to_address,
		       value, gas_price, gas_limit, gas_used, nonce, input, status, created_at
		FROM transactions
		WHERE hash = $1`

	err := r.db.pool.QueryRow(ctx, query, hash).Scan(
		&tx.Hash,
		&tx.BlockNumber,
		&tx.TransactionIndex,
		&tx.FromAddress,
		&tx.ToAddress,
		&valueStr,
		&gasPriceStr,
		&tx.GasLimit,
		&tx.GasUsed,
		&tx.Nonce,
		&tx.Input,
		&tx.Status,
		&tx.CreatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get transaction by hash: %w", err)
	}

	// Convert string values back to big.Int
	if valueStr != nil {
		tx.Value = new(big.Int)
		tx.Value.SetString(*valueStr, 10)
	}
	if gasPriceStr != nil {
		tx.GasPrice = new(big.Int)
		tx.GasPrice.SetString(*gasPriceStr, 10)
	}

	return &tx, nil
}

// UpdateTokenMetrics updates aggregated metrics for a specific token
func UpdateTokenMetrics(ctx context.Context, pool any, tokenAddress string) error {
	type Pooler interface {
		Exec(ctx context.Context, sql string, arguments ...any) (any, error)
	}
	
	pooler, ok := pool.(Pooler)
	if !ok {
		return fmt.Errorf("invalid pool type")
	}

	query := `
		WITH token_data AS (
			-- Get current price from tokens table
			SELECT address, price_usd, EXTRACT(EPOCH FROM NOW())::bigint AS now_ts
			FROM tokens WHERE address = $1
		),
		-- Calculate 24h volume from swaps (sum of all swap USD values)
		vol_24h AS (
			SELECT COALESCE(SUM(amount_usd), 0) as volume_24h
			FROM (
				SELECT amount_usd
				FROM uniswap_v2_swaps s 
				JOIN uniswap_v2_pairs p ON s.pair = p.address
				WHERE (p.token0 = $1 OR p.token1 = $1)
				  AND s.timestamp >= (SELECT now_ts FROM token_data) - 86400
				  AND s.amount_usd IS NOT NULL
				UNION ALL
				SELECT amount_usd
				FROM uniswap_v3_swaps s
				JOIN uniswap_v3_pools p ON s.pool = p.address  
				WHERE (p.token0 = $1 OR p.token1 = $1)
				  AND s.timestamp >= (SELECT now_ts FROM token_data) - 86400
				  AND s.amount_usd IS NOT NULL
			) combined
		),
		-- Calculate total liquidity from all pairs/pools
		liq AS (
			SELECT COALESCE(SUM(liq_usd), 0) as total_liq
			FROM (
				-- V2 pairs - use reserve_usd directly
				SELECT 
					COALESCE(p.reserve_usd, 0) as liq_usd
				FROM uniswap_v2_pairs p
				WHERE p.token0 = $1 OR p.token1 = $1
				UNION ALL
				-- V3 pools - calculate liquidity_usd from reserves
				SELECT
					((COALESCE(p.reserve0,0) / POWER(10::numeric, COALESCE(t0.decimals, 18))) * COALESCE(t0.price_usd, 0)) +
					((COALESCE(p.reserve1,0) / POWER(10::numeric, COALESCE(t1.decimals, 18))) * COALESCE(t1.price_usd, 0)) as liq_usd
				FROM uniswap_v3_pools p
				LEFT JOIN tokens t0 ON t0.address = p.token0
				LEFT JOIN tokens t1 ON t1.address = p.token1
				WHERE p.token0 = $1 OR p.token1 = $1
			) combined_liq
		),
		-- Get historical price snapshots (24h and 7d ago) by looking back in time
		-- We'll use a simple approach: capture price_usd at different timestamps
		-- For now, we'll skip price changes if we don't have historical data
		-- This can be improved later with a proper price history table
		price_history AS (
			SELECT 
				t.price_usd as current_price,
				NULL::numeric as price_24h_ago,
				NULL::numeric as price_7d_ago
			FROM tokens t
			WHERE t.address = $1
		)
		UPDATE tokens t
		SET 
			volume_24h_usd = (SELECT volume_24h FROM vol_24h),
			total_liquidity_usd = (SELECT total_liq FROM liq),
			price_change_24h = NULL,  -- Will implement with proper price history
			price_change_7d = NULL,   -- Will implement with proper price history
			updated_at = NOW()
		WHERE t.address = $1
	`
	
	_, err := pooler.Exec(ctx, query, tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to update token metrics for %s: %w", tokenAddress, err)
	}
	
	return nil
}