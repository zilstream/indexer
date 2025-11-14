package database

import (
	"context"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
func UpdateTokenMetrics(ctx context.Context, pool *pgxpool.Pool, tokenAddress string) error {
	// Get WZIL address for price calculation
	var wzilAddr string
	err := pool.QueryRow(ctx, "SELECT address FROM tokens WHERE symbol ILIKE 'WZIL' LIMIT 1").Scan(&wzilAddr)
	if err != nil {
		return fmt.Errorf("failed to get WZIL address: %w", err)
	}

	query := `
		WITH token_data AS (
			SELECT address, price_usd, now() AT TIME ZONE 'UTC' AS now_ts
			FROM tokens
			WHERE lower(address) = lower($1)
		),
		vol_24h AS (
			SELECT COALESCE(SUM(amount_usd), 0)::numeric AS volume_24h
			FROM (
				SELECT s.amount_usd
				FROM uniswap_v2_swaps s
				JOIN uniswap_v2_pairs p ON s.pair = p.address
				WHERE (lower(p.token0) = lower($1) OR lower(p.token1) = lower($1))
				  AND s.timestamp >= EXTRACT(EPOCH FROM (SELECT now_ts FROM token_data)) - 86400
				  AND s.amount_usd IS NOT NULL
				UNION ALL
				SELECT s.amount_usd
				FROM uniswap_v3_swaps s
				JOIN uniswap_v3_pools p ON s.pool = p.address
				WHERE (lower(p.token0) = lower($1) OR lower(p.token1) = lower($1))
				  AND s.timestamp >= EXTRACT(EPOCH FROM (SELECT now_ts FROM token_data)) - 86400
				  AND s.amount_usd IS NOT NULL
			) combined
		),
		liq AS (
			SELECT COALESCE(SUM(liq_usd), 0)::numeric AS total_liq
			FROM (
				SELECT COALESCE(p.reserve_usd, 0)::numeric AS liq_usd
				FROM uniswap_v2_pairs p
				WHERE lower(p.token0) = lower($1) OR lower(p.token1) = lower($1)
				UNION ALL
				SELECT
				  ((COALESCE(p.reserve0,0)::numeric / POWER(10::numeric, COALESCE(t0.decimals, 18))) * COALESCE(t0.price_usd, 0)::numeric) +
				  ((COALESCE(p.reserve1,0)::numeric / POWER(10::numeric, COALESCE(t1.decimals, 18))) * COALESCE(t1.price_usd, 0)::numeric) AS liq_usd
				FROM uniswap_v3_pools p
				LEFT JOIN tokens t0 ON t0.address = p.token0
				LEFT JOIN tokens t1 ON t1.address = p.token1
				WHERE lower(p.token0) = lower($1) OR lower(p.token1) = lower($1)
			) combined_liq
		),
		anchors AS (
			SELECT
				ARRAY(SELECT lower(address) FROM tokens WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'zUSDT', 'ZUSD', 'XSGD', 'kUSD'])) AS stables,
				lower($2) AS wzil
		),
		times AS (
			SELECT
				(SELECT now_ts FROM token_data) AS now_ts,
				(SELECT now_ts FROM token_data) - interval '24 hours' AS ts_24h,
				(SELECT now_ts FROM token_data) - interval '7 days' AS ts_7d
		),
		pools AS (
			SELECT
				dp.protocol,
				dp.address AS pool,
				dp.token0, dp.token1,
				COALESCE(t0.decimals, 18) AS dec0, COALESCE(t1.decimals, 18) AS dec1,
				(COALESCE(dp.liquidity_usd,0) + COALESCE(dp.volume_usd_24h,0))::numeric AS w,
				(lower(dp.token0) = lower($1)) AS is_target0,
				CASE
				  WHEN lower(dp.token0) = (SELECT wzil FROM anchors) THEN 'wzil0'
				  WHEN lower(dp.token1) = (SELECT wzil FROM anchors) THEN 'wzil1'
				  WHEN lower(dp.token0) = ANY(SELECT unnest(stables) FROM anchors) THEN 'stable0'
				  WHEN lower(dp.token1) = ANY(SELECT unnest(stables) FROM anchors) THEN 'stable1'
				  ELSE 'none'
				END AS anchor
			FROM dex_pools dp
			JOIN tokens t0 ON lower(t0.address) = lower(dp.token0)
			JOIN tokens t1 ON lower(t1.address) = lower(dp.token1)
			WHERE lower(dp.token0) = lower($1) OR lower(dp.token1) = lower($1)
			  AND (COALESCE(dp.liquidity_usd,0) + COALESCE(dp.volume_usd_24h,0)) >= 100
		),
		zil_24h AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_24h FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		zil_7d AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_7d FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		v2_24h AS (
			SELECT
				p.pool, p.w,
				CASE
				  WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL OR s.reserve0 = 0 OR s.reserve1 = 0 THEN NULL
				  ELSE
					CASE WHEN p.is_target0 THEN
					  CASE p.anchor
						WHEN 'wzil1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)) * (SELECT usd FROM zil_24h)
						WHEN 'wzil0' THEN (SELECT usd FROM zil_24h)
						WHEN 'stable1' THEN (s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)
						WHEN 'stable0' THEN 1::numeric
						ELSE NULL
					  END
					ELSE
					  CASE p.anchor
						WHEN 'wzil0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)) * (SELECT usd FROM zil_24h)
						WHEN 'wzil1' THEN (SELECT usd FROM zil_24h)
						WHEN 'stable0' THEN NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)
						WHEN 'stable1' THEN 1::numeric
						ELSE NULL
					  END
					END
				END AS price_usd
			FROM pools p
			LEFT JOIN LATERAL (
				SELECT reserve0, reserve1
				FROM uniswap_v2_syncs s
				WHERE s.pair = p.pool
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_24h FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		v3_24h AS (
			SELECT
				p.pool, p.w,
				CASE
				  WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 OR s.sqrt_price_x96 < 1000000 THEN NULL
				  ELSE
					CASE WHEN p.is_target0 THEN
					  CASE p.anchor
						WHEN 'wzil1' THEN ((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)) * (SELECT usd FROM zil_24h)
						WHEN 'wzil0' THEN (SELECT usd FROM zil_24h)
						WHEN 'stable1' THEN (POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
						WHEN 'stable0' THEN 1::numeric
						ELSE NULL
					  END
					ELSE
					  CASE p.anchor
						WHEN 'wzil0' THEN (1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)) * (SELECT usd FROM zil_24h)
						WHEN 'wzil1' THEN (SELECT usd FROM zil_24h)
						WHEN 'stable0' THEN 1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)
						WHEN 'stable1' THEN 1::numeric
						ELSE NULL
					  END
					END
				END AS price_usd
			FROM pools p
			LEFT JOIN LATERAL (
				SELECT sqrt_price_x96
				FROM uniswap_v3_swaps s
				WHERE s.pool = p.pool
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_24h FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		agg_24h AS (
			SELECT
				NULLIF(SUM(CASE WHEN price_usd IS NOT NULL THEN w ELSE 0 END), 0) AS total_w,
				SUM(price_usd * w) AS w_price_sum
			FROM (
				SELECT pool, w, price_usd FROM v2_24h
				UNION ALL
				SELECT pool, w, price_usd FROM v3_24h
			) x
		),
		v2_7d AS (
			SELECT
				p.pool, p.w,
				CASE
				  WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL OR s.reserve0 = 0 OR s.reserve1 = 0 THEN NULL
				  ELSE
					CASE WHEN p.is_target0 THEN
					  CASE p.anchor
						WHEN 'wzil1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)) * (SELECT usd FROM zil_7d)
						WHEN 'wzil0' THEN (SELECT usd FROM zil_7d)
						WHEN 'stable1' THEN (s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)
						WHEN 'stable0' THEN 1::numeric
						ELSE NULL
					  END
					ELSE
					  CASE p.anchor
						WHEN 'wzil0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)) * (SELECT usd FROM zil_7d)
						WHEN 'wzil1' THEN (SELECT usd FROM zil_7d)
						WHEN 'stable0' THEN NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)
						WHEN 'stable1' THEN 1::numeric
						ELSE NULL
					  END
					END
				END AS price_usd
			FROM pools p
			LEFT JOIN LATERAL (
				SELECT reserve0, reserve1
				FROM uniswap_v2_syncs s
				WHERE s.pair = p.pool
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_7d FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		v3_7d AS (
			SELECT
				p.pool, p.w,
				CASE
				  WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 OR s.sqrt_price_x96 < 1000000 THEN NULL
				  ELSE
					CASE WHEN p.is_target0 THEN
					  CASE p.anchor
						WHEN 'wzil1' THEN ((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)) * (SELECT usd FROM zil_7d)
						WHEN 'wzil0' THEN (SELECT usd FROM zil_7d)
						WHEN 'stable1' THEN (POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
						WHEN 'stable0' THEN 1::numeric
						ELSE NULL
					  END
					ELSE
					  CASE p.anchor
						WHEN 'wzil0' THEN (1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)) * (SELECT usd FROM zil_7d)
						WHEN 'wzil1' THEN (SELECT usd FROM zil_7d)
						WHEN 'stable0' THEN 1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)
						WHEN 'stable1' THEN 1::numeric
						ELSE NULL
					  END
					END
				END AS price_usd
			FROM pools p
			LEFT JOIN LATERAL (
				SELECT sqrt_price_x96
				FROM uniswap_v3_swaps s
				WHERE s.pool = p.pool
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_7d FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		agg_7d AS (
			SELECT
				NULLIF(SUM(CASE WHEN price_usd IS NOT NULL THEN w ELSE 0 END), 0) AS total_w,
				SUM(price_usd * w) AS w_price_sum
			FROM (
				SELECT pool, w, price_usd FROM v2_7d
				UNION ALL
				SELECT pool, w, price_usd FROM v3_7d
			) x
		),
		hist_prices AS (
			SELECT
				CASE 
					WHEN (SELECT total_w FROM agg_24h) > 0 AND (SELECT w_price_sum FROM agg_24h) IS NOT NULL 
					THEN NULLIF((SELECT w_price_sum FROM agg_24h) / (SELECT total_w FROM agg_24h), 0)
					ELSE NULL 
				END AS price_24h,
				CASE 
					WHEN (SELECT total_w FROM agg_7d) > 0 AND (SELECT w_price_sum FROM agg_7d) IS NOT NULL 
					THEN NULLIF((SELECT w_price_sum FROM agg_7d) / (SELECT total_w FROM agg_7d), 0)
					ELSE NULL 
				END AS price_7d
		)
		UPDATE tokens t
		SET
		  volume_24h_usd = (SELECT volume_24h FROM vol_24h),
		  total_liquidity_usd = (SELECT total_liq FROM liq),
		  price_change_24h = CASE
			WHEN (SELECT price_24h FROM hist_prices) IS NULL THEN NULL
			WHEN (SELECT price_usd FROM token_data) IS NULL OR (SELECT price_usd FROM token_data) = 0 THEN NULL
			ELSE (( (SELECT price_usd FROM token_data) - (SELECT price_24h FROM hist_prices) )
				 / (SELECT price_24h FROM hist_prices)) * 100
		  END,
		  price_change_7d = CASE
			WHEN (SELECT price_7d FROM hist_prices) IS NULL THEN NULL
			WHEN (SELECT price_usd FROM token_data) IS NULL OR (SELECT price_usd FROM token_data) = 0 THEN NULL
			ELSE (( (SELECT price_usd FROM token_data) - (SELECT price_7d FROM hist_prices) )
				 / (SELECT price_7d FROM hist_prices)) * 100
		  END,
		  updated_at = NOW()
		WHERE lower(t.address) = lower($1)
	`
	
	_, err = pool.Exec(ctx, query, tokenAddress, wzilAddr)
	if err != nil {
		return fmt.Errorf("failed to update token metrics for %s: %w", tokenAddress, err)
	}
	
	return nil
}

// UpdatePairMetrics calculates and updates price change percentages for a pair/pool
func UpdatePairMetrics(ctx context.Context, pool *pgxpool.Pool, pairAddress, protocol string) error {
	// Get WZIL address for price calculation
	var wzilAddr string
	err := pool.QueryRow(ctx, "SELECT address FROM tokens WHERE symbol ILIKE 'WZIL' LIMIT 1").Scan(&wzilAddr)
	if err != nil {
		return fmt.Errorf("failed to get WZIL address: %w", err)
	}

	if protocol == "uniswap_v2" {
		return updateV2PairMetrics(ctx, pool, pairAddress, wzilAddr)
	}
	return updateV3PoolMetrics(ctx, pool, pairAddress, wzilAddr)
}

func updateV2PairMetrics(ctx context.Context, pool *pgxpool.Pool, pairAddr, wzilAddr string) error {
	query := `
		WITH pair_info AS (
			SELECT p.address, p.token0, p.token1,
				COALESCE(t0.decimals, 18) AS dec0,
				COALESCE(t1.decimals, 18) AS dec1,
				lower($2) AS wzil,
				ARRAY(SELECT lower(address) FROM tokens WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'ZUSD', 'XSGD'])) AS stables
			FROM uniswap_v2_pairs p
			LEFT JOIN tokens t0 ON t0.address = p.token0
			LEFT JOIN tokens t1 ON t1.address = p.token1
			WHERE lower(p.address) = lower($1)
		),
		times AS (
			SELECT
				now() AT TIME ZONE 'UTC' AS now_ts,
				now() AT TIME ZONE 'UTC' - interval '24 hours' AS ts_24h,
				now() AT TIME ZONE 'UTC' - interval '7 days' AS ts_7d
		),
		zil_24h AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_24h FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		zil_7d AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_7d FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		current_price AS (
			SELECT
				CASE
				  WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL OR s.reserve0 = 0 OR s.reserve1 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0))
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pair_info p
			LEFT JOIN LATERAL (
				SELECT reserve0, reserve1
				FROM uniswap_v2_syncs s
				WHERE s.pair = p.address
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		price_24h AS (
			SELECT
				CASE
				  WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL OR s.reserve0 = 0 OR s.reserve1 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0))
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pair_info p
			LEFT JOIN LATERAL (
				SELECT reserve0, reserve1
				FROM uniswap_v2_syncs s
				WHERE s.pair = p.address
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_24h FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		price_7d AS (
			SELECT
				CASE
				  WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL OR s.reserve0 = 0 OR s.reserve1 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0))
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pair_info p
			LEFT JOIN LATERAL (
				SELECT reserve0, reserve1
				FROM uniswap_v2_syncs s
				WHERE s.pair = p.address
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_7d FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		)
		UPDATE uniswap_v2_pairs
		SET
		  price_change_24h = CASE
			WHEN (SELECT price FROM price_24h) IS NULL OR (SELECT price FROM price_24h) = 0 THEN NULL
			WHEN (SELECT price FROM current_price) IS NULL OR (SELECT price FROM current_price) = 0 THEN NULL
			ELSE (((SELECT price FROM current_price) - (SELECT price FROM price_24h)) / (SELECT price FROM price_24h)) * 100
		  END,
		  price_change_7d = CASE
			WHEN (SELECT price FROM price_7d) IS NULL OR (SELECT price FROM price_7d) = 0 THEN NULL
			WHEN (SELECT price FROM current_price) IS NULL OR (SELECT price FROM current_price) = 0 THEN NULL
			ELSE (((SELECT price FROM current_price) - (SELECT price FROM price_7d)) / (SELECT price FROM price_7d)) * 100
		  END,
		  updated_at = NOW()
		WHERE lower(address) = lower($1)
	`

	_, err := pool.Exec(ctx, query, pairAddr, wzilAddr)
	if err != nil {
		return fmt.Errorf("failed to update V2 pair metrics for %s: %w", pairAddr, err)
	}
	return nil
}

func updateV3PoolMetrics(ctx context.Context, pool *pgxpool.Pool, poolAddr, wzilAddr string) error {
	query := `
		WITH pool_info AS (
			SELECT p.address, p.token0, p.token1,
				COALESCE(t0.decimals, 18) AS dec0,
				COALESCE(t1.decimals, 18) AS dec1,
				lower($2) AS wzil,
				ARRAY(SELECT lower(address) FROM tokens WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'ZUSD', 'XSGD'])) AS stables
			FROM uniswap_v3_pools p
			LEFT JOIN tokens t0 ON t0.address = p.token0
			LEFT JOIN tokens t1 ON t1.address = p.token1
			WHERE lower(p.address) = lower($1)
		),
		times AS (
			SELECT
				now() AT TIME ZONE 'UTC' AS now_ts,
				now() AT TIME ZONE 'UTC' - interval '24 hours' AS ts_24h,
				now() AT TIME ZONE 'UTC' - interval '7 days' AS ts_7d
		),
		zil_24h AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_24h FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		zil_7d AS (
			SELECT (SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= (SELECT ts_7d FROM times) ORDER BY ts DESC LIMIT 1) AS usd
		),
		current_price AS (
			SELECT
				CASE
				  WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pool_info p
			LEFT JOIN LATERAL (
				SELECT sqrt_price_x96
				FROM uniswap_v3_swaps s
				WHERE s.pool = p.address
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		price_24h AS (
			SELECT
				CASE
				  WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pool_info p
			LEFT JOIN LATERAL (
				SELECT sqrt_price_x96
				FROM uniswap_v3_swaps s
				WHERE s.pool = p.address
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_24h FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		),
		price_7d AS (
			SELECT
				CASE
				  WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 THEN NULL
				  WHEN lower(p.token1) = p.wzil THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = p.wzil THEN
					1::numeric
				  WHEN lower(p.token1) = ANY(p.stables) THEN
					(POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
				  WHEN lower(p.token0) = ANY(p.stables) THEN
					1::numeric
				  ELSE NULL
				END AS price
			FROM pool_info p
			LEFT JOIN LATERAL (
				SELECT sqrt_price_x96
				FROM uniswap_v3_swaps s
				WHERE s.pool = p.address
				  AND s.timestamp <= EXTRACT(EPOCH FROM (SELECT ts_7d FROM times))
				ORDER BY s.timestamp DESC
				LIMIT 1
			) s ON TRUE
		)
		UPDATE uniswap_v3_pools
		SET
		  price_change_24h = CASE
			WHEN (SELECT price FROM price_24h) IS NULL OR (SELECT price FROM price_24h) = 0 THEN NULL
			WHEN (SELECT price FROM current_price) IS NULL OR (SELECT price FROM current_price) = 0 THEN NULL
			ELSE (((SELECT price FROM current_price) - (SELECT price FROM price_24h)) / (SELECT price FROM price_24h)) * 100
		  END,
		  price_change_7d = CASE
			WHEN (SELECT price FROM price_7d) IS NULL OR (SELECT price FROM price_7d) = 0 THEN NULL
			WHEN (SELECT price FROM current_price) IS NULL OR (SELECT price FROM current_price) = 0 THEN NULL
			ELSE (((SELECT price FROM current_price) - (SELECT price FROM price_7d)) / (SELECT price FROM price_7d)) * 100
		  END,
		  updated_at = NOW()
		WHERE lower(address) = lower($1)
	`

	_, err := pool.Exec(ctx, query, poolAddr, wzilAddr)
	if err != nil {
		return fmt.Errorf("failed to update V3 pool metrics for %s: %w", poolAddr, err)
	}
	return nil
}