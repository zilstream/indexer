package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DTOs for API responses (lightweight, no ORM tags)
type TokenDTO struct {
	Address          string   `json:"address"`
	Symbol           *string  `json:"symbol,omitempty"`
	Name             *string  `json:"name,omitempty"`
	Decimals         *int32   `json:"decimals,omitempty"`
	PriceUSD         *string  `json:"price_usd,omitempty"`
	MarketCapUSD     *string  `json:"market_cap_usd,omitempty"`
	LiquidityUSD     *string  `json:"liquidity_usd"`
	Volume24hUSD     *string  `json:"volume_24h_usd"`
	PriceChange24h   *string  `json:"price_change_24h"`
	PriceChange7d    *string  `json:"price_change_7d"`
}

type PairDTO struct {
	Protocol      string  `json:"protocol"`
	Address       string  `json:"address"`
	Token0        string  `json:"token0"`
	Token1        string  `json:"token1"`
	Token0Symbol  *string `json:"token0_symbol,omitempty"`
	Token0Name    *string `json:"token0_name,omitempty"`
	Token1Symbol  *string `json:"token1_symbol,omitempty"`
	Token1Name    *string `json:"token1_name,omitempty"`
	Fee           *string `json:"fee,omitempty"`
	Reserve0      *string `json:"reserve0,omitempty"`
	Reserve1      *string `json:"reserve1,omitempty"`
	Liquidity     *string `json:"liquidity,omitempty"`
	LiquidityUSD  *string `json:"liquidity_usd,omitempty"`
	VolumeUSD     *string `json:"volume_usd,omitempty"`
	VolumeUSD24h  *string `json:"volume_usd_24h,omitempty"`
	TxnCount      *int64  `json:"txn_count,omitempty"`
}

type PairEventDTO struct {
	Protocol     string  `json:"protocol"`
	EventType    string  `json:"event_type"`
	ID           string  `json:"id"`
	TransactionHash string `json:"transaction_hash"`
	LogIndex     int32   `json:"log_index"`
	BlockNumber  int64   `json:"block_number"`
	Timestamp    int64   `json:"timestamp"`
	Address      string  `json:"address"`
	Sender       *string `json:"sender,omitempty"`
	Recipient    *string `json:"recipient,omitempty"`
	ToAddress    *string `json:"to_address,omitempty"`
	Amount0In    *string `json:"amount0_in,omitempty"`
	Amount1In    *string `json:"amount1_in,omitempty"`
	Amount0Out   *string `json:"amount0_out,omitempty"`
	Amount1Out   *string `json:"amount1_out,omitempty"`
	Liquidity    *string `json:"liquidity,omitempty"`
	AmountUSD    *string `json:"amount_usd,omitempty"`
}

type StatsDTO struct {
	TotalPairs      int64  `json:"total_pairs"`
	TotalTokens     int64  `json:"total_tokens"`
	TotalLiquidity  string `json:"total_liquidity_usd"`
	TotalVolume24h  string `json:"total_volume_usd_24h"`
	TotalVolumeAll  string `json:"total_volume_usd_all"`
}

type BlockDTO struct {
	Number           int64   `json:"number"`
	Hash             string  `json:"hash"`
	ParentHash       string  `json:"parent_hash"`
	Timestamp        int64   `json:"timestamp"`
	GasLimit         *int64  `json:"gas_limit,omitempty"`
	GasUsed          *int64  `json:"gas_used,omitempty"`
	BaseFeePerGas    *string `json:"base_fee_per_gas,omitempty"`
	TransactionCount int     `json:"transaction_count"`
}

type TransactionDTO struct {
	Hash                  string  `json:"hash"`
	BlockNumber           int64   `json:"block_number"`
	TransactionIndex      int     `json:"transaction_index"`
	FromAddress           string  `json:"from_address"`
	ToAddress             *string `json:"to_address,omitempty"`
	Value                 string  `json:"value"`
	GasPrice              *string `json:"gas_price,omitempty"`
	GasLimit              *int64  `json:"gas_limit,omitempty"`
	GasUsed               *int64  `json:"gas_used,omitempty"`
	Nonce                 *int64  `json:"nonce,omitempty"`
	Status                *int    `json:"status,omitempty"`
	TransactionType       int     `json:"transaction_type"`
	OriginalTypeHex       *string `json:"original_type_hex,omitempty"`
	MaxFeePerGas          *string `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas  *string `json:"max_priority_fee_per_gas,omitempty"`
	EffectiveGasPrice     *string `json:"effective_gas_price,omitempty"`
	ContractAddress       *string `json:"contract_address,omitempty"`
	CumulativeGasUsed     *int64  `json:"cumulative_gas_used,omitempty"`
}

type PricePoint struct {
	Timestamp string  `json:"timestamp"`
	Price     *string `json:"price"`
	Source    *string `json:"source,omitempty"`
}

type PriceChartDTO struct {
	Address    string       `json:"address"`
	Protocol   string       `json:"protocol"`
	BaseToken  TokenBaseDTO `json:"base_token"`
	Quote      string       `json:"quote"`
	Timeframe  string       `json:"timeframe"`
	Interval   string       `json:"interval"`
	Points     []PricePoint `json:"points"`
}

type TokenBaseDTO struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals int32  `json:"decimals"`
}

// ListTokens queries tokens with optional fuzzy search (symbol/name) ordered by market_cap_usd desc
func GetToken(ctx context.Context, pool *pgxpool.Pool, address string) (*TokenDTO, error) {
	q := `
		SELECT address, symbol, name, decimals,
		       CAST(price_usd AS TEXT), 
		       CAST(market_cap_usd AS TEXT), 
		       CAST(total_liquidity_usd AS TEXT),
		       CAST(volume_24h_usd AS TEXT),
		       CAST(price_change_24h AS TEXT),
		       CAST(price_change_7d AS TEXT)
		FROM tokens
		WHERE address = $1`

	var t TokenDTO
	err := pool.QueryRow(ctx, q, address).Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, 
		&t.PriceUSD, &t.MarketCapUSD, &t.LiquidityUSD, &t.Volume24hUSD,
		&t.PriceChange24h, &t.PriceChange7d)
	if err != nil {
		return nil, fmt.Errorf("GetToken query failed: %w", err)
	}
	return &t, nil
}

func ListTokens(ctx context.Context, pool *pgxpool.Pool, limit, offset int, search *string) ([]TokenDTO, error) {
	q := `
		SELECT address, symbol, name, decimals,
		       CAST(price_usd AS TEXT), 
		       CAST(market_cap_usd AS TEXT), 
		       CAST(total_liquidity_usd AS TEXT),
		       CAST(volume_24h_usd AS TEXT),
		       CAST(price_change_24h AS TEXT),
		       CAST(price_change_7d AS TEXT)
		FROM tokens
		WHERE ($3::text IS NULL OR symbol ILIKE '%' || $3 || '%' OR name ILIKE '%' || $3 || '%')
		ORDER BY CAST(market_cap_usd AS NUMERIC) DESC NULLS LAST
		LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset, search)
	if err != nil {
		return nil, fmt.Errorf("ListTokens query failed: %w", err)
	}
	defer rows.Close()

	var out []TokenDTO
	for rows.Next() {
		var t TokenDTO
		if err := rows.Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, 
			&t.PriceUSD, &t.MarketCapUSD, &t.LiquidityUSD, &t.Volume24hUSD,
			&t.PriceChange24h, &t.PriceChange7d); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// ListPairsByToken returns all pairs containing a specific token address ordered by liquidity
func ListPairsByToken(ctx context.Context, pool *pgxpool.Pool, tokenAddress string, limit, offset int) ([]PairDTO, error) {
	q := `
		SELECT protocol, address, token0, token1,
		       token0_symbol, token0_name, token1_symbol, token1_name,
		       CAST(fee AS TEXT), CAST(reserve0 AS TEXT), CAST(reserve1 AS TEXT), CAST(liquidity AS TEXT),
		       CAST(liquidity_usd AS TEXT), CAST(volume_usd AS TEXT), CAST(volume_usd_24h AS TEXT), txn_count
		FROM dex_pools
		WHERE (token0 = $1 OR token1 = $1)
		  AND liquidity_usd > 0
		ORDER BY CAST(liquidity_usd AS NUMERIC) DESC NULLS LAST
		LIMIT $2 OFFSET $3`

	rows, err := pool.Query(ctx, q, tokenAddress, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListPairsByToken query failed: %w", err)
	}
	defer rows.Close()

	var out []PairDTO
	for rows.Next() {
		var p PairDTO
		if err := rows.Scan(&p.Protocol, &p.Address, &p.Token0, &p.Token1, 
			&p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name, 
			&p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity, 
			&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h, &p.TxnCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// ListPairs reads from dex_pools view to be protocol-agnostic (V2+V3)
func GetPair(ctx context.Context, pool *pgxpool.Pool, address string) (*PairDTO, error) {
	q := `
		SELECT protocol, address, token0, token1,
		       token0_symbol, token0_name, token1_symbol, token1_name,
		       CAST(fee AS TEXT), CAST(reserve0 AS TEXT), CAST(reserve1 AS TEXT), CAST(liquidity AS TEXT),
		       CAST(liquidity_usd AS TEXT), CAST(volume_usd AS TEXT), CAST(volume_usd_24h AS TEXT), txn_count
		FROM dex_pools
		WHERE address = $1`

	var p PairDTO
	err := pool.QueryRow(ctx, q, address).Scan(
		&p.Protocol, &p.Address, &p.Token0, &p.Token1,
		&p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name,
		&p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity,
		&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h, &p.TxnCount)
	if err != nil {
		return nil, fmt.Errorf("GetPair query failed: %w", err)
	}
	return &p, nil
}

func ListPairs(ctx context.Context, pool *pgxpool.Pool, limit, offset int, sortBy, sortOrder string) ([]PairDTO, error) {
	// Map user-friendly sort options to column names
	sortColumn := "volume_usd_24h"
	switch sortBy {
	case "volume_24h":
		sortColumn = "volume_usd_24h"
	case "liquidity":
		sortColumn = "liquidity_usd"
	case "created", "newest", "oldest":
		sortColumn = "created_at_timestamp"
	case "volume":
		sortColumn = "volume_usd"
	}

	// Validate sort order
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}

	q := fmt.Sprintf(`
		SELECT protocol, address, token0, token1,
		       token0_symbol, token0_name, token1_symbol, token1_name,
		       CAST(fee AS TEXT), CAST(reserve0 AS TEXT), CAST(reserve1 AS TEXT), CAST(liquidity AS TEXT),
		       CAST(liquidity_usd AS TEXT), CAST(volume_usd AS TEXT), CAST(volume_usd_24h AS TEXT), txn_count
		FROM dex_pools
		WHERE liquidity_usd > 0
		ORDER BY CAST(%s AS NUMERIC) %s NULLS LAST, CAST(liquidity_usd AS NUMERIC) DESC NULLS LAST
		LIMIT $1 OFFSET $2`, sortColumn, strings.ToUpper(sortOrder))

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListPairs query failed: %w", err)
	}
	defer rows.Close()

	var out []PairDTO
	for rows.Next() {
		var p PairDTO
		if err := rows.Scan(&p.Protocol, &p.Address, &p.Token0, &p.Token1, &p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name, &p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity, &p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h, &p.TxnCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// ListPairEvents returns unified events for a pair/pool address, with optional filters
func ListPairEvents(ctx context.Context, pool *pgxpool.Pool, address string, eventType *string, protocol *string, limit, offset int) ([]PairEventDTO, error) {
	q := `
		SELECT protocol, event_type, id, transaction_hash, log_index, block_number, timestamp,
		       address, sender, recipient, to_address,
		       CAST(amount0_in AS TEXT), CAST(amount1_in AS TEXT), CAST(amount0_out AS TEXT), CAST(amount1_out AS TEXT),
		       CAST(liquidity AS TEXT), CAST(amount_usd AS TEXT)
		FROM dex_pair_events
		WHERE address = $1
		  AND ($2::text IS NULL OR event_type = $2)
		  AND ($3::text IS NULL OR protocol = $3)
		ORDER BY timestamp DESC, log_index DESC
		LIMIT $4 OFFSET $5`

	rows, err := pool.Query(ctx, q, address, eventType, protocol, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListPairEvents query failed: %w", err)
	}
	defer rows.Close()

	var out []PairEventDTO
	for rows.Next() {
		var e PairEventDTO
		if err := rows.Scan(&e.Protocol, &e.EventType, &e.ID, &e.TransactionHash, &e.LogIndex, &e.BlockNumber, &e.Timestamp,
			&e.Address, &e.Sender, &e.Recipient, &e.ToAddress, &e.Amount0In, &e.Amount1In, &e.Amount0Out, &e.Amount1Out, &e.Liquidity, &e.AmountUSD); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// GetStats returns aggregate statistics across all pairs and tokens
func GetStats(ctx context.Context, pool *pgxpool.Pool) (*StatsDTO, error) {
	q := `
		SELECT 
			COUNT(DISTINCT address) AS total_pairs,
			COALESCE(to_char(SUM(liquidity_usd), 'FM999999999999999999999990.99'), '0') AS total_liquidity,
			COALESCE(to_char(SUM(volume_usd_24h), 'FM999999999999999999999990.99'), '0') AS total_volume_24h,
			COALESCE(to_char(SUM(volume_usd), 'FM999999999999999999999990.99'), '0') AS total_volume_all
		FROM dex_pools
		WHERE liquidity_usd > 0 AND liquidity_usd < 1e15
	`
	
	var stats StatsDTO
	err := pool.QueryRow(ctx, q).Scan(&stats.TotalPairs, &stats.TotalLiquidity, &stats.TotalVolume24h, &stats.TotalVolumeAll)
	if err != nil {
		return nil, fmt.Errorf("GetStats pairs query failed: %w", err)
	}

	// Get total tokens count
	tokenQ := `SELECT COUNT(*) FROM tokens`
	err = pool.QueryRow(ctx, tokenQ).Scan(&stats.TotalTokens)
	if err != nil {
		return nil, fmt.Errorf("GetStats tokens query failed: %w", err)
	}

	return &stats, nil
}

// ListBlocks returns a paginated list of blocks ordered by block number descending
func ListBlocks(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]BlockDTO, error) {
	q := `
		SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
		       CAST(base_fee_per_gas AS TEXT), transaction_count
		FROM blocks
		ORDER BY number DESC
		LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListBlocks query failed: %w", err)
	}
	defer rows.Close()

	var out []BlockDTO
	for rows.Next() {
		var b BlockDTO
		if err := rows.Scan(&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasLimit, &b.GasUsed, &b.BaseFeePerGas, &b.TransactionCount); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// GetBlock returns a single block by number
func GetBlock(ctx context.Context, pool *pgxpool.Pool, number int64) (*BlockDTO, error) {
	q := `
		SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used,
		       CAST(base_fee_per_gas AS TEXT), transaction_count
		FROM blocks
		WHERE number = $1`

	var b BlockDTO
	err := pool.QueryRow(ctx, q, number).Scan(&b.Number, &b.Hash, &b.ParentHash, &b.Timestamp, &b.GasLimit, &b.GasUsed, &b.BaseFeePerGas, &b.TransactionCount)
	if err != nil {
		return nil, fmt.Errorf("GetBlock query failed: %w", err)
	}
	return &b, nil
}

// ListTransactions returns a paginated list of transactions ordered by block number and index descending
func ListTransactions(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]TransactionDTO, error) {
	q := `
		SELECT hash, block_number, transaction_index, from_address, to_address,
		       CAST(value AS TEXT), CAST(gas_price AS TEXT), gas_limit, gas_used, nonce, status,
		       transaction_type, original_type_hex,
		       CAST(max_fee_per_gas AS TEXT), CAST(max_priority_fee_per_gas AS TEXT),
		       CAST(effective_gas_price AS TEXT), contract_address, cumulative_gas_used
		FROM transactions
		ORDER BY block_number DESC, transaction_index DESC
		LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListTransactions query failed: %w", err)
	}
	defer rows.Close()

	var out []TransactionDTO
	for rows.Next() {
		var tx TransactionDTO
		if err := rows.Scan(&tx.Hash, &tx.BlockNumber, &tx.TransactionIndex, &tx.FromAddress, &tx.ToAddress,
			&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.GasUsed, &tx.Nonce, &tx.Status,
			&tx.TransactionType, &tx.OriginalTypeHex,
			&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, nil
}

// ListTransactionsByAddress returns a paginated list of transactions for a specific address
func ListTransactionsByAddress(ctx context.Context, pool *pgxpool.Pool, address string, limit, offset int) ([]TransactionDTO, error) {
	q := `
		SELECT hash, block_number, transaction_index, from_address, to_address,
		       CAST(value AS TEXT), CAST(gas_price AS TEXT), gas_limit, gas_used, nonce, status,
		       transaction_type, original_type_hex,
		       CAST(max_fee_per_gas AS TEXT), CAST(max_priority_fee_per_gas AS TEXT),
		       CAST(effective_gas_price AS TEXT), contract_address, cumulative_gas_used
		FROM transactions
		WHERE from_address = $1 OR to_address = $1
		ORDER BY block_number DESC, transaction_index DESC
		LIMIT $2 OFFSET $3`

	rows, err := pool.Query(ctx, q, address, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListTransactionsByAddress query failed: %w", err)
	}
	defer rows.Close()

	var out []TransactionDTO
	for rows.Next() {
		var tx TransactionDTO
		if err := rows.Scan(&tx.Hash, &tx.BlockNumber, &tx.TransactionIndex, &tx.FromAddress, &tx.ToAddress,
			&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.GasUsed, &tx.Nonce, &tx.Status,
			&tx.TransactionType, &tx.OriginalTypeHex,
			&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, nil
}

// GetTransaction returns a single transaction by hash
func GetTransaction(ctx context.Context, pool *pgxpool.Pool, hash string) (*TransactionDTO, error) {
	q := `
		SELECT hash, block_number, transaction_index, from_address, to_address,
		       CAST(value AS TEXT), CAST(gas_price AS TEXT), gas_limit, gas_used, nonce, status,
		       transaction_type, original_type_hex,
		       CAST(max_fee_per_gas AS TEXT), CAST(max_priority_fee_per_gas AS TEXT),
		       CAST(effective_gas_price AS TEXT), contract_address, cumulative_gas_used
		FROM transactions
		WHERE hash = $1`

	var tx TransactionDTO
	err := pool.QueryRow(ctx, q, hash).Scan(&tx.Hash, &tx.BlockNumber, &tx.TransactionIndex, &tx.FromAddress, &tx.ToAddress,
		&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.GasUsed, &tx.Nonce, &tx.Status,
		&tx.TransactionType, &tx.OriginalTypeHex,
		&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed)
	if err != nil {
		return nil, fmt.Errorf("GetTransaction query failed: %w", err)
	}
	return &tx, nil
}

// GetPairPriceChart returns 42 price points (every 4h) over 7 days for a pair/pool
func GetPairPriceChart(ctx context.Context, pool *pgxpool.Pool, address string) (*PriceChartDTO, error) {
	// First, get pair info to determine protocol
	pair, err := GetPair(ctx, pool, address)
	if err != nil {
		return nil, fmt.Errorf("pair not found: %w", err)
	}

	// Get WZIL address
	wzilAddr, err := getWZILAddress(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get WZIL address: %w", err)
	}

	// Get stablecoin addresses
	stableAddrs, err := getStablecoinAddresses(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get stablecoin addresses: %w", err)
	}

	var points []PricePoint
	var token0Symbol string
	var token0Decimals int32

	if pair.Protocol == "uniswap_v2" {
		points, token0Symbol, token0Decimals, err = getPriceChartV2(ctx, pool, address, pair.Token0, pair.Token1, wzilAddr, stableAddrs)
	} else {
		points, token0Symbol, token0Decimals, err = getPriceChartV3(ctx, pool, address, pair.Token0, pair.Token1, wzilAddr, stableAddrs)
	}
	if err != nil {
		return nil, err
	}

	return &PriceChartDTO{
		Address:   address,
		Protocol:  pair.Protocol,
		BaseToken: TokenBaseDTO{
			Address:  pair.Token0,
			Symbol:   token0Symbol,
			Decimals: token0Decimals,
		},
		Quote:     "USD",
		Timeframe: "7d",
		Interval:  "4h",
		Points:    points,
	}, nil
}

func getWZILAddress(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var addr string
	err := pool.QueryRow(ctx, "SELECT address FROM tokens WHERE symbol ILIKE 'WZIL' LIMIT 1").Scan(&addr)
	if err != nil {
		return "", err
	}
	return addr, nil
}

func getStablecoinAddresses(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	rows, err := pool.Query(ctx, "SELECT address FROM tokens WHERE price_usd = 1.0 AND symbol IN ('USDT', 'USDC', 'DAI', 'BUSD')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addrs []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func getPriceChartV2(ctx context.Context, pool *pgxpool.Pool, pairAddr, token0, token1, wzil string, stables []string) ([]PricePoint, string, int32, error) {
	q := `
	WITH meta AS (
		SELECT 
			t0.address AS token0, t1.address AS token1,
			t0.symbol AS symbol0, t0.decimals AS dec0,
			t1.decimals AS dec1
		FROM tokens t0, tokens t1
		WHERE t0.address = $1 AND t1.address = $2
	),
	aligned AS (
		SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') 
		     - make_interval(hours := (EXTRACT(hour FROM now() AT TIME ZONE 'UTC')::int % 4)) AS end_ts
	),
	buckets AS (
		SELECT generate_series(
			(SELECT end_ts FROM aligned) - interval '7 days' + interval '4 hours',
			(SELECT end_ts FROM aligned),
			interval '4 hours'
		) AS ts
	),
	snaps AS (
		SELECT 
			b.ts,
			s.reserve0, 
			s.reserve1
		FROM buckets b
		LEFT JOIN LATERAL (
			SELECT reserve0, reserve1
			FROM uniswap_v2_syncs
			WHERE pair = $3
			  AND timestamp <= EXTRACT(EPOCH FROM b.ts)
			ORDER BY timestamp DESC
			LIMIT 1
		) s ON TRUE
	),
	zil_prices AS (
		SELECT 
			b.ts,
			(SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= b.ts ORDER BY ts DESC LIMIT 1) AS zil_usd
		FROM buckets b
	)
	SELECT 
		to_char(b.ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
		CASE
			WHEN LOWER($5) = LOWER(m.token1) THEN
				((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0)) * z.zil_usd
			WHEN LOWER($5) = LOWER(m.token0) THEN
				z.zil_usd
			WHEN LOWER(m.token1) = ANY($4::text[]) THEN
				((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0))
			WHEN LOWER(m.token0) = ANY($4::text[]) THEN
				1
			ELSE NULL
		END::text AS price,
		CASE
			WHEN LOWER($5) = LOWER(m.token1) OR LOWER($5) = LOWER(m.token0) THEN 'wzil'
			WHEN LOWER(m.token1) = ANY($4::text[]) OR LOWER(m.token0) = ANY($4::text[]) THEN 'stable'
			ELSE 'none'
		END AS source,
		m.symbol0,
		m.dec0
	FROM buckets b
	CROSS JOIN meta m
	LEFT JOIN snaps s ON s.ts = b.ts
	LEFT JOIN zil_prices z ON z.ts = b.ts
	ORDER BY b.ts ASC`

	rows, err := pool.Query(ctx, q, token0, token1, pairAddr, stables, wzil)
	if err != nil {
		return nil, "", 0, fmt.Errorf("V2 price chart query failed: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	var symbol string
	var decimals int32
	for rows.Next() {
		var p PricePoint
		if err := rows.Scan(&p.Timestamp, &p.Price, &p.Source, &symbol, &decimals); err != nil {
			return nil, "", 0, err
		}
		points = append(points, p)
	}

	return points, symbol, decimals, nil
}

func getPriceChartV3(ctx context.Context, pool *pgxpool.Pool, poolAddr, token0, token1, wzil string, stables []string) ([]PricePoint, string, int32, error) {
	q := `
	WITH meta AS (
		SELECT 
			t0.address AS token0, t1.address AS token1,
			t0.symbol AS symbol0, t0.decimals AS dec0,
			t1.decimals AS dec1
		FROM tokens t0, tokens t1
		WHERE t0.address = $1 AND t1.address = $2
	),
	aligned AS (
		SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') 
		     - make_interval(hours := (EXTRACT(hour FROM now() AT TIME ZONE 'UTC')::int % 4)) AS end_ts
	),
	buckets AS (
		SELECT generate_series(
			(SELECT end_ts FROM aligned) - interval '7 days' + interval '4 hours',
			(SELECT end_ts FROM aligned),
			interval '4 hours'
		) AS ts
	),
	snaps AS (
		SELECT 
			b.ts,
			s.sqrt_price_x96
		FROM buckets b
		LEFT JOIN LATERAL (
			SELECT sqrt_price_x96
			FROM uniswap_v3_swaps
			WHERE pool = $3
			  AND timestamp <= EXTRACT(EPOCH FROM b.ts)
			ORDER BY timestamp DESC
			LIMIT 1
		) s ON TRUE
	),
	zil_prices AS (
		SELECT 
			b.ts,
			(SELECT price::numeric FROM prices_zil_usd_minute WHERE ts <= b.ts ORDER BY ts DESC LIMIT 1) AS zil_usd
		FROM buckets b
	)
	SELECT 
		to_char(b.ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
		CASE
			WHEN s.sqrt_price_x96 IS NULL THEN NULL
			WHEN LOWER($5) = LOWER(m.token1) THEN
				(POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) 
				* POWER(10::numeric, m.dec0 - m.dec1) * z.zil_usd
			WHEN LOWER($5) = LOWER(m.token0) THEN
				z.zil_usd
			WHEN LOWER(m.token1) = ANY($4::text[]) THEN
				(POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) 
				* POWER(10::numeric, m.dec0 - m.dec1)
			WHEN LOWER(m.token0) = ANY($4::text[]) THEN
				1
			ELSE NULL
		END::text AS price,
		CASE
			WHEN LOWER($5) = LOWER(m.token1) OR LOWER($5) = LOWER(m.token0) THEN 'wzil'
			WHEN LOWER(m.token1) = ANY($4::text[]) OR LOWER(m.token0) = ANY($4::text[]) THEN 'stable'
			ELSE 'none'
		END AS source,
		m.symbol0,
		m.dec0
	FROM buckets b
	CROSS JOIN meta m
	LEFT JOIN snaps s ON s.ts = b.ts
	LEFT JOIN zil_prices z ON z.ts = b.ts
	ORDER BY b.ts ASC`

	rows, err := pool.Query(ctx, q, token0, token1, poolAddr, stables, wzil)
	if err != nil {
		return nil, "", 0, fmt.Errorf("V3 price chart query failed: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	var symbol string
	var decimals int32
	for rows.Next() {
		var p PricePoint
		if err := rows.Scan(&p.Timestamp, &p.Price, &p.Source, &symbol, &decimals); err != nil {
			return nil, "", 0, err
		}
		points = append(points, p)
	}

	return points, symbol, decimals, nil
}

// GetTokenPriceChart returns a weighted price chart for a token aggregated from all pairs
func GetTokenPriceChart(ctx context.Context, pool *pgxpool.Pool, address string) (*PriceChartDTO, error) {
	wzilAddr, err := getWZILAddress(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get WZIL address: %w", err)
	}

	stableAddrs, err := getStablecoinAddresses(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get stablecoin addresses: %w", err)
	}

	q := `
	WITH meta AS (
		SELECT t.address AS token, t.symbol AS symbol, t.decimals AS dec
		FROM tokens t
		WHERE lower(t.address) = lower($1)
	),
	aligned AS (
		SELECT date_trunc('hour', now() AT TIME ZONE 'UTC')
		     - make_interval(hours := (EXTRACT(hour FROM now() AT TIME ZONE 'UTC')::int % 4)) AS end_ts
	),
	buckets AS (
		SELECT generate_series(
			(SELECT end_ts FROM aligned) - interval '7 days' + interval '4 hours',
			(SELECT end_ts FROM aligned),
			interval '4 hours'
		) AS ts
	),
	pools AS (
		SELECT
			dp.protocol,
			dp.address AS pool,
			dp.token0, dp.token1,
			t0.decimals AS dec0, t1.decimals AS dec1,
			(COALESCE(dp.liquidity_usd,0) + COALESCE(dp.volume_usd_24h,0))::numeric AS w,
			(lower(dp.token0) = lower($1)) AS is_target0,
			CASE
				WHEN lower(dp.token0) = lower($3) THEN 'wzil0'
				WHEN lower(dp.token1) = lower($3) THEN 'wzil1'
				WHEN lower(dp.token0) = ANY($2::text[]) THEN 'stable0'
				WHEN lower(dp.token1) = ANY($2::text[]) THEN 'stable1'
				ELSE 'none'
			END AS anchor
		FROM dex_pools dp
		JOIN tokens t0 ON t0.address = dp.token0
		JOIN tokens t1 ON t1.address = dp.token1
		WHERE (lower(dp.token0) = lower($1) OR lower(dp.token1) = lower($1))
		  AND (
			lower(dp.token0) = lower($3) OR lower(dp.token1) = lower($3)
			OR lower(dp.token0) = ANY($2::text[]) OR lower(dp.token1) = ANY($2::text[])
		  )
		  AND (COALESCE(dp.liquidity_usd,0) + COALESCE(dp.volume_usd_24h,0)) > 0
	),
	zil_prices AS (
		SELECT
			b.ts,
			(SELECT price::numeric
			 FROM prices_zil_usd_minute p
			 WHERE p.ts <= b.ts
			 ORDER BY p.ts DESC
			 LIMIT 1) AS zil_usd
		FROM buckets b
	),
	v2_prices AS (
		SELECT
			b.ts,
			p.pool,
			p.w,
			CASE
				WHEN s.reserve0 IS NULL OR s.reserve1 IS NULL THEN NULL
				ELSE
					CASE
						WHEN p.is_target0 THEN
							CASE p.anchor
								WHEN 'wzil1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)) * z.zil_usd
								WHEN 'wzil0' THEN z.zil_usd
								WHEN 'stable1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0))
								WHEN 'stable0' THEN 1::numeric
								ELSE NULL
							END
						ELSE
							CASE p.anchor
								WHEN 'wzil0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)) * z.zil_usd
								WHEN 'wzil1' THEN z.zil_usd
								WHEN 'stable0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0))
								WHEN 'stable1' THEN 1::numeric
								ELSE NULL
							END
					END
			END AS price_usd
		FROM buckets b
		JOIN pools p ON p.protocol = 'uniswap_v2'
		LEFT JOIN LATERAL (
			SELECT reserve0, reserve1
			FROM uniswap_v2_syncs s
			WHERE s.pair = p.pool
			  AND s.timestamp <= EXTRACT(EPOCH FROM b.ts)
			ORDER BY s.timestamp DESC
			LIMIT 1
		) s ON TRUE
		LEFT JOIN zil_prices z ON z.ts = b.ts
	),
	v3_prices AS (
		SELECT
			b.ts,
			p.pool,
			p.w,
			CASE
				WHEN s.sqrt_price_x96 IS NULL THEN NULL
				ELSE
					CASE
						WHEN p.is_target0 THEN
							CASE p.anchor
								WHEN 'wzil1' THEN ((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)) * z.zil_usd
								WHEN 'wzil0' THEN z.zil_usd
								WHEN 'stable1' THEN (POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)
								WHEN 'stable0' THEN 1::numeric
								ELSE NULL
							END
						ELSE
							CASE p.anchor
								WHEN 'wzil0' THEN (1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)) * z.zil_usd
								WHEN 'wzil1' THEN z.zil_usd
								WHEN 'stable0' THEN 1 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) * POWER(10::numeric, p.dec0 - p.dec1)),0)
								WHEN 'stable1' THEN 1::numeric
								ELSE NULL
							END
					END
			END AS price_usd
		FROM buckets b
		JOIN pools p ON p.protocol = 'uniswap_v3'
		LEFT JOIN LATERAL (
			SELECT sqrt_price_x96
			FROM uniswap_v3_swaps s
			WHERE s.pool = p.pool
			  AND s.timestamp <= EXTRACT(EPOCH FROM b.ts)
			ORDER BY s.timestamp DESC
			LIMIT 1
		) s ON TRUE
		LEFT JOIN zil_prices z ON z.ts = b.ts
	),
	all_prices AS (
		SELECT ts, pool, w, price_usd FROM v2_prices
		UNION ALL
		SELECT ts, pool, w, price_usd FROM v3_prices
	),
	aggregated AS (
		SELECT
			b.ts,
			NULLIF(SUM(CASE WHEN ap.price_usd IS NOT NULL THEN ap.w ELSE 0 END), 0) AS total_w,
			SUM(ap.price_usd * ap.w) AS w_price_sum
		FROM buckets b
		LEFT JOIN all_prices ap ON ap.ts = b.ts
		GROUP BY b.ts
	)
	SELECT
		to_char(a.ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS timestamp,
		CASE WHEN a.total_w > 0 THEN (a.w_price_sum / a.total_w)::text ELSE NULL END AS price,
		'weighted' AS source,
		m.symbol,
		m.dec AS decimals
	FROM aggregated a
	CROSS JOIN meta m
	ORDER BY a.ts ASC`

	rows, err := pool.Query(ctx, q, address, stableAddrs, wzilAddr)
	if err != nil {
		return nil, fmt.Errorf("token price chart query failed: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	var symbol string
	var decimals int32
	for rows.Next() {
		var p PricePoint
		if err := rows.Scan(&p.Timestamp, &p.Price, &p.Source, &symbol, &decimals); err != nil {
			return nil, err
		}
		points = append(points, p)
	}

	if symbol == "" {
		return nil, fmt.Errorf("token not found")
	}

	return &PriceChartDTO{
		Address: address,
		Protocol: "aggregated",
		BaseToken: TokenBaseDTO{
			Address:  address,
			Symbol:   symbol,
			Decimals: decimals,
		},
		Quote:     "USD",
		Timeframe: "7d",
		Interval:  "4h",
		Points:    points,
	}, nil
}
