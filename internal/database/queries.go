package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DTOs for API responses (lightweight, no ORM tags)
type TokenDTO struct {
	Address            string   `json:"address"`
	Symbol             *string  `json:"symbol,omitempty"`
	Name               *string  `json:"name,omitempty"`
	Decimals           *int32   `json:"decimals,omitempty"`
	TotalSupply        *string  `json:"total_supply,omitempty"`
	PriceUSD           *string  `json:"price_usd,omitempty"`
	PriceETH           *string  `json:"price_eth,omitempty"`
	MarketCapUSD       *string  `json:"market_cap_usd,omitempty"`
	LiquidityUSD       *string  `json:"liquidity_usd"`
	Volume24hUSD       *string  `json:"volume_24h_usd"`
	TotalVolumeUSD     *string  `json:"total_volume_usd,omitempty"`
	PriceChange24h     *string  `json:"price_change_24h"`
	PriceChange7d      *string  `json:"price_change_7d"`
	LogoURI            *string  `json:"logo_uri,omitempty"`
	Website            *string  `json:"website,omitempty"`
	Description        *string  `json:"description,omitempty"`
	FirstSeenBlock     *int64   `json:"first_seen_block,omitempty"`
	FirstSeenTimestamp *int64   `json:"first_seen_timestamp,omitempty"`
}

type PairDTO struct {
	Protocol       string  `json:"protocol"`
	Address        string  `json:"address"`
	Token0         string  `json:"token0"`
	Token1         string  `json:"token1"`
	Token0Symbol   *string `json:"token0_symbol,omitempty"`
	Token0Name     *string `json:"token0_name,omitempty"`
	Token1Symbol   *string `json:"token1_symbol,omitempty"`
	Token1Name     *string `json:"token1_name,omitempty"`
	Fee            *string `json:"fee,omitempty"`
	Reserve0       *string `json:"reserve0,omitempty"`
	Reserve1       *string `json:"reserve1,omitempty"`
	Liquidity      *string `json:"liquidity,omitempty"`
	LiquidityUSD   *string `json:"liquidity_usd,omitempty"`
	VolumeUSD      *string `json:"volume_usd,omitempty"`
	VolumeUSD24h   *string `json:"volume_usd_24h,omitempty"`
	PriceUSD       *string `json:"price_usd,omitempty"`
	PriceETH       *string `json:"price_eth,omitempty"`
	PriceChange24h *string `json:"price_change_24h"`
	PriceChange7d  *string `json:"price_change_7d"`
	TxnCount       *int64  `json:"txn_count,omitempty"`
}

// WZIL address (lowercase)
const wzilAddress = "0x94e18ae7dd5ee57b55f30c4b63e2760c09efb192"

// stablecoinSymbols contains symbols that should appear as the quote token (second)
var stablecoinSymbols = map[string]bool{
	"USDT": true, "USDC": true, "DAI": true, "BUSD": true,
	"zUSDT": true, "ZUSDT": true, "ZUSD": true, "XSGD": true, "kUSD": true,
}

// normalizePairOrder swaps token0/token1 if token0 is WZIL or a stablecoin,
// so the "interesting" token appears first in the pair display.
func (p *PairDTO) normalizePairOrder() {
	shouldSwap := false
	// Check if token0 is WZIL
	if strings.ToLower(p.Token0) == wzilAddress {
		shouldSwap = true
	}
	// Check if token0 is a stablecoin (by symbol)
	if !shouldSwap && p.Token0Symbol != nil && stablecoinSymbols[*p.Token0Symbol] {
		shouldSwap = true
	}
	if shouldSwap {
		// Swap all token0/token1 fields
		p.Token0, p.Token1 = p.Token1, p.Token0
		p.Token0Symbol, p.Token1Symbol = p.Token1Symbol, p.Token0Symbol
		p.Token0Name, p.Token1Name = p.Token1Name, p.Token0Name
		p.Reserve0, p.Reserve1 = p.Reserve1, p.Reserve0
	}
}

// normalizePairsList applies normalizePairOrder to all pairs
func normalizePairsList(pairs []PairDTO) []PairDTO {
	for i := range pairs {
		pairs[i].normalizePairOrder()
	}
	return pairs
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
	Maker        *string `json:"maker,omitempty"`
	Recipient    *string `json:"recipient,omitempty"`
	ToAddress    *string `json:"to_address,omitempty"`
	Amount0In    *string `json:"amount0_in,omitempty"`
	Amount1In    *string `json:"amount1_in,omitempty"`
	Amount0Out   *string `json:"amount0_out,omitempty"`
	Amount1Out   *string `json:"amount1_out,omitempty"`
	Liquidity    *string `json:"liquidity,omitempty"`
	AmountUSD    *string `json:"amount_usd,omitempty"`
	Token0Address  *string `json:"token0_address,omitempty"`
	Token0Symbol   *string `json:"token0_symbol,omitempty"`
	Token0Decimals *int32  `json:"token0_decimals,omitempty"`
	Token1Address  *string `json:"token1_address,omitempty"`
	Token1Symbol   *string `json:"token1_symbol,omitempty"`
	Token1Decimals *int32  `json:"token1_decimals,omitempty"`
}

type StatsDTO struct {
	TotalPairs        int64   `json:"total_pairs"`
	TotalTokens       int64   `json:"total_tokens"`
	TotalLiquidity    string  `json:"total_liquidity_usd"`
	TotalVolume24h    string  `json:"total_volume_usd_24h"`
	TotalVolumeAll    string  `json:"total_volume_usd_all"`
	ZilPriceUSD       *string `json:"zil_price_usd,omitempty"`
	ZilPriceChange24h *string `json:"zil_price_change_24h,omitempty"`
}

type BlockDTO struct {
	Number           int64            `json:"number"`
	Hash             string           `json:"hash"`
	ParentHash       string           `json:"parent_hash"`
	Timestamp        int64            `json:"timestamp"`
	GasLimit         *int64           `json:"gas_limit,omitempty"`
	GasUsed          *int64           `json:"gas_used,omitempty"`
	BaseFeePerGas    *string          `json:"base_fee_per_gas,omitempty"`
	TransactionCount int              `json:"transaction_count"`
	Transactions     []TransactionDTO `json:"transactions,omitempty"`
}

type TransactionDTO struct {
	Hash                  string        `json:"hash"`
	BlockNumber           int64         `json:"block_number"`
	TransactionIndex      int           `json:"transaction_index"`
	FromAddress           string        `json:"from_address"`
	ToAddress             *string       `json:"to_address,omitempty"`
	Value                 string        `json:"value"`
	GasPrice              *string       `json:"gas_price,omitempty"`
	GasLimit              *int64        `json:"gas_limit,omitempty"`
	GasUsed               *int64        `json:"gas_used,omitempty"`
	Nonce                 *int64        `json:"nonce,omitempty"`
	Status                *int          `json:"status,omitempty"`
	TransactionType       int           `json:"transaction_type"`
	OriginalTypeHex       *string       `json:"original_type_hex,omitempty"`
	MaxFeePerGas          *string       `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFeePerGas  *string       `json:"max_priority_fee_per_gas,omitempty"`
	EffectiveGasPrice     *string       `json:"effective_gas_price,omitempty"`
	ContractAddress       *string       `json:"contract_address,omitempty"`
	CumulativeGasUsed     *int64        `json:"cumulative_gas_used,omitempty"`
	Timestamp             int64         `json:"timestamp"`
	Events                []EventLogDTO `json:"events,omitempty"`
}

type EventLogDTO struct {
	LogIndex         int      `json:"log_index"`
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	TransactionIndex int      `json:"transaction_index"`
	BlockNumber      int64    `json:"block_number"`
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

type CandleDTO struct {
	Timestamp int64   `json:"t"` // unix seconds
	Open      *string `json:"o"`
	High      *string `json:"h"`
	Low       *string `json:"l"`
	Close     *string `json:"c"`
	Volume    *string `json:"v"`
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
		       CAST(total_supply AS TEXT),
		       CAST(price_usd AS TEXT),
		       CAST(price_eth AS TEXT),
		       CAST(market_cap_usd AS TEXT), 
		       CAST(total_liquidity_usd AS TEXT),
		       CAST(volume_24h_usd AS TEXT),
		       CAST(total_volume_usd AS TEXT),
		       CAST(price_change_24h AS TEXT),
		       CAST(price_change_7d AS TEXT),
		       logo_uri, website, description,
		       first_seen_block, first_seen_timestamp
		FROM tokens
		WHERE address = $1`

	var t TokenDTO
	err := pool.QueryRow(ctx, q, address).Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, 
		&t.TotalSupply, &t.PriceUSD, &t.PriceETH, &t.MarketCapUSD, &t.LiquidityUSD, 
		&t.Volume24hUSD, &t.TotalVolumeUSD, &t.PriceChange24h, &t.PriceChange7d,
		&t.LogoURI, &t.Website, &t.Description, &t.FirstSeenBlock, &t.FirstSeenTimestamp)
	if err != nil {
		return nil, fmt.Errorf("GetToken query failed: %w", err)
	}
	return &t, nil
}

func ListTokens(ctx context.Context, pool *pgxpool.Pool, limit, offset int, search *string) ([]TokenDTO, error) {
	q := `
		SELECT address, symbol, name, decimals,
		       CAST(total_supply AS TEXT),
		       CAST(price_usd AS TEXT),
		       CAST(price_eth AS TEXT),
		       CAST(market_cap_usd AS TEXT), 
		       CAST(total_liquidity_usd AS TEXT),
		       CAST(volume_24h_usd AS TEXT),
		       CAST(total_volume_usd AS TEXT),
		       CAST(price_change_24h AS TEXT),
		       CAST(price_change_7d AS TEXT),
		       logo_uri, website, description,
		       first_seen_block, first_seen_timestamp
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
			&t.TotalSupply, &t.PriceUSD, &t.PriceETH, &t.MarketCapUSD, &t.LiquidityUSD, 
			&t.Volume24hUSD, &t.TotalVolumeUSD, &t.PriceChange24h, &t.PriceChange7d,
			&t.LogoURI, &t.Website, &t.Description, &t.FirstSeenBlock, &t.FirstSeenTimestamp); err != nil {
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
		       CAST(liquidity_usd AS TEXT), CAST(volume_usd AS TEXT), CAST(volume_usd_24h AS TEXT),
		       CAST(price_usd AS TEXT), CAST(price_eth AS TEXT), txn_count
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
			&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h,
			&p.PriceUSD, &p.PriceETH, &p.TxnCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return normalizePairsList(out), nil
}

// GetPair reads from dex_pools view to be protocol-agnostic (V2+V3)
func GetPair(ctx context.Context, pool *pgxpool.Pool, address string) (*PairDTO, error) {
	q := `
		SELECT dp.protocol, dp.address, dp.token0, dp.token1,
		       dp.token0_symbol, dp.token0_name, dp.token1_symbol, dp.token1_name,
		       CAST(dp.fee AS TEXT), CAST(dp.reserve0 AS TEXT), CAST(dp.reserve1 AS TEXT), CAST(dp.liquidity AS TEXT),
		       CAST(dp.liquidity_usd AS TEXT), CAST(dp.volume_usd AS TEXT), CAST(dp.volume_usd_24h AS TEXT),
		       CAST(dp.price_usd AS TEXT), CAST(dp.price_eth AS TEXT),
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_24h AS TEXT) ELSE CAST(v3.price_change_24h AS TEXT) END,
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_7d AS TEXT) ELSE CAST(v3.price_change_7d AS TEXT) END,
		       dp.txn_count
		FROM dex_pools dp
		LEFT JOIN uniswap_v2_pairs v2 ON dp.protocol = 'uniswap_v2' AND v2.address = dp.address
		LEFT JOIN uniswap_v3_pools v3 ON dp.protocol = 'uniswap_v3' AND v3.address = dp.address
		WHERE dp.address = $1`

	var p PairDTO
	err := pool.QueryRow(ctx, q, address).Scan(
		&p.Protocol, &p.Address, &p.Token0, &p.Token1,
		&p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name,
		&p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity,
		&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h,
		&p.PriceUSD, &p.PriceETH, &p.PriceChange24h, &p.PriceChange7d, &p.TxnCount)
	if err != nil {
		return nil, fmt.Errorf("GetPair query failed: %w", err)
	}
	p.normalizePairOrder()
	return &p, nil
}

func GetPairsByAddresses(ctx context.Context, pool *pgxpool.Pool, addresses []string) ([]PairDTO, error) {
	if len(addresses) == 0 {
		return []PairDTO{}, nil
	}

	q := `
		SELECT dp.protocol, dp.address, dp.token0, dp.token1,
		       dp.token0_symbol, dp.token0_name, dp.token1_symbol, dp.token1_name,
		       CAST(dp.fee AS TEXT), CAST(dp.reserve0 AS TEXT), CAST(dp.reserve1 AS TEXT), CAST(dp.liquidity AS TEXT),
		       CAST(dp.liquidity_usd AS TEXT), CAST(dp.volume_usd AS TEXT), CAST(dp.volume_usd_24h AS TEXT),
		       CAST(dp.price_usd AS TEXT), CAST(dp.price_eth AS TEXT),
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_24h AS TEXT) ELSE CAST(v3.price_change_24h AS TEXT) END,
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_7d AS TEXT) ELSE CAST(v3.price_change_7d AS TEXT) END,
		       dp.txn_count
		FROM dex_pools dp
		LEFT JOIN uniswap_v2_pairs v2 ON dp.protocol = 'uniswap_v2' AND v2.address = dp.address
		LEFT JOIN uniswap_v3_pools v3 ON dp.protocol = 'uniswap_v3' AND v3.address = dp.address
		WHERE dp.address = ANY($1)`

	rows, err := pool.Query(ctx, q, addresses)
	if err != nil {
		return nil, fmt.Errorf("GetPairsByAddresses query failed: %w", err)
	}
	defer rows.Close()

	var pairs []PairDTO
	for rows.Next() {
		var p PairDTO
		err := rows.Scan(
			&p.Protocol, &p.Address, &p.Token0, &p.Token1,
			&p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name,
			&p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity,
			&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h,
			&p.PriceUSD, &p.PriceETH, &p.PriceChange24h, &p.PriceChange7d, &p.TxnCount)
		if err != nil {
			return nil, fmt.Errorf("GetPairsByAddresses scan failed: %w", err)
		}
		pairs = append(pairs, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetPairsByAddresses rows error: %w", err)
	}

	return normalizePairsList(pairs), nil
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
		SELECT dp.protocol, dp.address, dp.token0, dp.token1,
		       dp.token0_symbol, dp.token0_name, dp.token1_symbol, dp.token1_name,
		       CAST(dp.fee AS TEXT), CAST(dp.reserve0 AS TEXT), CAST(dp.reserve1 AS TEXT), CAST(dp.liquidity AS TEXT),
		       CAST(dp.liquidity_usd AS TEXT), CAST(dp.volume_usd AS TEXT), CAST(dp.volume_usd_24h AS TEXT),
		       CAST(dp.price_usd AS TEXT), CAST(dp.price_eth AS TEXT),
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_24h AS TEXT) ELSE CAST(v3.price_change_24h AS TEXT) END,
		       CASE WHEN dp.protocol = 'uniswap_v2' THEN CAST(v2.price_change_7d AS TEXT) ELSE CAST(v3.price_change_7d AS TEXT) END,
		       dp.txn_count
		FROM dex_pools dp
		LEFT JOIN uniswap_v2_pairs v2 ON dp.protocol = 'uniswap_v2' AND v2.address = dp.address
		LEFT JOIN uniswap_v3_pools v3 ON dp.protocol = 'uniswap_v3' AND v3.address = dp.address
		WHERE dp.liquidity_usd > 0
		ORDER BY CAST(%s AS NUMERIC) %s NULLS LAST, CAST(dp.liquidity_usd AS NUMERIC) DESC NULLS LAST
		LIMIT $1 OFFSET $2`, sortColumn, strings.ToUpper(sortOrder))

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListPairs query failed: %w", err)
	}
	defer rows.Close()

	var out []PairDTO
	for rows.Next() {
		var p PairDTO
		if err := rows.Scan(&p.Protocol, &p.Address, &p.Token0, &p.Token1,
			&p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name,
			&p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity,
			&p.LiquidityUSD, &p.VolumeUSD, &p.VolumeUSD24h,
			&p.PriceUSD, &p.PriceETH, &p.PriceChange24h, &p.PriceChange7d, &p.TxnCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return normalizePairsList(out), nil
}

// ListEventsByAddress returns all DEX events where address is sender, recipient, or to_address
func ListEventsByAddress(ctx context.Context, pool *pgxpool.Pool, address string, eventType *string, protocol *string, limit, offset int) ([]PairEventDTO, error) {
	q := `
		SELECT 
			e.protocol, e.event_type, e.id, e.transaction_hash, e.log_index, e.block_number, e.timestamp,
			e.address, e.sender, e.recipient, e.to_address,
			CAST(e.amount0_in AS TEXT), CAST(e.amount1_in AS TEXT), 
			CAST(e.amount0_out AS TEXT), CAST(e.amount1_out AS TEXT),
			CAST(e.liquidity AS TEXT), CAST(e.amount_usd AS TEXT),
			COALESCE(pair.token0, pool.token0) AS token0_address,
			t0.symbol AS token0_symbol,
			t0.decimals AS token0_decimals,
			COALESCE(pair.token1, pool.token1) AS token1_address,
			t1.symbol AS token1_symbol,
			t1.decimals AS token1_decimals,
			e.maker
		FROM dex_pair_events e
		LEFT JOIN uniswap_v2_pairs pair ON e.protocol = 'uniswap_v2' AND e.address = pair.address
		LEFT JOIN uniswap_v3_pools pool ON e.protocol = 'uniswap_v3' AND e.address = pool.address
		LEFT JOIN tokens t0 ON COALESCE(pair.token0, pool.token0) = t0.address
		LEFT JOIN tokens t1 ON COALESCE(pair.token1, pool.token1) = t1.address
		WHERE (lower(e.sender) = lower($1) OR lower(e.recipient) = lower($1) OR lower(e.to_address) = lower($1))
		  AND ($2::text IS NULL OR e.event_type = $2)
		  AND ($3::text IS NULL OR e.protocol = $3)
		ORDER BY e.timestamp DESC, e.log_index DESC
		LIMIT $4 OFFSET $5`

	rows, err := pool.Query(ctx, q, address, eventType, protocol, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListEventsByAddress query failed: %w", err)
	}
	defer rows.Close()

	var out []PairEventDTO
	for rows.Next() {
		var e PairEventDTO
		if err := rows.Scan(&e.Protocol, &e.EventType, &e.ID, &e.TransactionHash, &e.LogIndex, &e.BlockNumber, &e.Timestamp,
			&e.Address, &e.Sender, &e.Recipient, &e.ToAddress, &e.Amount0In, &e.Amount1In, &e.Amount0Out, &e.Amount1Out, &e.Liquidity, &e.AmountUSD,
			&e.Token0Address, &e.Token0Symbol, &e.Token0Decimals, &e.Token1Address, &e.Token1Symbol, &e.Token1Decimals, &e.Maker); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// ListPairEvents returns unified events for a pair/pool address, with optional filters
func ListPairEvents(ctx context.Context, pool *pgxpool.Pool, address string, eventType *string, protocol *string, limit, offset int) ([]PairEventDTO, error) {
	q := `
		SELECT protocol, event_type, id, transaction_hash, log_index, block_number, timestamp,
		       address, sender, recipient, to_address,
		       CAST(amount0_in AS TEXT), CAST(amount1_in AS TEXT), CAST(amount0_out AS TEXT), CAST(amount1_out AS TEXT),
		       CAST(liquidity AS TEXT), CAST(amount_usd AS TEXT), maker
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
			&e.Address, &e.Sender, &e.Recipient, &e.ToAddress, &e.Amount0In, &e.Amount1In, &e.Amount0Out, &e.Amount1Out, &e.Liquidity, &e.AmountUSD, &e.Maker); err != nil {
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

	// Get current ZIL price and 24h change
	zilQ := `
		WITH current_price AS (
			SELECT price FROM prices_zil_usd_minute ORDER BY ts DESC LIMIT 1
		),
		price_24h_ago AS (
			SELECT price FROM prices_zil_usd_minute 
			WHERE ts <= NOW() - INTERVAL '24 hours' 
			ORDER BY ts DESC LIMIT 1
		)
		SELECT 
			c.price::text,
			CASE WHEN p.price > 0 
				THEN to_char(((c.price - p.price) / p.price) * 100, 'FM990.99')
				ELSE NULL 
			END
		FROM current_price c
		LEFT JOIN price_24h_ago p ON true
	`
	var zilPrice, zilChange24h *string
	if err := pool.QueryRow(ctx, zilQ).Scan(&zilPrice, &zilChange24h); err == nil {
		stats.ZilPriceUSD = zilPrice
		stats.ZilPriceChange24h = zilChange24h
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
	
	transactions, err := ListTransactionsByBlock(ctx, pool, number)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch block transactions: %w", err)
	}
	b.Transactions = transactions
	
	return &b, nil
}

// ListTransactions returns a paginated list of transactions ordered by block number and index descending
func ListTransactions(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]TransactionDTO, error) {
	q := `
		SELECT t.hash, t.block_number, t.transaction_index, t.from_address, t.to_address,
		       CAST(t.value AS TEXT), CAST(t.gas_price AS TEXT), t.gas_limit, t.gas_used, t.nonce, t.status,
		       t.transaction_type, t.original_type_hex,
		       CAST(t.max_fee_per_gas AS TEXT), CAST(t.max_priority_fee_per_gas AS TEXT),
		       CAST(t.effective_gas_price AS TEXT), t.contract_address, t.cumulative_gas_used,
		       b.timestamp
		FROM transactions t
		JOIN blocks b ON t.block_number = b.number
		ORDER BY t.block_number DESC, t.transaction_index DESC
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
			&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed,
			&tx.Timestamp); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, nil
}

// ListTransactionsByAddress returns a paginated list of transactions for a specific address
func ListTransactionsByAddress(ctx context.Context, pool *pgxpool.Pool, address string, limit, offset int) ([]TransactionDTO, error) {
	q := `
		SELECT t.hash, t.block_number, t.transaction_index, t.from_address, t.to_address,
		       CAST(t.value AS TEXT), CAST(t.gas_price AS TEXT), t.gas_limit, t.gas_used, t.nonce, t.status,
		       t.transaction_type, t.original_type_hex,
		       CAST(t.max_fee_per_gas AS TEXT), CAST(t.max_priority_fee_per_gas AS TEXT),
		       CAST(t.effective_gas_price AS TEXT), t.contract_address, t.cumulative_gas_used,
		       b.timestamp
		FROM transactions t
		JOIN blocks b ON t.block_number = b.number
		WHERE lower(t.from_address) = lower($1) OR lower(t.to_address) = lower($1)
		ORDER BY t.block_number DESC, t.transaction_index DESC
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
			&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed,
			&tx.Timestamp); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, nil
}

// ListTransactionsByBlock returns all transactions for a specific block number
func ListTransactionsByBlock(ctx context.Context, pool *pgxpool.Pool, blockNumber int64) ([]TransactionDTO, error) {
	q := `
		SELECT t.hash, t.block_number, t.transaction_index, t.from_address, t.to_address,
		       CAST(t.value AS TEXT), CAST(t.gas_price AS TEXT), t.gas_limit, t.gas_used, t.nonce, t.status,
		       t.transaction_type, t.original_type_hex,
		       CAST(t.max_fee_per_gas AS TEXT), CAST(t.max_priority_fee_per_gas AS TEXT),
		       CAST(t.effective_gas_price AS TEXT), t.contract_address, t.cumulative_gas_used,
		       b.timestamp
		FROM transactions t
		JOIN blocks b ON t.block_number = b.number
		WHERE t.block_number = $1
		ORDER BY t.transaction_index ASC`

	rows, err := pool.Query(ctx, q, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ListTransactionsByBlock query failed: %w", err)
	}
	defer rows.Close()

	var out []TransactionDTO
	for rows.Next() {
		var tx TransactionDTO
		if err := rows.Scan(&tx.Hash, &tx.BlockNumber, &tx.TransactionIndex, &tx.FromAddress, &tx.ToAddress,
			&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.GasUsed, &tx.Nonce, &tx.Status,
			&tx.TransactionType, &tx.OriginalTypeHex,
			&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed,
			&tx.Timestamp); err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, nil
}

// GetTransaction returns a single transaction by hash
func GetTransaction(ctx context.Context, pool *pgxpool.Pool, hash string) (*TransactionDTO, error) {
	q := `
		SELECT t.hash, t.block_number, t.transaction_index, t.from_address, t.to_address,
		       CAST(t.value AS TEXT), CAST(t.gas_price AS TEXT), t.gas_limit, t.gas_used, t.nonce, t.status,
		       t.transaction_type, t.original_type_hex,
		       CAST(t.max_fee_per_gas AS TEXT), CAST(t.max_priority_fee_per_gas AS TEXT),
		       CAST(t.effective_gas_price AS TEXT), t.contract_address, t.cumulative_gas_used,
		       b.timestamp
		FROM transactions t
		JOIN blocks b ON t.block_number = b.number
		WHERE t.hash = $1`

	var tx TransactionDTO
	err := pool.QueryRow(ctx, q, hash).Scan(&tx.Hash, &tx.BlockNumber, &tx.TransactionIndex, &tx.FromAddress, &tx.ToAddress,
		&tx.Value, &tx.GasPrice, &tx.GasLimit, &tx.GasUsed, &tx.Nonce, &tx.Status,
		&tx.TransactionType, &tx.OriginalTypeHex,
		&tx.MaxFeePerGas, &tx.MaxPriorityFeePerGas, &tx.EffectiveGasPrice, &tx.ContractAddress, &tx.CumulativeGasUsed,
		&tx.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("GetTransaction query failed: %w", err)
	}
	return &tx, nil
}

// GetEventLogsByTransaction returns all event logs for a transaction
func GetEventLogsByTransaction(ctx context.Context, pool *pgxpool.Pool, hash string) ([]EventLogDTO, error) {
	q := `
		SELECT log_index, address, topics, data, transaction_index, block_number
		FROM event_logs
		WHERE transaction_hash = $1
		ORDER BY log_index ASC`

	rows, err := pool.Query(ctx, q, hash)
	if err != nil {
		return nil, fmt.Errorf("GetEventLogsByTransaction query failed: %w", err)
	}
	defer rows.Close()

	var events []EventLogDTO
	for rows.Next() {
		var event EventLogDTO
		err := rows.Scan(&event.LogIndex, &event.Address, &event.Topics, &event.Data, &event.TransactionIndex, &event.BlockNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event log: %w", err)
		}
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event logs: %w", err)
	}
	return events, nil
}

// GetPairPriceChart returns 42 price points (every 4h) over 7 days for a pair/pool
func GetPairPriceChart(ctx context.Context, pool *pgxpool.Pool, address string) (*PriceChartDTO, error) {
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

	// Query RAW token order from the database (not normalized)
	var protocol, rawToken0, rawToken1 string
	err = pool.QueryRow(ctx, `
		SELECT 'uniswap_v2', token0, token1 FROM uniswap_v2_pairs WHERE address = $1
		UNION ALL
		SELECT 'uniswap_v3', token0, token1 FROM uniswap_v3_pools WHERE address = $1
		LIMIT 1
	`, address).Scan(&protocol, &rawToken0, &rawToken1)
	if err != nil {
		return nil, fmt.Errorf("pair not found: %w", err)
	}

	var points []PricePoint
	var baseTokenAddr, baseTokenSymbol string
	var baseTokenDecimals int32

	if protocol == "uniswap_v2" {
		points, baseTokenAddr, baseTokenSymbol, baseTokenDecimals, err = getPriceChartV2(ctx, pool, address, rawToken0, rawToken1, wzilAddr, stableAddrs)
	} else {
		points, baseTokenAddr, baseTokenSymbol, baseTokenDecimals, err = getPriceChartV3(ctx, pool, address, rawToken0, rawToken1, wzilAddr, stableAddrs)
	}
	if err != nil {
		return nil, err
	}

	return &PriceChartDTO{
		Address:   address,
		Protocol:  protocol,
		BaseToken: TokenBaseDTO{
			Address:  baseTokenAddr,
			Symbol:   baseTokenSymbol,
			Decimals: baseTokenDecimals,
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
	rows, err := pool.Query(ctx, "SELECT address FROM tokens WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'zUSDT', 'ZUSD', 'XSGD', 'kUSD'])")
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

func getPriceChartV2(ctx context.Context, pool *pgxpool.Pool, pairAddr, token0, token1, wzil string, stables []string) ([]PricePoint, string, string, int32, error) {
	// V2 price chart with proper handling of all token pair combinations
	// Returns the price of the "interesting" token (non-WZIL, non-stablecoin) over time
	q := `
	WITH meta AS (
		SELECT
			t0.address AS token0, t1.address AS token1,
			t0.symbol AS symbol0, t1.symbol AS symbol1,
			t0.decimals AS dec0, t1.decimals AS dec1
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
		ROUND(
			(CASE
				-- token0=WZIL, token1=stablecoin → show stablecoin price = (1/rate) * zil
				WHEN LOWER($5) = LOWER(m.token0) AND LOWER(m.token1) = ANY($4::text[]) THEN
					((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0)) * z.zil_usd
				-- token0=WZIL → show token1 price = (1/rate) * zil
				WHEN LOWER($5) = LOWER(m.token0) THEN
					((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0)) * z.zil_usd
				-- token0=stablecoin, token1=WZIL → show WZIL price = zil
				WHEN LOWER(m.token0) = ANY($4::text[]) AND LOWER($5) = LOWER(m.token1) THEN
					z.zil_usd
				-- token0=stablecoin → show token1 price (via stablecoin)
				WHEN LOWER(m.token0) = ANY($4::text[]) THEN
					((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0))
				-- token1=WZIL → show token0 price = rate * zil
				WHEN LOWER($5) = LOWER(m.token1) THEN
					((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0)) * z.zil_usd
				-- token1=stablecoin → show token0 price = rate * stablecoin
				WHEN LOWER(m.token1) = ANY($4::text[]) THEN
					((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0))
				ELSE NULL
			END)::numeric, 8
		)::text AS price,
		CASE
			WHEN LOWER($5) = LOWER(m.token1) OR LOWER($5) = LOWER(m.token0) THEN 'wzil'
			WHEN LOWER(m.token1) = ANY($4::text[]) OR LOWER(m.token0) = ANY($4::text[]) THEN 'stable'
			ELSE 'none'
		END AS source,
		-- Return the "interesting" token info (non-WZIL, non-stablecoin)
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.token1
			ELSE m.token0
		END AS base_addr,
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.symbol1
			ELSE m.symbol0
		END AS base_symbol,
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.dec1
			ELSE m.dec0
		END AS base_dec
	FROM buckets b
	CROSS JOIN meta m
	LEFT JOIN snaps s ON s.ts = b.ts
	LEFT JOIN zil_prices z ON z.ts = b.ts
	ORDER BY b.ts ASC`

	rows, err := pool.Query(ctx, q, token0, token1, pairAddr, stables, wzil)
	if err != nil {
		return nil, "", "", 0, fmt.Errorf("V2 price chart query failed: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	var baseAddr, baseSymbol string
	var baseDecimals int32
	for rows.Next() {
		var p PricePoint
		if err := rows.Scan(&p.Timestamp, &p.Price, &p.Source, &baseAddr, &baseSymbol, &baseDecimals); err != nil {
			return nil, "", "", 0, err
		}
		points = append(points, p)
	}

	return points, baseAddr, baseSymbol, baseDecimals, nil
}

func getPriceChartV3(ctx context.Context, pool *pgxpool.Pool, poolAddr, token0, token1, wzil string, stables []string) ([]PricePoint, string, string, int32, error) {
	// V3 price chart with proper handling of all token pair combinations
	// rate = (sqrt_price_x96^2 / 2^192) / 10^(dec1-dec0) = token0 price in token1
	q := `
	WITH meta AS (
		SELECT
			t0.address AS token0, t1.address AS token1,
			t0.symbol AS symbol0, t1.symbol AS symbol1,
			t0.decimals AS dec0, t1.decimals AS dec1
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
		ROUND(
			(CASE
				WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 OR s.sqrt_price_x96 < 1000000 OR s.sqrt_price_x96 > 1e38 THEN NULL
				-- token0=WZIL, token1=stablecoin → show stablecoin price = (1/rate) * zil
				WHEN LOWER($5) = LOWER(m.token0) AND LOWER(m.token1) = ANY($4::text[]) THEN
					(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
					 POWER(10::numeric, m.dec1 - m.dec0), 0)) * z.zil_usd
				-- token0=WZIL → show token1 price = (1/rate) * zil
				WHEN LOWER($5) = LOWER(m.token0) THEN
					(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
					 POWER(10::numeric, m.dec1 - m.dec0), 0)) * z.zil_usd
				-- token0=stablecoin, token1=WZIL → show WZIL price = zil
				WHEN LOWER(m.token0) = ANY($4::text[]) AND LOWER($5) = LOWER(m.token1) THEN
					z.zil_usd
				-- token0=stablecoin → show token1 price (via stablecoin)
				WHEN LOWER(m.token0) = ANY($4::text[]) THEN
					(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
					 POWER(10::numeric, m.dec1 - m.dec0), 0))
				-- token1=WZIL → show token0 price = rate * zil
				WHEN LOWER($5) = LOWER(m.token1) THEN
					((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
					 POWER(10::numeric, m.dec1 - m.dec0)) * z.zil_usd
				-- token1=stablecoin → show token0 price = rate * stablecoin
				WHEN LOWER(m.token1) = ANY($4::text[]) THEN
					((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
					 POWER(10::numeric, m.dec1 - m.dec0))
				ELSE NULL
			END)::numeric, 8
		)::text AS price,
		CASE
			WHEN LOWER($5) = LOWER(m.token1) OR LOWER($5) = LOWER(m.token0) THEN 'wzil'
			WHEN LOWER(m.token1) = ANY($4::text[]) OR LOWER(m.token0) = ANY($4::text[]) THEN 'stable'
			ELSE 'none'
		END AS source,
		-- Return the "interesting" token info (non-WZIL, non-stablecoin)
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.token1
			ELSE m.token0
		END AS base_addr,
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.symbol1
			ELSE m.symbol0
		END AS base_symbol,
		CASE
			WHEN LOWER($5) = LOWER(m.token0) OR LOWER(m.token0) = ANY($4::text[]) THEN m.dec1
			ELSE m.dec0
		END AS base_dec
	FROM buckets b
	CROSS JOIN meta m
	LEFT JOIN snaps s ON s.ts = b.ts
	LEFT JOIN zil_prices z ON z.ts = b.ts
	ORDER BY b.ts ASC`

	rows, err := pool.Query(ctx, q, token0, token1, poolAddr, stables, wzil)
	if err != nil {
		return nil, "", "", 0, fmt.Errorf("V3 price chart query failed: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	var baseAddr, baseSymbol string
	var baseDecimals int32
	for rows.Next() {
		var p PricePoint
		if err := rows.Scan(&p.Timestamp, &p.Price, &p.Source, &baseAddr, &baseSymbol, &baseDecimals); err != nil {
			return nil, "", "", 0, err
		}
		points = append(points, p)
	}

	return points, baseAddr, baseSymbol, baseDecimals, nil
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
			COALESCE(t0.price_usd, 1) AS price0,
			COALESCE(t1.price_usd, 1) AS price1,
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
		  AND (COALESCE(dp.liquidity_usd,0) + COALESCE(dp.volume_usd_24h,0)) >= 1.0
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
								-- target=token0, anchor=WZIL in token1 → rate * zil_usd
								WHEN 'wzil1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)) * z.zil_usd
								-- target=token0=WZIL → zil_usd
								WHEN 'wzil0' THEN z.zil_usd
								-- target=token0, anchor=stablecoin in token1 → rate * stablecoin_price
								WHEN 'stable1' THEN ((s.reserve1::numeric / POWER(10::numeric, p.dec1)) / NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0)) * p.price1
								-- target=token0=stablecoin → use its known price
								WHEN 'stable0' THEN p.price0
								ELSE NULL
							END
						ELSE
							CASE p.anchor
								-- target=token1, anchor=WZIL in token0 → (1/rate) * zil_usd
								WHEN 'wzil0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)) * z.zil_usd
								-- target=token1=WZIL → zil_usd
								WHEN 'wzil1' THEN z.zil_usd
								-- target=token1, anchor=stablecoin in token0 → (1/rate) * stablecoin_price
								WHEN 'stable0' THEN (NULLIF((s.reserve0::numeric / POWER(10::numeric, p.dec0)),0) / NULLIF((s.reserve1::numeric / POWER(10::numeric, p.dec1)),0)) * p.price0
								-- target=token1=stablecoin → use its known price
								WHEN 'stable1' THEN p.price1
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
								-- target=token0, anchor=WZIL in token1 → rate * zil_usd
								-- V3 rate = (sqrt^2/2^192) / 10^(dec1-dec0) = token0 price in token1
								WHEN 'wzil1' THEN ((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) / POWER(10::numeric, p.dec1 - p.dec0)) * z.zil_usd
								-- target=token0=WZIL → zil_usd
								WHEN 'wzil0' THEN z.zil_usd
								-- target=token0, anchor=stablecoin in token1 → rate * stablecoin_price
								WHEN 'stable1' THEN ((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) / POWER(10::numeric, p.dec1 - p.dec0)) * p.price1
								-- target=token0=stablecoin → use its known price
								WHEN 'stable0' THEN p.price0
								ELSE NULL
							END
						ELSE
							CASE p.anchor
								-- target=token1, anchor=WZIL in token0 → (1/rate) * zil_usd
								WHEN 'wzil0' THEN (1.0 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) / POWER(10::numeric, p.dec1 - p.dec0)),0)) * z.zil_usd
								-- target=token1=WZIL → zil_usd
								WHEN 'wzil1' THEN z.zil_usd
								-- target=token1, anchor=stablecoin in token0 → (1/rate) * stablecoin_price
								WHEN 'stable0' THEN (1.0 / NULLIF(((POWER(s.sqrt_price_x96::numeric,2) / POWER(2::numeric,192)) / POWER(10::numeric, p.dec1 - p.dec0)),0)) * p.price0
								-- target=token1=stablecoin → use its known price
								WHEN 'stable1' THEN p.price1
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
		CASE WHEN a.total_w > 0 THEN ROUND((a.w_price_sum / a.total_w)::numeric, 8)::text ELSE NULL END AS price,
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

func GetPairOHLCV(
	ctx context.Context,
	pool *pgxpool.Pool,
	address string,
	from, to int64,
	interval string, // e.g. "1 minute", "5 minutes", "1 day"
) ([]CandleDTO, error) {
	// Query RAW token order from the database (not normalized)
	var protocol, rawToken0, rawToken1 string
	err := pool.QueryRow(ctx, `
		SELECT 'uniswap_v2', token0, token1 FROM uniswap_v2_pairs WHERE address = $1
		UNION ALL
		SELECT 'uniswap_v3', token0, token1 FROM uniswap_v3_pools WHERE address = $1
		LIMIT 1
	`, address).Scan(&protocol, &rawToken0, &rawToken1)
	if err != nil {
		return nil, fmt.Errorf("pair not found: %w", err)
	}

	if protocol == "uniswap_v2" {
		return getPairOHLCVV2(ctx, pool, rawToken0, rawToken1, address, from, to, interval)
	}
	return getPairOHLCVV3(ctx, pool, rawToken0, rawToken1, address, from, to, interval)
}

func getPairOHLCVV2(
	ctx context.Context,
	pool *pgxpool.Pool,
	token0, token1 string,
	address string,
	from, to int64,
	interval string,
) ([]CandleDTO, error) {
	wzilAddr, err := getWZILAddress(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get WZIL address: %w", err)
	}
	stableAddrs, err := getStablecoinAddresses(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get stablecoin addresses: %w", err)
	}

	// V2 OHLCV with proper price calculation matching dex_pools view
	q := `
		WITH meta AS (
			SELECT
				t0.address  AS token0,
				t1.address  AS token1,
				t0.decimals AS dec0,
				t1.decimals AS dec1
			FROM tokens t0, tokens t1
			WHERE t0.address = $1 AND t1.address = $2
		),
		params AS (
			SELECT
				to_timestamp($6) AT TIME ZONE 'UTC' AS from_ts,
				to_timestamp($7) AT TIME ZONE 'UTC' AS to_ts
		),
		buckets AS (
			SELECT
				gs AS bucket_start,
				gs + $8::interval AS bucket_end
			FROM params p,
				 generate_series(
					 p.from_ts,
					 p.to_ts,
					 $8::interval
				 ) AS gs
		),
		price_points AS (
			SELECT
				to_timestamp(s.timestamp) AT TIME ZONE 'UTC' AS ts,
				CASE
					-- token0=WZIL, token1=stablecoin → show stablecoin price
					WHEN LOWER($5) = LOWER(m.token0) AND LOWER(m.token1) = ANY($4::text[]) THEN
						((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0)) * z.zil_usd
					-- token0=WZIL → show token1 price
					WHEN LOWER($5) = LOWER(m.token0) THEN
						((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0)) * z.zil_usd
					-- token0=stablecoin, token1=WZIL → show WZIL price
					WHEN LOWER(m.token0) = ANY($4::text[]) AND LOWER($5) = LOWER(m.token1) THEN
						z.zil_usd
					-- token0=stablecoin → show token1 price
					WHEN LOWER(m.token0) = ANY($4::text[]) THEN
						((s.reserve0::numeric / POWER(10, m.dec0)) / NULLIF(s.reserve1::numeric / POWER(10, m.dec1), 0))
					-- token1=WZIL → show token0 price
					WHEN LOWER($5) = LOWER(m.token1) THEN
						((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0)) * z.zil_usd
					-- token1=stablecoin → show token0 price
					WHEN LOWER(m.token1) = ANY($4::text[]) THEN
						((s.reserve1::numeric / POWER(10, m.dec1)) / NULLIF(s.reserve0::numeric / POWER(10, m.dec0), 0))
					ELSE NULL
				END AS price_usd
			FROM uniswap_v2_syncs s
			CROSS JOIN meta m
			LEFT JOIN LATERAL (
				SELECT price::numeric AS zil_usd
				FROM prices_zil_usd_minute p
				WHERE p.ts <= to_timestamp(s.timestamp) AT TIME ZONE 'UTC'
				ORDER BY p.ts DESC
				LIMIT 1
			) z ON TRUE
			WHERE s.pair = $3
			  AND s.timestamp <= $7
		),
		swap_points AS (
			SELECT
				to_timestamp(sw.timestamp) AT TIME ZONE 'UTC' AS ts,
				ABS(sw.amount_usd) AS volume_usd
			FROM uniswap_v2_swaps sw
			WHERE sw.pair = $3
			  AND sw.timestamp >= $6
			  AND sw.timestamp <= $7
		),
		agg AS (
			SELECT
				b.bucket_start,
				(
					SELECT price_usd
					FROM price_points pp
					WHERE pp.ts <= b.bucket_end AND pp.price_usd IS NOT NULL
					ORDER BY pp.ts DESC
					LIMIT 1
				) AS close_price,
				(
					SELECT price_usd
					FROM price_points pp
					WHERE pp.ts <= b.bucket_start AND pp.price_usd IS NOT NULL
					ORDER BY pp.ts DESC
					LIMIT 1
				) AS open_price,
				(
					SELECT MAX(price_usd)
					FROM price_points pp
					WHERE pp.ts > b.bucket_start
					  AND pp.ts <= b.bucket_end
				) AS high_price,
				(
					SELECT MIN(price_usd)
					FROM price_points pp
					WHERE pp.ts > b.bucket_start
					  AND pp.ts <= b.bucket_end
				) AS low_price,
				(
					SELECT COALESCE(SUM(volume_usd), 0)
					FROM swap_points sp
					WHERE sp.ts > b.bucket_start
					  AND sp.ts <= b.bucket_end
				) AS volume_usd
			FROM buckets b
		)
		SELECT
			EXTRACT(EPOCH FROM bucket_start)::bigint AS ts,
			CAST(COALESCE(open_price, close_price) AS TEXT) AS o,
			CAST(COALESCE(high_price, open_price, close_price) AS TEXT) AS h,
			CAST(COALESCE(low_price, open_price, close_price)  AS TEXT) AS l,
			CAST(close_price AS TEXT) AS c,
			CAST(volume_usd  AS TEXT) AS v
		FROM agg
		WHERE close_price IS NOT NULL
		ORDER BY bucket_start ASC
	`

	rows, err := pool.Query(
		ctx,
		q,
		token0,
		token1,
		address,
		stableAddrs,
		wzilAddr,
		from,
		to,
		interval,
	)
	if err != nil {
		return nil, fmt.Errorf("getPairOHLCVV2 query failed: %w", err)
	}
	defer rows.Close()

	var out []CandleDTO
	for rows.Next() {
		var c CandleDTO
		if err := rows.Scan(
			&c.Timestamp,
			&c.Open,
			&c.High,
			&c.Low,
			&c.Close,
			&c.Volume,
		); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func getPairOHLCVV3(
	ctx context.Context,
	pool *pgxpool.Pool,
	token0, token1 string,
	address string,
	from, to int64,
	interval string,
) ([]CandleDTO, error) {
	wzilAddr, err := getWZILAddress(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get WZIL address: %w", err)
	}
	stableAddrs, err := getStablecoinAddresses(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to get stablecoin addresses: %w", err)
	}

	// V3 OHLCV with proper price calculation matching dex_pools view
	// V3 rate = (sqrt_price_x96^2 / 2^192) / 10^(dec1-dec0) = token0 price in token1 (human units)
	q := `
		WITH meta AS (
			SELECT
				t0.address  AS token0,
				t1.address  AS token1,
				t0.decimals AS dec0,
				t1.decimals AS dec1
			FROM tokens t0, tokens t1
			WHERE t0.address = $1 AND t1.address = $2
		),
		params AS (
			SELECT
				to_timestamp($6) AT TIME ZONE 'UTC' AS from_ts,
				to_timestamp($7) AT TIME ZONE 'UTC' AS to_ts
		),
		buckets AS (
			SELECT
				gs AS bucket_start,
				gs + $8::interval AS bucket_end
			FROM params p,
				 generate_series(
					 p.from_ts,
					 p.to_ts,
					 $8::interval
				 ) AS gs
		),
		swaps AS (
			SELECT
				to_timestamp(s.timestamp) AT TIME ZONE 'UTC' AS ts,
				CASE
					WHEN s.sqrt_price_x96 IS NULL OR s.sqrt_price_x96 = 0 THEN NULL
					-- token0=WZIL, token1=stablecoin → show stablecoin price = (1/rate) * zil_price
					WHEN LOWER($5) = LOWER(m.token0) AND LOWER(m.token1) = ANY($4::text[]) THEN
						(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
						 POWER(10::numeric, m.dec1 - m.dec0), 0)) * z.zil_usd
					-- token0=WZIL → show token1 price = (1/rate) * zil_price
					WHEN LOWER($5) = LOWER(m.token0) THEN
						(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
						 POWER(10::numeric, m.dec1 - m.dec0), 0)) * z.zil_usd
					-- token0=stablecoin, token1=WZIL → show WZIL price
					WHEN LOWER(m.token0) = ANY($4::text[]) AND LOWER($5) = LOWER(m.token1) THEN
						z.zil_usd
					-- token0=stablecoin → show token1 price = (1/rate) * stablecoin_price (assumed ~1)
					WHEN LOWER(m.token0) = ANY($4::text[]) THEN
						(1.0 / NULLIF((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
						 POWER(10::numeric, m.dec1 - m.dec0), 0))
					-- token1=WZIL → show token0 price = rate * zil_price
					WHEN LOWER($5) = LOWER(m.token1) THEN
						((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
						 POWER(10::numeric, m.dec1 - m.dec0)) * z.zil_usd
					-- token1=stablecoin → show token0 price = rate * stablecoin_price (assumed ~1)
					WHEN LOWER(m.token1) = ANY($4::text[]) THEN
						((POWER(s.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
						 POWER(10::numeric, m.dec1 - m.dec0))
					ELSE NULL
				END AS price_usd,
				ABS(s.amount_usd) AS volume_usd
			FROM uniswap_v3_swaps s
			CROSS JOIN meta m
			LEFT JOIN LATERAL (
				SELECT price::numeric AS zil_usd
				FROM prices_zil_usd_minute p
				WHERE p.ts <= to_timestamp(s.timestamp) AT TIME ZONE 'UTC'
				ORDER BY p.ts DESC
				LIMIT 1
			) z ON TRUE
			WHERE s.pool = $3
			  AND s.timestamp <= $7
		),
		agg AS (
			SELECT
				b.bucket_start,
				(
					SELECT price_usd
					FROM swaps s
					WHERE s.ts <= b.bucket_end AND s.price_usd IS NOT NULL
					ORDER BY s.ts DESC
					LIMIT 1
				) AS close_price,
				(
					SELECT price_usd
					FROM swaps s
					WHERE s.ts <= b.bucket_start AND s.price_usd IS NOT NULL
					ORDER BY s.ts DESC
					LIMIT 1
				) AS open_price,
				(
					SELECT MAX(price_usd)
					FROM swaps s
					WHERE s.ts > b.bucket_start
					  AND s.ts <= b.bucket_end
				) AS high_price,
				(
					SELECT MIN(price_usd)
					FROM swaps s
					WHERE s.ts > b.bucket_start
					  AND s.ts <= b.bucket_end
				) AS low_price,
				(
					SELECT COALESCE(SUM(volume_usd), 0)
					FROM swaps s
					WHERE s.ts > b.bucket_start
					  AND s.ts <= b.bucket_end
				) AS volume_usd
			FROM buckets b
		)
		SELECT
			EXTRACT(EPOCH FROM bucket_start)::bigint AS ts,
			CAST(COALESCE(open_price, close_price) AS TEXT) AS o,
			CAST(COALESCE(high_price, open_price, close_price) AS TEXT) AS h,
			CAST(COALESCE(low_price, open_price, close_price)  AS TEXT) AS l,
			CAST(close_price AS TEXT) AS c,
			CAST(volume_usd  AS TEXT) AS v
		FROM agg
		WHERE close_price IS NOT NULL
		ORDER BY bucket_start ASC
	`

	rows, err := pool.Query(
		ctx,
		q,
		token0,
		token1,
		address,
		stableAddrs,
		wzilAddr,
		from,
		to,
		interval,
	)
	if err != nil {
		return nil, fmt.Errorf("getPairOHLCVV3 query failed: %w", err)
	}
	defer rows.Close()

	var out []CandleDTO
	for rows.Next() {
		var c CandleDTO
		if err := rows.Scan(
			&c.Timestamp,
			&c.Open,
			&c.High,
			&c.Low,
			&c.Close,
			&c.Volume,
		); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
