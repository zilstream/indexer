package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DTOs for API responses (lightweight, no ORM tags)
type TokenDTO struct {
	Address        string   `json:"address"`
	Symbol         *string  `json:"symbol,omitempty"`
	Name           *string  `json:"name,omitempty"`
	Decimals       *int32   `json:"decimals,omitempty"`
	PriceUSD       *string  `json:"price_usd,omitempty"`
	MarketCapUSD   *string  `json:"market_cap_usd,omitempty"`
	TotalVolumeUSD *string  `json:"total_volume_usd,omitempty"`
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

// ListTokens queries tokens with optional fuzzy search (symbol/name) ordered by market_cap_usd desc
func ListTokens(ctx context.Context, pool *pgxpool.Pool, limit, offset int, search *string) ([]TokenDTO, error) {
	q := `
		SELECT address, symbol, name, decimals,
		       CAST(price_usd AS TEXT), CAST(market_cap_usd AS TEXT), CAST(total_volume_usd AS TEXT)
		FROM tokens
		WHERE ($3::text IS NULL OR symbol ILIKE '%' || $3 || '%' OR name ILIKE '%' || $3 || '%')
		ORDER BY market_cap_usd DESC NULLS LAST
		LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset, search)
	if err != nil {
		return nil, fmt.Errorf("ListTokens query failed: %w", err)
	}
	defer rows.Close()

	var out []TokenDTO
	for rows.Next() {
		var t TokenDTO
		if err := rows.Scan(&t.Address, &t.Symbol, &t.Name, &t.Decimals, &t.PriceUSD, &t.MarketCapUSD, &t.TotalVolumeUSD); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// ListPairs reads from dex_pools view to be protocol-agnostic (V2+V3)
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
		ORDER BY %s %s NULLS LAST
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
			to_char(COALESCE(SUM(liquidity_usd), 0), 'FM9999999999999999999999999999999999990.999999999999999999') AS total_liquidity,
			to_char(COALESCE(SUM(volume_usd_24h), 0), 'FM9999999999999999999999999999999999990.999999999999999999') AS total_volume_24h,
			to_char(COALESCE(SUM(volume_usd), 0), 'FM9999999999999999999999999999999999990.999999999999999999') AS total_volume_all
		FROM dex_pools
		WHERE liquidity_usd > 0
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
