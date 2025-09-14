package database

import (
	"context"
	"fmt"

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
func ListPairs(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]PairDTO, error) {
	q := `
		SELECT protocol, address, token0, token1,
		       token0_symbol, token0_name, token1_symbol, token1_name,
		       CAST(fee AS TEXT), CAST(reserve0 AS TEXT), CAST(reserve1 AS TEXT), CAST(liquidity AS TEXT),
		       CAST(liquidity_usd AS TEXT), CAST(volume_usd AS TEXT), txn_count
		FROM dex_pools
		WHERE liquidity_usd > 0
		ORDER BY volume_usd DESC NULLS LAST, liquidity_usd DESC NULLS LAST
		LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("ListPairs query failed: %w", err)
	}
	defer rows.Close()

	var out []PairDTO
	for rows.Next() {
		var p PairDTO
		if err := rows.Scan(&p.Protocol, &p.Address, &p.Token0, &p.Token1, &p.Token0Symbol, &p.Token0Name, &p.Token1Symbol, &p.Token1Name, &p.Fee, &p.Reserve0, &p.Reserve1, &p.Liquidity, &p.LiquidityUSD, &p.VolumeUSD, &p.TxnCount); err != nil {
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
