package uniswapv3

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/zilstream/indexer/internal/modules/core"
)

// registerEventHandlers sets up event signature to handler mappings
func (m *UniswapV3Module) registerEventHandlers() error {
	// Map by topic hash using ABI to be robust
	if ev, ok := m.factoryABI.Events["PoolCreated"]; ok {
		m.handlers[ev.ID] = handlePoolCreated
	}
	if ev, ok := m.poolABI.Events["Initialize"]; ok { m.handlers[ev.ID] = handleInitialize }
	if ev, ok := m.poolABI.Events["Swap"]; ok { m.handlers[ev.ID] = handleSwap }
	// Zilliqa Uniswap V3 variant (observed on-chain) uses a different Swap topic
	// Map the known topic to the same handler so swaps are processed
	zilSwapTopic := common.HexToHash("0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83")
	m.handlers[zilSwapTopic] = handleSwap
	if ev, ok := m.poolABI.Events["Mint"]; ok { m.handlers[ev.ID] = handleMint }
	if ev, ok := m.poolABI.Events["Burn"]; ok { m.handlers[ev.ID] = handleBurn }
	if ev, ok := m.poolABI.Events["Collect"]; ok { m.handlers[ev.ID] = handleCollect }
	return nil
}

// handlePoolCreated processes PoolCreated events from the factory
func handlePoolCreated(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	module.logger.Info().Str("event", "PoolCreated").Str("address", event.Address.Hex()).Uint64("block", event.BlockNumber).Msg("Processing PoolCreated")

	// Extract event parameters
	token0, ok := event.Args["token0"].(common.Address)
	if !ok { return fmt.Errorf("invalid token0 address in PoolCreated") }
	token1, ok := event.Args["token1"].(common.Address)
	if !ok { return fmt.Errorf("invalid token1 address in PoolCreated") }
	pool, ok := event.Args["pool"].(common.Address)
	if !ok { return fmt.Errorf("invalid pool address in PoolCreated") }
	// fee is indexed, available in topics[2]

	// The true factory address is the emitter of PoolCreated
	factoryAddr := event.Address

	// Ensure tokens exist in universal token table (insert if not exists)
	if err := module.ensureToken(ctx, token0, event.BlockNumber, event.Timestamp.Int64()); err != nil { return fmt.Errorf("failed to ensure token0: %w", err) }
	if err := module.ensureToken(ctx, token1, event.BlockNumber, event.Timestamp.Int64()); err != nil { return fmt.Errorf("failed to ensure token1: %w", err) }

	// Ensure factory row exists
	_, _ = module.db.Pool().Exec(ctx, `
		INSERT INTO uniswap_v3_factory (address, pool_count, created_at_block, created_at_timestamp)
		VALUES ($1, 0, $2, $3)
		ON CONFLICT (address) DO NOTHING`,
		strings.ToLower(factoryAddr.Hex()), event.BlockNumber, event.Timestamp.Int64(),
	)

	// Insert pool; only increment factory pool_count if this is a new row.
	insertQuery := `
		INSERT INTO uniswap_v3_pools (
			address, factory, token0, token1, fee, created_at_block, created_at_timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO NOTHING`

	fee := "0"
	if v, ok := event.Args["fee"].(*big.Int); ok && v != nil { fee = v.String() }

	tag, err := module.db.Pool().Exec(ctx, insertQuery,
		strings.ToLower(pool.Hex()),
		strings.ToLower(factoryAddr.Hex()),
		strings.ToLower(token0.Hex()),
		strings.ToLower(token1.Hex()),
		fee,
		event.BlockNumber,
		event.Timestamp.Int64(),
	)
	if err != nil { return fmt.Errorf("failed to insert v3 pool: %w", err) }

	if tag.RowsAffected() == 1 {
		// New pool inserted -> increment factory pool_count
		updateFactory := `UPDATE uniswap_v3_factory SET pool_count = pool_count + 1, updated_at = CURRENT_TIMESTAMP WHERE address = $1`
		module.db.Pool().Exec(ctx, updateFactory, strings.ToLower(factoryAddr.Hex()))
	} else {
		// Pool already existed -> ensure metadata is up-to-date
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v3_pools SET
				factory = $2,
				token0 = $3,
				token1 = $4,
				fee = $5,
				updated_at = CURRENT_TIMESTAMP
			WHERE address = $1`,
			strings.ToLower(pool.Hex()),
			strings.ToLower(factoryAddr.Hex()),
			strings.ToLower(token0.Hex()),
			strings.ToLower(token1.Hex()),
			fee,
		)
	}

	// Add pool ABI to parser
	module.parser.AddContract(pool, module.poolABI)

	module.logger.Info().Str("pool", pool.Hex()).Str("token0", token0.Hex()).Str("token1", token1.Hex()).Str("factory", factoryAddr.Hex()).Msg("V3 pool created")
	return nil
}

// handleInitialize records initial sqrtPrice and tick
func handleInitialize(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	// Parse values
	sqrtPrice, _ := event.Args["sqrtPriceX96"].(*big.Int)
	tick, _ := event.Args["tick"].(*big.Int)

	query := `UPDATE uniswap_v3_pools SET sqrt_price_x96 = $2, tick = $3, updated_at = CURRENT_TIMESTAMP WHERE address = $1`
	_, err := module.db.Pool().Exec(ctx, query,
		strings.ToLower(event.Address.Hex()),
		func() string { if sqrtPrice != nil { return sqrtPrice.String() }; return "0" }(),
		func() string { if tick != nil { return tick.String() }; return "0" }(),
	)
	return err
}

// handleSwap processes v3 Swap events
func handleSwap(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	var sender, recipient common.Address
	if len(event.Log.Topics) >= 3 {
		sender = common.BytesToAddress(event.Log.Topics[1].Bytes())
		recipient = common.BytesToAddress(event.Log.Topics[2].Bytes())
	}
	amount0, _ := event.Args["amount0"].(*big.Int)
	amount1, _ := event.Args["amount1"].(*big.Int)
	sqrtPriceX96, _ := event.Args["sqrtPriceX96"].(*big.Int)
	liquidity, _ := event.Args["liquidity"].(*big.Int)
	tick, _ := event.Args["tick"].(*big.Int)
	// If parser couldn't decode (e.g. alt ABI), decode manually from data
	if amount0 == nil || amount1 == nil || sqrtPriceX96 == nil || liquidity == nil || tick == nil {
		data := event.Log.Data
		if len(data) >= 160 { // 5 * 32 bytes
			word := func(i int) []byte { return data[i*32 : (i+1)*32] }
			parseSigned := func(b []byte) *big.Int {
				v := new(big.Int).SetBytes(b)
				// if sign bit set, subtract 2^256 to get negative value
				if b[0]&0x80 != 0 {
					pow := new(big.Int).Lsh(big.NewInt(1), 256)
					v.Sub(v, pow)
				}
				return v
			}
			if amount0 == nil { amount0 = parseSigned(word(0)) }
			if amount1 == nil { amount1 = parseSigned(word(1)) }
			if sqrtPriceX96 == nil { sqrtPriceX96 = new(big.Int).SetBytes(word(2)) }
			if liquidity == nil { liquidity = new(big.Int).SetBytes(word(3)) }
			if tick == nil { tick = parseSigned(word(4)) }
		}
	}

	id := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)
	// Duplicate check for Zilliqa duplicate logs
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v3_swaps
		WHERE transaction_hash = $1
		  AND pool = $2
		  AND sender = $3
		  AND recipient = $4
		  AND amount0 = $5
		  AND amount1 = $6
		  AND sqrt_price_x96 = $7
		  AND liquidity = $8
		  AND tick = $9
		  AND block_number = $10
		  AND id != $11`
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()),
		strings.ToLower(recipient.Hex()),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
		func() string { if sqrtPriceX96 != nil { return sqrtPriceX96.String() }; return "0" }(),
		func() string { if liquidity != nil { return liquidity.String() }; return "0" }(),
		func() string { if tick != nil { return tick.String() }; return "0" }(),
		event.BlockNumber,
		id,
	).Scan(&duplicateCount)
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().Str("swap_id", id).Int("duplicates", duplicateCount).Msg("Skipping duplicate v3 swap event")
		return nil
	}

	query := `
		INSERT INTO uniswap_v3_swaps (
			id, transaction_hash, log_index, block_number, timestamp,
			pool, sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO NOTHING`
	_, err = module.db.Pool().Exec(ctx, query,
		id,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()),
		strings.ToLower(recipient.Hex()),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
		func() string { if sqrtPriceX96 != nil { return sqrtPriceX96.String() }; return "0" }(),
		func() string { if liquidity != nil { return liquidity.String() }; return "0" }(),
		func() string { if tick != nil { return tick.String() }; return "0" }(),
	)
	if err != nil { return err }
	// Increment transaction count for this pool
	_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v3_pools SET txn_count = COALESCE(txn_count,0) + 1, updated_at = CURRENT_TIMESTAMP WHERE address = $1`, strings.ToLower(event.Address.Hex()))
	// Update pool's rolling state: liquidity and prices
	if liquidity != nil || sqrtPriceX96 != nil || tick != nil {
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v3_pools
			SET 
				liquidity = COALESCE($2::numeric, liquidity),
				sqrt_price_x96 = COALESCE($3::numeric, sqrt_price_x96),
				tick = COALESCE($4::numeric, tick),
				updated_at = CURRENT_TIMESTAMP
			WHERE address = $1`,
			strings.ToLower(event.Address.Hex()),
			func() *string { if liquidity != nil { s := liquidity.String(); return &s }; return nil }(),
			func() *string { if sqrtPriceX96 != nil { s := sqrtPriceX96.String(); return &s }; return nil }(),
			func() *string { if tick != nil { s := tick.String(); return &s }; return nil }(),
		)
	}
	// Seed reserves once if empty (start mid-history) using on-chain balances
	module.refreshReservesIfEmpty(ctx, event.Address)
	// Update reserves based on signed deltas from Swap (amount0/amount1 are int256 deltas of pool balances)
	{
		var d0, d1 string
		if amount0 != nil { d0 = amount0.String() } else { d0 = "0" }
		if amount1 != nil { d1 = amount1.String() } else { d1 = "0" }
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v3_pools
			SET 
				reserve0 = GREATEST(COALESCE(reserve0, 0) + $2::numeric, 0),
				reserve1 = GREATEST(COALESCE(reserve1, 0) + $3::numeric, 0),
				updated_at = CURRENT_TIMESTAMP
			WHERE address = $1`,
			strings.ToLower(event.Address.Hex()), d0, d1,
		)
	}
	// Compute amount_usd using stablecoin/ZIL direct paths, then router fallback
	var usd string
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v3_pools WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			l0 := strings.ToLower(t0)
			l1 := strings.ToLower(t1)
			zilAddr := strings.ToLower(module.wethAddress.Hex())
			// Resolve block timestamp via DB, fallback to RPC if DB row not yet visible
			minute, ok := module.blockTime(ctx, event.BlockNumber)
			if ok {
				var usd0, usd1 string
				// Calculate USD for both sides
				if module.isStablecoin(l0) && amount0 != nil {
					dec := module.tokenDecimals(ctx, l0)
					usd0 = divBigIntByPow10Str(absBig(amount0), dec)
				} else if l0 == zilAddr && amount0 != nil {
					if price, ok := module.priceProvider.PriceZILUSD(ctx, minute); ok {
						usd0 = mulStr(divBigIntByPow10Str(absBig(amount0), 18), price)
					}
				} else if module.priceRouter != nil && amount0 != nil {
					if p, ok := module.priceRouter.PriceTokenUSD(ctx, l0, minute); ok {
						dec0 := module.tokenDecimals(ctx, l0)
						usd0 = mulStr(divBigIntByPow10Str(absBig(amount0), dec0), p)
					}
				}
				
				if module.isStablecoin(l1) && amount1 != nil {
					dec := module.tokenDecimals(ctx, l1)
					usd1 = divBigIntByPow10Str(absBig(amount1), dec)
				} else if l1 == zilAddr && amount1 != nil {
					if price, ok := module.priceProvider.PriceZILUSD(ctx, minute); ok {
						usd1 = mulStr(divBigIntByPow10Str(absBig(amount1), 18), price)
					}
				} else if module.priceRouter != nil && amount1 != nil {
					if p, ok := module.priceRouter.PriceTokenUSD(ctx, l1, minute); ok {
						dec1 := module.tokenDecimals(ctx, l1)
						usd1 = mulStr(divBigIntByPow10Str(absBig(amount1), dec1), p)
					}
				}
				
				// Sum both sides if both present, else double the available one
				if usd0 != "" && usd1 != "" {
					r0 := new(big.Rat)
					_, _ = r0.SetString(usd0)
					r1 := new(big.Rat)
					_, _ = r1.SetString(usd1)
					sum := new(big.Rat).Add(r0, r1)
					usd = sum.FloatString(18)
				} else if usd0 != "" {
					usd = mulStr(usd0, "2")
				} else if usd1 != "" {
					usd = mulStr(usd1, "2")
				}
				if usd != "" {
					// Set swap amount_usd
					_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v3_swaps SET amount_usd = $2::numeric WHERE id = $1`, id, usd)
					// Update pool volume_usd and fees_usd
					_, _ = module.db.Pool().Exec(ctx, `
						UPDATE uniswap_v3_pools
						SET 
							volume_usd = COALESCE(volume_usd,0) + $2::numeric,
							fees_usd = COALESCE(fees_usd,0) + ($2::numeric * (COALESCE(fee,0)::numeric / 1000000.0))
						WHERE address = $1`, strings.ToLower(event.Address.Hex()), usd)
					// Increment token-level volumes
					_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET total_volume_usd = COALESCE(total_volume_usd,0) + $2::numeric, updated_at = NOW() WHERE address = $1`, l0, usd)
					_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET total_volume_usd = COALESCE(total_volume_usd,0) + $2::numeric, updated_at = NOW() WHERE address = $1`, l1, usd)
					// Update token prices if router/provider available
					if module.priceRouter != nil {
						if p0, ok := module.priceRouter.PriceTokenUSD(ctx, l0, minute); ok {
							if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = ($2 / $3::numeric),
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, l0, p0, zil)
							} else {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, l0, p0)
							}
						}
						if p1, ok := module.priceRouter.PriceTokenUSD(ctx, l1, minute); ok {
							if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = ($2 / $3::numeric),
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, l1, p1, zil)
							} else {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, l1, p1)
							}
						}
					} else {
						// Ensure ZIL price at least
						if l0 == zilAddr { if p, ok := module.priceProvider.PriceZILUSD(ctx, minute); ok { _, _ = module.db.Pool().Exec(ctx, `
	UPDATE tokens
	SET price_usd = $2,
	    price_eth = 1,
	    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
	    updated_at = NOW()
	WHERE address = $1`, l0, p) } }
						if l1 == zilAddr { if p, ok := module.priceProvider.PriceZILUSD(ctx, minute); ok { _, _ = module.db.Pool().Exec(ctx, `
	UPDATE tokens
	SET price_usd = $2,
	    price_eth = 1,
	    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
	    updated_at = NOW()
	WHERE address = $1`, l1, p) } }
					}
					// Recompute liquidity for both tokens
					recomputeTokenLiquidityUSD(ctx, module, l0)
					recomputeTokenLiquidityUSD(ctx, module, l1)
				}
			}
		}
	}
	return nil
}

// handleMint stores v3 Mint events
func handleMint(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	owner := common.Address{}
	if len(event.Log.Topics) >= 2 { owner = common.BytesToAddress(event.Log.Topics[1].Bytes()) }
	tickLower, _ := event.Args["tickLower"].(*big.Int)
	tickUpper, _ := event.Args["tickUpper"].(*big.Int)
	amount, _ := event.Args["amount"].(*big.Int)
	amount0, _ := event.Args["amount0"].(*big.Int)
	amount1, _ := event.Args["amount1"].(*big.Int)
	id := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)
	// Duplicate check
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v3_mints
		WHERE transaction_hash = $1
		  AND pool = $2
		  AND owner = $3
		  AND tick_lower = $4
		  AND tick_upper = $5
		  AND amount = $6
		  AND amount0 = $7
		  AND amount1 = $8
		  AND block_number = $9
		  AND id != $10`
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount != nil { return amount.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
		event.BlockNumber,
		id,
	).Scan(&duplicateCount)
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().Str("mint_id", id).Int("duplicates", duplicateCount).Msg("Skipping duplicate v3 mint event")
		return nil
	}

	query := `
		INSERT INTO uniswap_v3_mints (
			id, transaction_hash, log_index, block_number, timestamp,
			pool, owner, tick_lower, tick_upper, amount, amount0, amount1
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO NOTHING`
	_, err = module.db.Pool().Exec(ctx, query,
		id,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount != nil { return amount.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
	)
	if err != nil { return err }
	// Update reserves: Mint deposits tokens into the pool (amount0/amount1 are uint256)
	{
		var a0, a1 string
		if amount0 != nil { a0 = amount0.String() } else { a0 = "0" }
		if amount1 != nil { a1 = amount1.String() } else { a1 = "0" }
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v3_pools
			SET 
				reserve0 = GREATEST(COALESCE(reserve0, 0) + $2::numeric, 0),
				reserve1 = GREATEST(COALESCE(reserve1, 0) + $3::numeric, 0),
				updated_at = CURRENT_TIMESTAMP
			WHERE address = $1`, strings.ToLower(event.Address.Hex()), a0, a1)
	}
	// Compute amount_usd (sum of both legs) for mint
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v3_pools WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			l0 := strings.ToLower(t0); l1 := strings.ToLower(t1)
			minute, ok := module.blockTime(ctx, event.BlockNumber)
			if ok {
				total := "0"
				if amount0 != nil && amount0.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l0, minute, absBig(amount0)); ok { total = addStr(total, usd) }
				}
				if amount1 != nil && amount1.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l1, minute, absBig(amount1)); ok { total = addStr(total, usd) }
				}
				if total != "0" {
					_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v3_mints SET amount_usd = $2::numeric WHERE id = $1`, id, total)
				}
				// Update token prices if router/provider available
				if module.priceRouter != nil {
					if p0, ok := module.priceRouter.PriceTokenUSD(ctx, l0, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l0, p0)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l0, zil)
}
	// recompute market cap for token0
	recomputeTokenMarketCapUSD(ctx, module, l0)
}
					if p1, ok := module.priceRouter.PriceTokenUSD(ctx, l1, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l1, p1)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l1, zil)
}
	// recompute market cap for token1
	recomputeTokenMarketCapUSD(ctx, module, l1)
}
				}
				// Recompute liquidity for both tokens
				recomputeTokenLiquidityUSD(ctx, module, l0)
				recomputeTokenLiquidityUSD(ctx, module, l1)
			}
		}
	}
	return nil
}

// handleBurn stores v3 Burn events
func handleBurn(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	owner := common.Address{}
	if len(event.Log.Topics) >= 2 { owner = common.BytesToAddress(event.Log.Topics[1].Bytes()) }
	tickLower, _ := event.Args["tickLower"].(*big.Int)
	tickUpper, _ := event.Args["tickUpper"].(*big.Int)
	amount, _ := event.Args["amount"].(*big.Int)
	amount0, _ := event.Args["amount0"].(*big.Int)
	amount1, _ := event.Args["amount1"].(*big.Int)
	id := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)
	// Duplicate check
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v3_burns
		WHERE transaction_hash = $1
		  AND pool = $2
		  AND owner = $3
		  AND tick_lower = $4
		  AND tick_upper = $5
		  AND amount = $6
		  AND amount0 = $7
		  AND amount1 = $8
		  AND block_number = $9
		  AND id != $10`
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount != nil { return amount.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
		event.BlockNumber,
		id,
	).Scan(&duplicateCount)
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().Str("burn_id", id).Int("duplicates", duplicateCount).Msg("Skipping duplicate v3 burn event")
		return nil
	}

	query := `
		INSERT INTO uniswap_v3_burns (
			id, transaction_hash, log_index, block_number, timestamp,
			pool, owner, tick_lower, tick_upper, amount, amount0, amount1
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO NOTHING`
	_, err = module.db.Pool().Exec(ctx, query,
		id,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount != nil { return amount.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
	)
	if err != nil { return err }
	// Note: Burn does not transfer tokens; balances are adjusted on Collect.
	// No reserve update here.
	// Compute amount_usd (sum of both legs) for burn
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v3_pools WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			l0 := strings.ToLower(t0); l1 := strings.ToLower(t1)
			minute, ok := module.blockTime(ctx, event.BlockNumber)
			if ok {
				total := "0"
				if amount0 != nil && amount0.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l0, minute, absBig(amount0)); ok { total = addStr(total, usd) }
				}
				if amount1 != nil && amount1.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l1, minute, absBig(amount1)); ok { total = addStr(total, usd) }
				}
				if total != "0" {
					_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v3_burns SET amount_usd = $2::numeric WHERE id = $1`, id, total)
				}
				// Update token prices if router/provider available
				if module.priceRouter != nil {
					if p0, ok := module.priceRouter.PriceTokenUSD(ctx, l0, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l0, p0)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l0, zil)
}
	// recompute market cap for token0
	recomputeTokenMarketCapUSD(ctx, module, l0)
}
					if p1, ok := module.priceRouter.PriceTokenUSD(ctx, l1, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l1, p1)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l1, zil)
}
	// recompute market cap for token1
	recomputeTokenMarketCapUSD(ctx, module, l1)
}
				}
				// Recompute liquidity for both tokens
				recomputeTokenLiquidityUSD(ctx, module, l0)
				recomputeTokenLiquidityUSD(ctx, module, l1)
			}
		}
	}
	return nil
}

// handleCollect stores v3 Collect events
func handleCollect(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error {
	owner := common.Address{}
	if len(event.Log.Topics) >= 2 { owner = common.BytesToAddress(event.Log.Topics[1].Bytes()) }
	recipient, _ := event.Args["recipient"].(common.Address)
	tickLower, _ := event.Args["tickLower"].(*big.Int)
	tickUpper, _ := event.Args["tickUpper"].(*big.Int)
	amount0, _ := event.Args["amount0"].(*big.Int)
	amount1, _ := event.Args["amount1"].(*big.Int)
	id := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)

	// Duplicate check similar to Swap/Mint/Burn to handle Zilliqa duplicate logs
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v3_collects
		WHERE transaction_hash = $1
		  AND pool = $2
		  AND owner = $3
		  AND recipient = $4
		  AND tick_lower = $5
		  AND tick_upper = $6
		  AND amount0 = $7
		  AND amount1 = $8
		  AND block_number = $9
		  AND id != $10`
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		strings.ToLower(recipient.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
		event.BlockNumber,
		id,
	).Scan(&duplicateCount)
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().Str("collect_id", id).Int("duplicates", duplicateCount).Msg("Skipping duplicate v3 collect event")
		return nil
	}

	query := `
		INSERT INTO uniswap_v3_collects (
			id, transaction_hash, log_index, block_number, timestamp,
			pool, owner, recipient, tick_lower, tick_upper, amount0, amount1
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (id) DO NOTHING`
	_, err = module.db.Pool().Exec(ctx, query,
		id,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(owner.Hex()),
		strings.ToLower(recipient.Hex()),
		func() string { if tickLower != nil { return tickLower.String() }; return "0" }(),
		func() string { if tickUpper != nil { return tickUpper.String() }; return "0" }(),
		func() string { if amount0 != nil { return amount0.String() }; return "0" }(),
		func() string { if amount1 != nil { return amount1.String() }; return "0" }(),
	)
	if err != nil { return err }
	// Seed reserves once if empty (start mid-history) before subtracting
	module.refreshReservesIfEmpty(ctx, event.Address)
	// Update reserves: Collect withdraws fees from the pool
	{
		var c0, c1 string
		if amount0 != nil { c0 = amount0.String() } else { c0 = "0" }
		if amount1 != nil { c1 = amount1.String() } else { c1 = "0" }
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v3_pools
			SET 
				reserve0 = GREATEST(COALESCE(reserve0, 0) - $2::numeric, 0),
				reserve1 = GREATEST(COALESCE(reserve1, 0) - $3::numeric, 0),
				updated_at = CURRENT_TIMESTAMP
			WHERE address = $1`, strings.ToLower(event.Address.Hex()), c0, c1)
	}
	// Compute amount_usd (sum of both legs) for collect
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v3_pools WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			l0 := strings.ToLower(t0); l1 := strings.ToLower(t1)
			minute, ok := module.blockTime(ctx, event.BlockNumber)
			if ok {
				total := "0"
				if amount0 != nil && amount0.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l0, minute, absBig(amount0)); ok { total = addStr(total, usd) }
				}
				if amount1 != nil && amount1.Sign() != 0 {
					if usd, ok := module.valueTokenAmountUSD(ctx, l1, minute, absBig(amount1)); ok { total = addStr(total, usd) }
				}
				if total != "0" {
					_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v3_collects SET amount_usd = $2::numeric WHERE id = $1`, id, total)
				}
				// Update token prices if router/provider available
				if module.priceRouter != nil {
					if p0, ok := module.priceRouter.PriceTokenUSD(ctx, l0, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l0, p0)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l0, zil)
}
	// recompute market cap for token0
	recomputeTokenMarketCapUSD(ctx, module, l0)
}
					if p1, ok := module.priceRouter.PriceTokenUSD(ctx, l1, minute); ok {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_usd = $2, updated_at = NOW() WHERE address = $1`, l1, p1)
// maintain price_eth = price_usd / ZIL_USD
if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minute); ok2 {
	_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET price_eth = CASE WHEN price_usd IS NULL THEN NULL ELSE (price_usd / $2::numeric) END, updated_at = NOW() WHERE address = $1`, l1, zil)
}
	// recompute market cap for token1
	recomputeTokenMarketCapUSD(ctx, module, l1)
}
				}
				// Recompute liquidity for both tokens
				recomputeTokenLiquidityUSD(ctx, module, l0)
				recomputeTokenLiquidityUSD(ctx, module, l1)
			}
		}
	}
	return nil
}

// ensureToken ensures a token exists in the universal tokens table
func (m *UniswapV3Module) ensureToken(ctx context.Context, tokenAddress common.Address, firstSeenBlock uint64, firstSeenTimestamp int64) error {
	addr := strings.ToLower(tokenAddress.Hex())
	// Check if token already exists
	var exists bool
	q := `SELECT EXISTS(SELECT 1 FROM tokens WHERE address = $1)`
	if err := m.db.Pool().QueryRow(ctx, q, addr).Scan(&exists); err != nil { return err }
	if exists { return nil }

	// Fetch metadata only for new tokens
	meta, err := m.fetchTokenMetadata(ctx, tokenAddress)
	if err != nil {
		// Fallback to defaults on error
		m.logger.Warn().Err(err).Str("token", tokenAddress.Hex()).Msg("V3: metadata fetch failed, using defaults")
		meta = &TokenMetadata{Name: "Unknown", Symbol: "???", Decimals: 18, TotalSupply: big.NewInt(0)}
	}

	insert := `
		INSERT INTO tokens (address, name, symbol, decimals, total_supply, first_seen_block, first_seen_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO NOTHING`
	_, err = m.db.Pool().Exec(ctx, insert,
		addr, meta.Name, meta.Symbol, meta.Decimals, meta.TotalSupply.String(),
		firstSeenBlock, firstSeenTimestamp,
	)
	return err
}

// Pricing helpers and math utilities
// These are local to the module to avoid wider dependencies.
func (m *UniswapV3Module) isStablecoin(addr string) bool {
	if m.stablecoinSet == nil { return false }
	_, ok := m.stablecoinSet[strings.ToLower(addr)]
	return ok
}

func (m *UniswapV3Module) tokenDecimals(ctx context.Context, addr string) int {
	var d sql.NullInt32
	_ = m.db.Pool().QueryRow(ctx, `SELECT decimals FROM tokens WHERE address = $1`, strings.ToLower(addr)).Scan(&d)
	if d.Valid { return int(d.Int32) }
	return 18
}

func (m *UniswapV3Module) valueTokenAmountUSD(ctx context.Context, token string, ts time.Time, amount *big.Int) (string, bool) {
	l := strings.ToLower(token)
	if m.isStablecoin(l) {
		dec := m.tokenDecimals(ctx, l)
		return divBigIntByPow10Str(amount, dec), true
	}
	if l == strings.ToLower(m.wethAddress.Hex()) {
		if price, ok := m.priceProvider.PriceZILUSD(ctx, ts); ok {
			usd := mulStr(divBigIntByPow10Str(amount, 18), price)
			return usd, usd != ""
		}
		return "", false
	}
	if m.priceRouter != nil {
		if p, ok := m.priceRouter.PriceTokenUSD(ctx, l, ts); ok {
			dec := m.tokenDecimals(ctx, l)
			usd := mulStr(divBigIntByPow10Str(amount, dec), p)
			return usd, usd != ""
		}
	}
	return "", false
}

func absBig(x *big.Int) *big.Int { if x == nil { return big.NewInt(0) }; return new(big.Int).Abs(x) }

func divBigIntByPow10Str(x *big.Int, decimals int) string {
	if x == nil { return "0" }
	n := new(big.Rat).SetInt(x)
	d := new(big.Rat).SetFloat64(pow10Float(decimals))
	if d.Sign() == 0 { return "0" }
	n.Quo(n, d)
	return n.FloatString(18)
}

func addStr(a, b string) string {
	x := new(big.Rat); x.SetString(a)
	y := new(big.Rat); y.SetString(b)
	x.Add(x, y)
	return x.FloatString(18)
}

func mulStr(a, b string) string {
	x := new(big.Rat); if _, ok := x.SetString(a); !ok { return "" }
	y := new(big.Rat); if _, ok := y.SetString(b); !ok { return "" }
	x.Mul(x, y)
	return x.FloatString(18)
}

// local pow10Float for decimal scaling
func pow10Float(n int) float64 {
	v := 1.0
	if n > 0 { for i:=0; i<n; i++ { v *= 10 } }
	if n < 0 { for i:=0; i<(-n); i++ { v /= 10 } }
	return v
}

// recomputeTokenLiquidityUSD recalculates tokens.total_liquidity_usd as the sum of the token's
// reserve USD across all Uniswap V2 pairs and Uniswap V3 pools.
func recomputeTokenLiquidityUSD(ctx context.Context, module *UniswapV3Module, addr string) {
	q := `
	WITH v2 AS (
		SELECT COALESCE(SUM(
			CASE 
				WHEN p.token0 = $1 THEN (COALESCE(p.reserve0,0) / POWER(10::numeric, COALESCE(t0.decimals,18))) * COALESCE(t0.price_usd,0)
				WHEN p.token1 = $1 THEN (COALESCE(p.reserve1,0) / POWER(10::numeric, COALESCE(t1.decimals,18))) * COALESCE(t1.price_usd,0)
				ELSE 0
			END
		),0) AS usd
		FROM uniswap_v2_pairs p
		LEFT JOIN tokens t0 ON t0.address = p.token0
		LEFT JOIN tokens t1 ON t1.address = p.token1
		WHERE p.token0 = $1 OR p.token1 = $1
	), v3 AS (
		SELECT COALESCE(SUM(
			CASE 
				WHEN p.token0 = $1 THEN (COALESCE(p.reserve0,0) / POWER(10::numeric, COALESCE(t0.decimals,18))) * COALESCE(t0.price_usd,0)
				WHEN p.token1 = $1 THEN (COALESCE(p.reserve1,0) / POWER(10::numeric, COALESCE(t1.decimals,18))) * COALESCE(t1.price_usd,0)
				ELSE 0
			END
		),0) AS usd
		FROM uniswap_v3_pools p
		LEFT JOIN tokens t0 ON t0.address = p.token0
		LEFT JOIN tokens t1 ON t1.address = p.token1
		WHERE p.token0 = $1 OR p.token1 = $1
	)
	UPDATE tokens t
	SET total_liquidity_usd = COALESCE((SELECT v2.usd FROM v2),0) + COALESCE((SELECT v3.usd FROM v3),0),
		updated_at = NOW()
	WHERE t.address = $1`
	_, _ = module.db.Pool().Exec(ctx, q, strings.ToLower(addr))
}

// recomputeTokenMarketCapUSD updates tokens.market_cap_usd = (total_supply / 10^decimals) * price_usd
// Uses NULL when either total_supply or price_usd is NULL. Decimals default to 18 when NULL.
func recomputeTokenMarketCapUSD(ctx context.Context, module *UniswapV3Module, addr string) {
	q := `
		UPDATE tokens t
		SET market_cap_usd = CASE 
			WHEN t.price_usd IS NULL OR t.total_supply IS NULL THEN NULL
			ELSE ( (t.total_supply / POWER(10::numeric, COALESCE(t.decimals,18))) * t.price_usd )
		END,
		updated_at = NOW()
		WHERE t.address = $1`
	_, _ = module.db.Pool().Exec(ctx, q, strings.ToLower(addr))
}
