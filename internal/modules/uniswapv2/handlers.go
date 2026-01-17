package uniswapv2

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/zilstream/indexer/internal/modules/core"
)

// registerEventHandlers sets up event signature to handler mappings
func (m *UniswapV2Module) registerEventHandlers() error {
	// PairCreated: 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
	pairCreatedSig := common.HexToHash("0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9")
	m.handlers[pairCreatedSig] = handlePairCreated

	// Swap: 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
	swapSig := common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822")
	m.handlers[swapSig] = handleSwap

	// Sync: 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1
	syncSig := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1")
	m.handlers[syncSig] = handleSync

	// Mint: 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f
	mintSig := common.HexToHash("0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f")
	m.handlers[mintSig] = handleMint

	// Burn: 0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496
	burnSig := common.HexToHash("0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496")
	m.handlers[burnSig] = handleBurn

	// Transfer: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
	transferSig := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	m.handlers[transferSig] = handleTransfer

	return nil
}

// handlePairCreated processes PairCreated events from the factory
func handlePairCreated(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {
	module.logger.Info().
		Str("event", "PairCreated").
		Str("address", event.Address.Hex()).
		Uint64("block", event.BlockNumber).
		Msg("Processing PairCreated event")

	// Extract event parameters
	module.logger.Debug().
		Interface("args", event.Args).
		Msg("PairCreated event args")

	token0, ok := event.Args["token0"].(common.Address)
	if !ok {
		module.logger.Error().
			Interface("args", event.Args).
			Msg("Invalid token0 in PairCreated event")
		return fmt.Errorf("invalid token0 address in PairCreated event")
	}

	token1, ok := event.Args["token1"].(common.Address)
	if !ok {
		return fmt.Errorf("invalid token1 address in PairCreated event")
	}

	pair, ok := event.Args["pair"].(common.Address)
	if !ok {
		return fmt.Errorf("invalid pair address in PairCreated event")
	}

	// The pair index might be named "arg3" or "uint256" depending on ABI parsing
	var pairIndex *big.Int
	if val, ok := event.Args["arg3"]; ok {
		switch v := val.(type) {
		case *big.Int:
			pairIndex = v
		case int64:
			pairIndex = big.NewInt(v)
		case float64:
			pairIndex = big.NewInt(int64(v))
		case int:
			pairIndex = big.NewInt(int64(v))
		default:
			return fmt.Errorf("invalid pair index type in PairCreated event: %T", val)
		}
	} else if val, ok := event.Args["uint256"]; ok {
		pairIndex, ok = val.(*big.Int)
		if !ok {
			return fmt.Errorf("invalid pair index in PairCreated event")
		}
	} else {
		// Fallback: just use 0 if we can't find the index
		module.logger.Warn().Msg("Could not find pair index in PairCreated event, using 0")
		pairIndex = big.NewInt(0)
	}

	// Ensure tokens exist in the universal tokens table
	// Price updates (including stablecoins) are handled via ZIL-anchored routing
	if err := module.ensureToken(ctx, token0, event.BlockNumber, event.Timestamp.Int64()); err != nil {
		return fmt.Errorf("failed to ensure token0: %w", err)
	}

	if err := module.ensureToken(ctx, token1, event.BlockNumber, event.Timestamp.Int64()); err != nil {
		return fmt.Errorf("failed to ensure token1: %w", err)
	}

	// Create or update the pair
	query := `
		INSERT INTO uniswap_v2_pairs (
			address, factory, token0, token1, 
			created_at_block, created_at_timestamp
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (address) DO UPDATE SET
			factory = EXCLUDED.factory,
			token0 = EXCLUDED.token0,
			token1 = EXCLUDED.token1,
			updated_at = CURRENT_TIMESTAMP`

	_, err := module.db.Pool().Exec(ctx, query,
		strings.ToLower(pair.Hex()),
		strings.ToLower(module.factoryAddress.Hex()),
		strings.ToLower(token0.Hex()),
		strings.ToLower(token1.Hex()),
		event.BlockNumber,
		event.Timestamp.Int64(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert pair: %w", err)
	}

	// Update factory pair count
	if err := module.updateFactoryPairCount(ctx, pairIndex.Uint64()); err != nil {
		return fmt.Errorf("failed to update factory pair count: %w", err)
	}

	// Add the pair ABI to the parser for future event parsing
	module.parser.AddContract(pair, module.pairABI)

	module.logger.Info().
		Str("pair", pair.Hex()).
		Str("token0", token0.Hex()).
		Str("token1", token1.Hex()).
		Uint64("pair_index", pairIndex.Uint64()).
		Msg("Pair created")

	return nil
}

// handleSwap processes Swap events from pairs
func handleSwap(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {
	// For Swap events:
	// topics[0] = event signature
	// topics[1] = indexed sender address
	// topics[2] = indexed to address
	// data contains: amount0In, amount1In, amount0Out, amount1Out

	// Extract indexed parameters from topics
	var sender, to common.Address
	if len(event.Log.Topics) >= 3 {
		sender = common.BytesToAddress(event.Log.Topics[1].Bytes())
		to = common.BytesToAddress(event.Log.Topics[2].Bytes())
	} else {
		return fmt.Errorf("insufficient topics for Swap event")
	}

	// Parse amounts from data field
	// The data contains 4 uint256 values (32 bytes each):
	// amount0In, amount1In, amount0Out, amount1Out
	if len(event.Log.Data) < 128 { // 4 * 32 bytes
		return fmt.Errorf("insufficient data for Swap event")
	}

	amount0In := new(big.Int).SetBytes(event.Log.Data[0:32])
	amount1In := new(big.Int).SetBytes(event.Log.Data[32:64])
	amount0Out := new(big.Int).SetBytes(event.Log.Data[64:96])
	amount1Out := new(big.Int).SetBytes(event.Log.Data[96:128])

	// Create swap record ID: txhash-logindex
	swapID := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)

	// Check for duplicate by looking for same transaction hash and values
	// This handles Zilliqa's duplicate event issue
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v2_swaps 
		WHERE transaction_hash = $1
		  AND pair = $2 
		  AND sender = $3 
		  AND recipient = $4
		  AND amount0_in = $5 
		  AND amount1_in = $6
		  AND amount0_out = $7 
		  AND amount1_out = $8
		  AND block_number = $9
		  AND id != $10`

	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()),
		strings.ToLower(to.Hex()),
		amount0In.String(),
		amount1In.String(),
		amount0Out.String(),
		amount1Out.String(),
		event.BlockNumber,
		swapID,
	).Scan(&duplicateCount)

	if err == nil && duplicateCount > 0 {
		module.logger.Debug().
			Str("swap_id", swapID).
			Int("duplicates", duplicateCount).
			Msg("Skipping duplicate swap event")
		return nil
	}

	pairAddr := strings.ToLower(event.Address.Hex())

	// Insert swap record
	query := `
		INSERT INTO uniswap_v2_swaps (
			id, transaction_hash, log_index, block_number, block_hash, timestamp,
			pair, sender, recipient, amount0_in, amount1_in, amount0_out, amount1_out
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (id) DO UPDATE SET
			amount0_in = EXCLUDED.amount0_in,
			amount1_in = EXCLUDED.amount1_in,
			amount0_out = EXCLUDED.amount0_out,
			amount1_out = EXCLUDED.amount1_out`

	_, err = module.db.Pool().Exec(ctx, query,
		swapID,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.BlockHash.Hex(),
		event.Timestamp.Int64(),
		pairAddr, // pair address
		strings.ToLower(sender.Hex()),
		strings.ToLower(to.Hex()),
		amount0In.String(),
		amount1In.String(),
		amount0Out.String(),
		amount1Out.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert swap: %w", err)
	}

	// Update pair token volumes (raw units) and transaction count
	// For each token side, add the absolute net amount swapped: abs(out - in)
	pairAddrSwap := strings.ToLower(event.Address.Hex())
	module.logger.Info().
		Str("pair", pairAddrSwap).
		Str("amount0In", amount0In.String()).
		Str("amount0Out", amount0Out.String()).
		Str("amount1In", amount1In.String()).
		Str("amount1Out", amount1Out.String()).
		Msg("Updating pair volumes from Swap event")

	resultSwap, err := module.db.Pool().Exec(ctx, `
		UPDATE uniswap_v2_pairs
		SET
			volume_token0 = COALESCE(volume_token0,0) + ABS(($2::numeric) - ($3::numeric)),
			volume_token1 = COALESCE(volume_token1,0) + ABS(($4::numeric) - ($5::numeric)),
			txn_count = COALESCE(txn_count,0) + 1,
			updated_at = NOW()
		WHERE address = $1`,
		pairAddrSwap,
		amount0Out.String(), amount0In.String(),
		amount1Out.String(), amount1In.String(),
	)

	if err != nil {
		module.logger.Error().
			Err(err).
			Str("pair", pairAddrSwap).
			Msg("Failed to update pair volumes and txn_count")
	} else {
		module.logger.Info().
			Str("pair", pairAddrSwap).
			Int64("rows_affected", resultSwap.RowsAffected()).
			Msg("Pair volumes update result")
	}

	// Compute USD notional for swaps when pricing is available
	var swapUSD string
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			zilAddr := strings.ToLower(module.wethAddress.Hex())
			var ts int64
			_ = module.db.Pool().QueryRow(ctx, `SELECT timestamp FROM blocks WHERE number = $1`, event.BlockNumber).Scan(&ts)
			if ts == 0 {
				ts = module.getBlockTimestamp(ctx, event.BlockNumber)
			}
			if ts > 0 {
				minute := time.Unix(ts, 0).UTC()
				// Fast path: ZIL pair using direct ZIL price
				var zilDelta *big.Int
				if strings.ToLower(t0) == zilAddr {
					zilDelta = new(big.Int).Sub(amount0Out, amount0In)
				} else if strings.ToLower(t1) == zilAddr {
					zilDelta = new(big.Int).Sub(amount1Out, amount1In)
				}
				if zilDelta != nil && zilDelta.Sign() != 0 {
					if price, ok := module.priceProvider.PriceZILUSD(ctx, minute); ok {
						// amount_usd = abs(zilDelta)/1e18 * price * 2 (both sides)
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE uniswap_v2_swaps
							SET amount_usd = (abs($2::numeric) / 1e18::numeric) * $3::numeric * 2
							WHERE id = $1`, swapID, zilDelta.String(), price)
						// capture value for token updates
						swapUSD = ""
						_ = module.db.Pool().QueryRow(ctx, `SELECT amount_usd FROM uniswap_v2_swaps WHERE id = $1`, swapID).Scan(&swapUSD)
						// increment pair volume_usd by this swap's usd
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE uniswap_v2_pairs
							SET volume_usd = COALESCE(volume_usd,0) + COALESCE((SELECT amount_usd FROM uniswap_v2_swaps WHERE id = $1),0)
							WHERE address = $2`, swapID, strings.ToLower(event.Address.Hex()))
						// record ZIL token price snapshot (price_eth=1 + market cap)
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE tokens
							SET price_usd = $2,
							    price_eth = 1,
							    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
							    updated_at = NOW()
							WHERE address = $1`, zilAddr, price)
					}
				} else if module.priceRouter != nil {
					// General case via router: use the traded side USD
					dec0 := module.tokenDecimals(ctx, t0)
					dec1 := module.tokenDecimals(ctx, t1)
					vol0 := new(big.Int).Set(amount0In)
					if vol0.Sign() == 0 {
						vol0.Set(amount0Out)
					}
					vol1 := new(big.Int).Set(amount1In)
					if vol1.Sign() == 0 {
						vol1.Set(amount1Out)
					}
					usd0 := ""
					usd1 := ""
					if p0, ok := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t0), minute); ok {
						usd0 = mulStr(divBigIntByPow10Str(vol0, dec0), p0)
						module.logger.Debug().
							Str("token", t0).
							Str("price", p0).
							Str("volume_usd", usd0).
							Msg("Token0 USD price from router")
					} else {
						module.logger.Debug().
							Str("token", t0).
							Msg("No USD price for token0 from router")
					}
					if p1, ok := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t1), minute); ok {
						usd1 = mulStr(divBigIntByPow10Str(vol1, dec1), p1)
						module.logger.Debug().
							Str("token", t1).
							Str("price", p1).
							Str("volume_usd", usd1).
							Msg("Token1 USD price from router")
					} else {
						module.logger.Debug().
							Str("token", t1).
							Msg("No USD price for token1 from router")
					}
					// sum both sides if both present
					sel := ""
					if usd0 != "" && usd1 != "" {
						r0 := new(big.Rat)
						_, _ = r0.SetString(usd0)
						r1 := new(big.Rat)
						_, _ = r1.SetString(usd1)
						sum := new(big.Rat).Add(r0, r1)
						sel = sum.FloatString(18)
					} else if usd0 != "" {
						sel = mulStr(usd0, "2")
					} else if usd1 != "" {
						sel = mulStr(usd1, "2")
					}
					if sel != "" {
						swapUSD = sel
						_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v2_swaps SET amount_usd = $2::numeric WHERE id = $1`, swapID, sel)
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE uniswap_v2_pairs
							SET volume_usd = COALESCE(volume_usd,0) + $2::numeric
							WHERE address = $1`, strings.ToLower(event.Address.Hex()), sel)
					} else {
						// Final fallback: use tokens.price_usd for either side if available
						var p0db, p1db string
						_ = module.db.Pool().QueryRow(ctx, `SELECT COALESCE(t0.price_usd::text,''), COALESCE(t1.price_usd::text,'') FROM tokens t0, tokens t1 WHERE t0.address = $1 AND t1.address = $2`, strings.ToLower(t0), strings.ToLower(t1)).Scan(&p0db, &p1db)
						usd0 := ""
						usd1 := ""
						if p0db != "" {
							usd0 = mulStr(divBigIntByPow10Str(vol0, dec0), p0db)
						}
						if p1db != "" {
							usd1 = mulStr(divBigIntByPow10Str(vol1, dec1), p1db)
						}
						if usd0 != "" || usd1 != "" {
							sel2 := ""
							if usd0 != "" && usd1 != "" {
								r0 := new(big.Rat)
								_, _ = r0.SetString(usd0)
								r1 := new(big.Rat)
								_, _ = r1.SetString(usd1)
								sum := new(big.Rat).Add(r0, r1)
								sel2 = sum.FloatString(18)
							} else if usd0 != "" {
								sel2 = mulStr(usd0, "2")
							} else {
								sel2 = mulStr(usd1, "2")
							}
							swapUSD = sel2
							_, _ = module.db.Pool().Exec(ctx, `UPDATE uniswap_v2_swaps SET amount_usd = $2::numeric WHERE id = $1`, swapID, sel2)
							_, _ = module.db.Pool().Exec(ctx, `
								UPDATE uniswap_v2_pairs
								SET volume_usd = COALESCE(volume_usd,0) + $2::numeric
								WHERE address = $1`, strings.ToLower(event.Address.Hex()), sel2)
						}
					}
				}
				// Update token-level volume if we have USD
				if swapUSD != "" {
					_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET total_volume_usd = COALESCE(total_volume_usd,0) + $2::numeric, updated_at = NOW() WHERE address = $1`, strings.ToLower(t0), swapUSD)
					_, _ = module.db.Pool().Exec(ctx, `UPDATE tokens SET total_volume_usd = COALESCE(total_volume_usd,0) + $2::numeric, updated_at = NOW() WHERE address = $1`, strings.ToLower(t1), swapUSD)
				}
				// Recompute token liquidity for both sides using V2+V3 aggregates
				recomputeTokenLiquidityUSD(ctx, module, strings.ToLower(t0))
				recomputeTokenLiquidityUSD(ctx, module, strings.ToLower(t1))
			}
		}
	}

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("sender", sender.Hex()).
		Str("swap_id", swapID).
		Msg("Swap processed")

	// Publish event if publisher is available
	if module.publisher != nil {
		payload := map[string]interface{}{
			"id":               swapID,
			"transaction_hash": event.TransactionHash.Hex(),
			"log_index":        event.LogIndex,
			"block_number":     event.BlockNumber,
			"timestamp":        event.Timestamp.Int64(),
			"address":          pairAddr,
			"sender":           strings.ToLower(sender.Hex()),
			"recipient":        strings.ToLower(to.Hex()),
			"amount0_in":       amount0In.String(),
			"amount1_in":       amount1In.String(),
			"amount0_out":      amount0Out.String(),
			"amount1_out":      amount1Out.String(),
			"amount_usd":       swapUSD,
			"protocol":         "uniswap_v2",
		}
		// Add token_in/token_out for clarity on trade direction
		var token0, token1, sym0, sym1 string
		var dec0, dec1 int32
		err := module.db.Pool().QueryRow(ctx, `
			SELECT p.token0, p.token1, COALESCE(t0.symbol,''), COALESCE(t0.decimals,18), COALESCE(t1.symbol,''), COALESCE(t1.decimals,18)
			FROM uniswap_v2_pairs p
			LEFT JOIN tokens t0 ON t0.address = p.token0
			LEFT JOIN tokens t1 ON t1.address = p.token1
			WHERE p.address = $1`, pairAddr).Scan(&token0, &token1, &sym0, &dec0, &sym1, &dec1)
		if err == nil {
			if amount0In.Sign() > 0 {
				payload["token_in_address"] = token0
				payload["token_in_symbol"] = sym0
				payload["token_in_decimals"] = dec0
			} else if amount1In.Sign() > 0 {
				payload["token_in_address"] = token1
				payload["token_in_symbol"] = sym1
				payload["token_in_decimals"] = dec1
			}
			if amount0Out.Sign() > 0 {
				payload["token_out_address"] = token0
				payload["token_out_symbol"] = sym0
				payload["token_out_decimals"] = dec0
			} else if amount1Out.Sign() > 0 {
				payload["token_out_address"] = token1
				payload["token_out_symbol"] = sym1
				payload["token_out_decimals"] = dec1
			}
		}
		module.publisher.PublishEvent(pairAddr, "swap", payload)
	}

	return nil
}

// handleSync processes Sync events from pairs (reserve updates)
func handleSync(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {
	// For Sync events:
	// topics[0] = event signature
	// data contains: reserve0 (uint112), reserve1 (uint112)

	// Parse reserves from data field
	if len(event.Log.Data) < 64 { // Need at least 2 * 32 bytes
		return fmt.Errorf("insufficient data for Sync event")
	}

	reserve0 := new(big.Int).SetBytes(event.Log.Data[0:32])
	reserve1 := new(big.Int).SetBytes(event.Log.Data[32:64])

	// Create sync record ID
	syncID := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)

	pairAddr := strings.ToLower(event.Address.Hex())

	// Insert sync record
	query := `
		INSERT INTO uniswap_v2_syncs (
			id, transaction_hash, log_index, block_number, timestamp,
			pair, reserve0, reserve1
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			reserve0 = EXCLUDED.reserve0,
			reserve1 = EXCLUDED.reserve1`

	_, err := module.db.Pool().Exec(ctx, query,
		syncID,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		pairAddr, // pair address
		reserve0.String(),
		reserve1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert sync: %w", err)
	}

	// Update pair reserves
	updatePairQuery := `
	UPDATE uniswap_v2_pairs
	SET reserve0 = $2::numeric, reserve1 = $3::numeric, updated_at = CURRENT_TIMESTAMP
	WHERE address = $1`

	module.logger.Info().
		Str("pair", pairAddr).
		Str("reserve0", reserve0.String()).
		Str("reserve1", reserve1.String()).
		Msg("Updating pair reserves from Sync event")

	resultSync, err := module.db.Pool().Exec(ctx, updatePairQuery,
		pairAddr,
		reserve0.String(),
		reserve1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to update pair reserves: %w", err)
	}

	module.logger.Info().
		Str("pair", pairAddr).
		Int64("rows_affected", resultSync.RowsAffected()).
		Msg("Pair reserves update result")

	// Update reserve_usd and token prices, then recompute token liquidity
	if module.priceProvider != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			zilAddr := strings.ToLower(module.wethAddress.Hex())
			var ts int64
			_ = module.db.Pool().QueryRow(ctx, `SELECT timestamp FROM blocks WHERE number = $1`, event.BlockNumber).Scan(&ts)
			if ts == 0 {
				ts = module.getBlockTimestamp(ctx, event.BlockNumber)
			}
			if ts > 0 {
				minTime := time.Unix(ts, 0).UTC()
				// ZIL pair: update reserve_usd and refresh token prices via ZIL-anchored router
				if strings.ToLower(t0) == zilAddr {
					if zilPrice, ok := module.priceProvider.PriceZILUSD(ctx, minTime); ok {
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE uniswap_v2_pairs
							SET reserve_usd = ( ($2::numeric) / 1e18::numeric ) * 2 * $3::numeric
							WHERE address = $1`, strings.ToLower(event.Address.Hex()), reserve0.String(), zilPrice)
						// update ZIL token price (price_eth=1 + market cap)
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE tokens
							SET price_usd = $2,
							    price_eth = 1,
							    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
							    updated_at = NOW()
							WHERE address = $1`, zilAddr, zilPrice)
						// Update the non-ZIL token (t1) price using the ZIL-anchored router
						if module.priceRouter != nil {
							if tokenPriceUSD, ok := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t1), minTime); ok && tokenPriceUSD != "" {
								priceEth := ""
								if zilPrice != "" {
									priceEth = divStr(tokenPriceUSD, zilPrice)
								}
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = CASE WHEN $3 = '' THEN NULL ELSE $3::numeric END,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t1), tokenPriceUSD, priceEth)
							}
						} else if reserve1.Sign() > 0 {
							// Fallback: derive from this pool's reserves
							dec0 := module.tokenDecimals(ctx, t0)
							dec1 := module.tokenDecimals(ctx, t1)
							tokenPriceInZil := divBigIntByPow10Str(reserve0, dec0)
							tokenPriceInZil = divStr(tokenPriceInZil, divBigIntByPow10Str(reserve1, dec1))
							if tokenPriceInZil != "" {
								tokenPriceUSD := mulStr(tokenPriceInZil, zilPrice)
								if tokenPriceUSD != "" {
									_, _ = module.db.Pool().Exec(ctx, `
										UPDATE tokens
										SET price_usd = $2,
										    price_eth = $3,
										    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
										    updated_at = NOW()
										WHERE address = $1`, strings.ToLower(t1), tokenPriceUSD, tokenPriceInZil)
								}
							}
						}
					}
				} else if strings.ToLower(t1) == zilAddr {
					if zilPrice, ok := module.priceProvider.PriceZILUSD(ctx, minTime); ok {
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE uniswap_v2_pairs
							SET reserve_usd = ( ($2::numeric) / 1e18::numeric ) * 2 * $3::numeric
							WHERE address = $1`, strings.ToLower(event.Address.Hex()), reserve1.String(), zilPrice)
						// update ZIL token price (price_eth=1 + market cap)
						_, _ = module.db.Pool().Exec(ctx, `
							UPDATE tokens
							SET price_usd = $2,
							    price_eth = 1,
							    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
							    updated_at = NOW()
							WHERE address = $1`, zilAddr, zilPrice)
						// Update the non-ZIL token (t0) price using the ZIL-anchored router
						if module.priceRouter != nil {
							if tokenPriceUSD, ok := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t0), minTime); ok && tokenPriceUSD != "" {
								priceEth := ""
								if zilPrice != "" {
									priceEth = divStr(tokenPriceUSD, zilPrice)
								}
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = CASE WHEN $3 = '' THEN NULL ELSE $3::numeric END,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t0), tokenPriceUSD, priceEth)
							}
						} else if reserve0.Sign() > 0 {
							// Fallback: derive from this pool's reserves
							dec0 := module.tokenDecimals(ctx, t0)
							dec1 := module.tokenDecimals(ctx, t1)
							tokenPriceInZil := divBigIntByPow10Str(reserve1, dec1)
							tokenPriceInZil = divStr(tokenPriceInZil, divBigIntByPow10Str(reserve0, dec0))
							if tokenPriceInZil != "" {
								tokenPriceUSD := mulStr(tokenPriceInZil, zilPrice)
								if tokenPriceUSD != "" {
									_, _ = module.db.Pool().Exec(ctx, `
										UPDATE tokens
										SET price_usd = $2,
										    price_eth = $3,
										    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
										    updated_at = NOW()
										WHERE address = $1`, strings.ToLower(t0), tokenPriceUSD, tokenPriceInZil)
								}
							}
						}
					}
				} else if module.priceRouter != nil {
					// General case: sum(reserve0*price0 + reserve1*price1) in USD
					p0, ok0 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t0), minTime)
					p1, ok1 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t1), minTime)
					if ok0 || ok1 {
						dec0 := module.tokenDecimals(ctx, t0)
						dec1 := module.tokenDecimals(ctx, t1)
						usd0 := "0"
						usd1 := "0"
						if ok0 {
							usd0 = mulStr(divBigIntByPow10Str(reserve0, dec0), p0)
							if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minTime); ok2 {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = ($2 / $3::numeric),
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t0), p0, zil)
							} else {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t0), p0)
							}
						}
						if ok1 {
							usd1 = mulStr(divBigIntByPow10Str(reserve1, dec1), p1)
							if zil, ok2 := module.priceProvider.PriceZILUSD(ctx, minTime); ok2 {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    price_eth = ($2 / $3::numeric),
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t1), p1, zil)
							} else {
								_, _ = module.db.Pool().Exec(ctx, `
									UPDATE tokens
									SET price_usd = $2,
									    market_cap_usd = CASE WHEN $2 IS NULL OR total_supply IS NULL THEN NULL ELSE ((total_supply / POWER(10::numeric, COALESCE(decimals,18))) * $2) END,
									    updated_at = NOW()
									WHERE address = $1`, strings.ToLower(t1), p1)
							}
						}
						total := addStr(usd0, usd1)
						if total != "0" {
							_, _ = module.db.Pool().Exec(ctx, `
								UPDATE uniswap_v2_pairs
								SET reserve_usd = $2::numeric
								WHERE address = $1`, strings.ToLower(event.Address.Hex()), total)
						}
					}
				}
				// Recompute token liquidity for both sides using V2+V3 aggregates
				recomputeTokenLiquidityUSD(ctx, module, strings.ToLower(t0))
				recomputeTokenLiquidityUSD(ctx, module, strings.ToLower(t1))
			}
		}
	}

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("reserve0", reserve0.String()).
		Str("reserve1", reserve1.String()).
		Msg("Sync processed")

	return nil
}

// math helpers (local)
func divBigIntByPow10Str(x *big.Int, decimals int) string {
	if x == nil {
		return "0"
	}
	n := new(big.Rat).SetInt(x)
	d := new(big.Rat).SetFloat64(pow10Float(decimals))
	if d.Sign() == 0 {
		return "0"
	}
	n.Quo(n, d)
	return n.FloatString(18)
}

func addStr(a, b string) string {
	x := new(big.Rat)
	x.SetString(a)
	y := new(big.Rat)
	y.SetString(b)
	x.Add(x, y)
	return x.FloatString(18)
}

func mulStr(a, b string) string {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok {
		return ""
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return ""
	}
	x.Mul(x, y)
	return x.FloatString(18)
}

func divStr(a, b string) string {
	x := new(big.Rat)
	if _, ok := x.SetString(a); !ok {
		return ""
	}
	y := new(big.Rat)
	if _, ok := y.SetString(b); !ok {
		return ""
	}
	if y.Sign() == 0 {
		return ""
	}
	x.Quo(x, y)
	return x.FloatString(18)
}

func pow10Float(n int) float64 {
	v := 1.0
	if n > 0 {
		for i := 0; i < n; i++ {
			v *= 10
		}
	}
	if n < 0 {
		for i := 0; i < (-n); i++ {
			v /= 10
		}
	}
	return v
}

// handleMint processes Mint events (liquidity additions)
func handleMint(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {

	// For Mint events:
	// topics[0] = event signature
	// topics[1] = indexed sender address
	// data contains: amount0, amount1

	// Debug log the raw event data
	module.logger.Debug().
		Int("topics_count", len(event.Log.Topics)).
		Str("tx_hash", event.TransactionHash.Hex()).
		Uint64("block", event.BlockNumber).
		Str("address", event.Address.Hex()).
		Interface("topics", event.Log.Topics).
		Int("data_len", len(event.Log.Data)).
		Hex("data", event.Log.Data).
		Interface("parsed_args", event.Args).
		Msg("Processing Mint event raw data")

	// Extract sender - some contracts have it indexed, others don't
	var sender common.Address
	if len(event.Log.Topics) >= 2 {
		// Standard: sender is indexed (second topic)
		sender = common.BytesToAddress(event.Log.Topics[1].Bytes())
	} else if len(event.Log.Data) >= 96 {
		// Non-standard: sender is in data field (first 32 bytes)
		// This happens with some non-standard Uniswap V2 implementations
		sender = common.BytesToAddress(event.Log.Data[0:32])
		module.logger.Debug().
			Str("tx_hash", event.TransactionHash.Hex()).
			Str("sender", sender.Hex()).
			Msg("Extracted sender from data field (non-standard Mint event)")
	} else {
		// Fallback: use zero address
		module.logger.Warn().
			Int("topic_count", len(event.Log.Topics)).
			Int("data_len", len(event.Log.Data)).
			Str("tx_hash", event.TransactionHash.Hex()).
			Uint64("block", event.BlockNumber).
			Str("address", event.Address.Hex()).
			Msg("Cannot extract sender from Mint event, using zero address")
		sender = common.Address{}
	}

	// Parse amounts from data field
	// Standard: amount0 and amount1 are at positions 0 and 32
	// Non-standard: amount0 and amount1 are at positions 32 and 64 (after sender)
	var amount0, amount1 *big.Int
	if len(event.Log.Topics) >= 2 {
		// Standard format: amounts start at position 0
		if len(event.Log.Data) < 64 {
			return fmt.Errorf("insufficient data for standard Mint event")
		}
		amount0 = new(big.Int).SetBytes(event.Log.Data[0:32])
		amount1 = new(big.Int).SetBytes(event.Log.Data[32:64])
	} else {
		// Non-standard format: amounts start at position 32 (after sender)
		if len(event.Log.Data) < 96 {
			return fmt.Errorf("insufficient data for non-standard Mint event")
		}
		amount0 = new(big.Int).SetBytes(event.Log.Data[32:64])
		amount1 = new(big.Int).SetBytes(event.Log.Data[64:96])
	}

	// Try to complete a pending mint from the same tx/pair
	// Fetch block timestamp for USD valuation if needed
	ts := module.getBlockTimestamp(ctx, event.BlockNumber)

	// Optional USD
	var amountUSD string
	if module.priceRouter != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			minTime := time.Unix(ts, 0).UTC()
			p0, ok0 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t0), minTime)
			p1, ok1 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t1), minTime)
			if ok0 || ok1 {
				dec0 := module.tokenDecimals(ctx, t0)
				dec1 := module.tokenDecimals(ctx, t1)
				usd0 := "0"
				usd1 := "0"
				if ok0 {
					usd0 = mulStr(divBigIntByPow10Str(amount0, dec0), p0)
				}
				if ok1 {
					usd1 = mulStr(divBigIntByPow10Str(amount1, dec1), p1)
				}
				amountUSD = addStr(usd0, usd1)
			}
		}
	}

	// Early duplicate guard: if a mint already exists with same tx/pair/sender and amounts, skip
	var exists int
	_ = module.db.Pool().QueryRow(ctx, `
	SELECT COUNT(*) FROM uniswap_v2_mints
	WHERE transaction_hash=$1 AND pair=$2 AND sender=$3 AND amount0=$4 AND amount1=$5`,
		event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()), strings.ToLower(sender.Hex()), amount0.String(), amount1.String(),
	).Scan(&exists)
	if exists > 0 {
		module.logger.Debug().Str("pair", event.Address.Hex()).Msg("Duplicate Mint event skipped")
		return nil
	}

	// Complete pending row targeted at non-zero to_address only
	ct, err := module.db.Pool().Exec(ctx, `
	UPDATE uniswap_v2_mints m
	SET sender = $3, amount0 = $4, amount1 = $5,
	    amount_usd = CASE WHEN $6 = '' THEN amount_usd ELSE $6::numeric END
	WHERE m.ctid IN (
	 SELECT ctid FROM uniswap_v2_mints
	 WHERE transaction_hash = $1 AND pair = $2 AND amount0 = 0 AND amount1 = 0 AND to_address <> $7
	 ORDER BY log_index ASC
	 LIMIT 1
	 )`,
		event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()), amount0.String(), amount1.String(), amountUSD, strings.ToLower(common.Address{}.Hex()),
	)
	if err != nil {
		module.logger.Error().Err(err).Str("pair", event.Address.Hex()).Msg("Mint update failed")
	}
	if ct.RowsAffected() == 0 {
		// No pending mint to complete; skip to avoid duplicate rows
		module.logger.Debug().Str("pair", event.Address.Hex()).Msg("No pending mint to complete; skipping")
		return nil
	}

	module.logger.Debug().Str("pair", event.Address.Hex()).Str("sender", sender.Hex()).Msg("Mint processed")
	return nil
}

// handleBurn processes Burn events (liquidity removals)
func handleBurn(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {

	// For Burn events:
	// topics[0] = event signature
	// topics[1] = indexed sender address
	// topics[2] = indexed to address
	// data contains: amount0, amount1

	// Extract indexed parameters from topics
	var sender, to common.Address
	if len(event.Log.Topics) >= 3 {
		sender = common.BytesToAddress(event.Log.Topics[1].Bytes())
		to = common.BytesToAddress(event.Log.Topics[2].Bytes())
	} else {
		return fmt.Errorf("insufficient topics for Burn event")
	}

	// Parse amounts from data field
	if len(event.Log.Data) < 64 { // Need at least 2 * 32 bytes
		return fmt.Errorf("insufficient data for Burn event")
	}

	amount0 := new(big.Int).SetBytes(event.Log.Data[0:32])
	amount1 := new(big.Int).SetBytes(event.Log.Data[32:64])

	// Complete a pending burn from the same tx/pair using token amounts
	ts := module.getBlockTimestamp(ctx, event.BlockNumber)

	// Optional USD
	var amountUSD string
	if module.priceRouter != nil {
		var t0, t1 string
		if err := module.db.Pool().QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`, strings.ToLower(event.Address.Hex())).Scan(&t0, &t1); err == nil {
			minTime := time.Unix(ts, 0).UTC()
			p0, ok0 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t0), minTime)
			p1, ok1 := module.priceRouter.PriceTokenUSD(ctx, strings.ToLower(t1), minTime)
			if ok0 || ok1 {
				dec0 := module.tokenDecimals(ctx, t0)
				dec1 := module.tokenDecimals(ctx, t1)
				usd0 := "0"
				usd1 := "0"
				if ok0 {
					usd0 = mulStr(divBigIntByPow10Str(amount0, dec0), p0)
				}
				if ok1 {
					usd1 = mulStr(divBigIntByPow10Str(amount1, dec1), p1)
				}
				amountUSD = addStr(usd0, usd1)
			}
		}
	}

	ct, err := module.db.Pool().Exec(ctx, `
	 UPDATE uniswap_v2_burns b
	 SET to_address = $3, amount0 = $4, amount1 = $5,
	     amount_usd = CASE WHEN $6 = '' THEN amount_usd ELSE $6::numeric END
	 WHERE b.ctid IN (
	 	SELECT ctid FROM uniswap_v2_burns
	 	WHERE transaction_hash = $1 AND pair = $2 AND amount0 = 0 AND amount1 = 0
	 	ORDER BY log_index ASC
	 	LIMIT 1
	 )`,
		event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()),
		strings.ToLower(to.Hex()), amount0.String(), amount1.String(), amountUSD,
	)
	if err != nil {
		module.logger.Error().Err(err).Str("pair", event.Address.Hex()).Msg("Burn update failed")
	}
	if ct.RowsAffected() == 0 {
		_, _ = module.db.Pool().Exec(ctx, `
	  INSERT INTO uniswap_v2_burns (
	  id, transaction_hash, log_index, block_number, timestamp,
	 pair, to_address, sender, amount0, amount1, liquidity, amount_usd
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,$11)
	 ON CONFLICT (id) DO NOTHING`,
			fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex),
			event.TransactionHash.Hex(), event.LogIndex, event.BlockNumber, ts,
			strings.ToLower(event.Address.Hex()), strings.ToLower(to.Hex()), strings.ToLower(sender.Hex()),
			amount0.String(), amount1.String(),
			func() string {
				if amountUSD == "" {
					return "0"
				}
				return amountUSD
			}(),
		)
	}

	module.logger.Debug().Str("pair", event.Address.Hex()).Str("to", to.Hex()).Msg("Burn processed")
	return nil
}

// handleTransfer processes Transfer events (for LP token tracking)
func handleTransfer(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {
	// For Transfer events:
	// topics[0] = event signature
	// topics[1] = indexed from address
	// topics[2] = indexed to address
	// data contains: value (uint256)

	// Extract indexed parameters from topics
	var from, to common.Address
	if len(event.Log.Topics) >= 3 {
		from = common.BytesToAddress(event.Log.Topics[1].Bytes())
		to = common.BytesToAddress(event.Log.Topics[2].Bytes())
	} else {
		return nil // Skip if not enough topics
	}

	// Parse value from data field
	if len(event.Log.Data) < 32 {
		return nil // Skip if not enough data
	}

	value := new(big.Int).SetBytes(event.Log.Data[0:32])

	// Only process transfers emitted by known Uniswap V2 pair contracts
	var isPair bool
	if err := module.db.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM uniswap_v2_pairs WHERE address = $1)`, strings.ToLower(event.Address.Hex())).Scan(&isPair); err != nil {
		return nil
	}
	if !isPair {
		return nil
	}

	// Fetch block timestamp
	ts := module.getBlockTimestamp(ctx, event.BlockNumber)

	zero := common.Address{}

	// Mint start: LP tokens minted (from == zero)
	if from == zero {
		// Ignore the initial minimum liquidity lock (to == zero, value == 1000)
		if to == zero && value.Cmp(big.NewInt(1000)) == 0 {
			// Still reflect total_supply increase
			_, _ = module.db.Pool().Exec(ctx, `
				UPDATE uniswap_v2_pairs
				SET total_supply = COALESCE(total_supply,0) + $2::numeric,
				    updated_at = NOW()
				WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())
			module.logger.Debug().Str("pair", event.Address.Hex()).Str("value", value.String()).Msg("Ignored min-liquidity lock mint row")
			return nil
		}
		// Duplicate guard: same tx/pair/to/liquidity/block
		var dup int
		_ = module.db.Pool().QueryRow(ctx, `
			SELECT COUNT(*) FROM uniswap_v2_mints
			WHERE transaction_hash=$1 AND pair=$2 AND to_address=$3 AND liquidity=$4 AND block_number=$5`,
			event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()), strings.ToLower(to.Hex()), value.String(), event.BlockNumber,
		).Scan(&dup)
		if dup == 0 {
			_, _ = module.db.Pool().Exec(ctx, `
				INSERT INTO uniswap_v2_mints (
					id, transaction_hash, log_index, block_number, timestamp,
					pair, to_address, sender, amount0, amount1, liquidity
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
				ON CONFLICT (id) DO NOTHING`,
				fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex),
				event.TransactionHash.Hex(),
				event.LogIndex,
				event.BlockNumber,
				ts,
				strings.ToLower(event.Address.Hex()),
				strings.ToLower(to.Hex()),
				strings.ToLower(zero.Hex()),
				"0",
				"0",
				value.String(),
			)
			// Increase pair total_supply
			_, _ = module.db.Pool().Exec(ctx, `
				UPDATE uniswap_v2_pairs
				SET total_supply = COALESCE(total_supply,0) + $2::numeric,
				    updated_at = NOW()
				WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())
		}
		module.logger.Debug().Str("pair", event.Address.Hex()).Str("to", to.Hex()).Str("value", value.String()).Msg("LP mint transfer recorded")
		return nil
	}

	// Burn start: LP tokens sent to pair (to == pair)
	if strings.EqualFold(to.Hex(), event.Address.Hex()) {
		var dup int
		_ = module.db.Pool().QueryRow(ctx, `
			SELECT COUNT(*) FROM uniswap_v2_burns
			WHERE transaction_hash=$1 AND pair=$2 AND sender=$3 AND liquidity=$4 AND block_number=$5`,
			event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()), strings.ToLower(from.Hex()), value.String(), event.BlockNumber,
		).Scan(&dup)
		if dup == 0 {
			_, _ = module.db.Pool().Exec(ctx, `
				INSERT INTO uniswap_v2_burns (
					id, transaction_hash, log_index, block_number, timestamp,
					pair, to_address, sender, amount0, amount1, liquidity
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
				ON CONFLICT (id) DO NOTHING`,
				fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex),
				event.TransactionHash.Hex(),
				event.LogIndex,
				event.BlockNumber,
				ts,
				strings.ToLower(event.Address.Hex()),
				strings.ToLower(zero.Hex()),
				strings.ToLower(from.Hex()),
				"0",
				"0",
				value.String(),
			)
		}
		module.logger.Debug().Str("pair", event.Address.Hex()).Str("from", from.Hex()).Str("value", value.String()).Msg("LP burn transfer (start) recorded")
		return nil
	}

	// Burn finalization: LP tokens sent from pair to zero (destroy)
	if strings.EqualFold(from.Hex(), event.Address.Hex()) && to == zero {
		// Update the earliest pending burn in this tx/pair (amount0/1 == 0)
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v2_burns b
			SET to_address = $4,
			    liquidity  = $3
			WHERE b.ctid IN (
				SELECT ctid FROM uniswap_v2_burns
				WHERE transaction_hash = $1 AND pair = $2 AND amount0 = 0 AND amount1 = 0
				ORDER BY log_index ASC
				LIMIT 1
			)`,
			event.TransactionHash.Hex(), strings.ToLower(event.Address.Hex()), value.String(), strings.ToLower(zero.Hex()),
		)
		// Decrease pair total_supply
		_, _ = module.db.Pool().Exec(ctx, `
			UPDATE uniswap_v2_pairs
			SET total_supply = GREATEST(COALESCE(total_supply,0) - $2::numeric, 0),
			    updated_at = NOW()
			WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())
		module.logger.Debug().Str("pair", event.Address.Hex()).Str("value", value.String()).Msg("LP burn transfer (final) applied")
		return nil
	}

	return nil
}

// Helper functions

// ensureToken makes sure a token exists in the tokens table
func (m *UniswapV2Module) ensureToken(ctx context.Context, tokenAddress common.Address, firstSeenBlock uint64, firstSeenTimestamp int64) error {
	// Check if token already exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM tokens WHERE address = $1)`
	err := m.db.Pool().QueryRow(ctx, query, strings.ToLower(tokenAddress.Hex())).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check token existence: %w", err)
	}

	if exists {
		return nil // Token already exists
	}

	// Fetch token metadata from contract
	tokenInfo, err := m.fetchTokenMetadata(ctx, tokenAddress)
	if err != nil {
		m.logger.Warn().
			Err(err).
			Str("token", tokenAddress.Hex()).
			Msg("Failed to fetch token metadata, using defaults")
		// Use defaults if fetching fails
		tokenInfo = &TokenMetadata{
			Name:     "Unknown",
			Symbol:   "???",
			Decimals: 18,
		}
	}

	// Check if this is a stablecoin
	tokenAddrLower := strings.ToLower(tokenAddress.Hex())
	isStablecoin := false

	for _, stable := range m.config.Stablecoins {
		if strings.ToLower(stable.Address) == tokenAddrLower {
			isStablecoin = true
			break
		}
	}

	// Insert token with metadata (price set later via ZIL-anchored routing)
	insertQuery := `
		INSERT INTO tokens (address, name, symbol, decimals, total_supply, first_seen_block, first_seen_timestamp, price_usd)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (address) DO UPDATE SET
			name = COALESCE(tokens.name, EXCLUDED.name),
			symbol = COALESCE(tokens.symbol, EXCLUDED.symbol),
			decimals = COALESCE(tokens.decimals, EXCLUDED.decimals),
			total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply),
			price_usd = COALESCE(tokens.price_usd, EXCLUDED.price_usd)` // Only set price if not already set

	var priceToSet interface{} = nil

	_, err = m.db.Pool().Exec(ctx, insertQuery,
		tokenAddrLower,
		tokenInfo.Name,
		tokenInfo.Symbol,
		tokenInfo.Decimals,
		tokenInfo.TotalSupply.String(),
		firstSeenBlock,
		firstSeenTimestamp,
		priceToSet, // Leave NULL; price is updated via ZIL-anchored routing
	)

	if err != nil {
		return fmt.Errorf("failed to insert token: %w", err)
	}

	if isStablecoin {
		m.logger.Info().
			Str("token", tokenAddrLower).
			Str("symbol", tokenInfo.Symbol).
			Msg("Created stablecoin token (price set via ZIL-anchored routing)")
	}

	m.logger.Info().
		Str("token", tokenAddress.Hex()).
		Str("name", tokenInfo.Name).
		Str("symbol", tokenInfo.Symbol).
		Int("decimals", tokenInfo.Decimals).
		Msg("Token created with metadata")
	return nil
}

// fetchTokenMetadata fetches token metadata from the blockchain
func (m *UniswapV2Module) fetchTokenMetadata(ctx context.Context, tokenAddress common.Address) (*TokenMetadata, error) {
	metadata := &TokenMetadata{
		TotalSupply: big.NewInt(0),
		Name:        "Unknown",
		Symbol:      "???",
		Decimals:    18,
	}

	// If we don't have an RPC client, return defaults
	if m.rpcClient == nil {
		m.logger.Debug().
			Str("token", tokenAddress.Hex()).
			Msg("No RPC client available for token metadata")
		return metadata, nil
	}

	// Create ERC20 ABI for token metadata calls
	const erc20ABIString = `[
		{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"type":"function"}
	]`

	erc20ABI, err := abi.JSON(strings.NewReader(erc20ABIString))
	if err != nil {
		return metadata, fmt.Errorf("failed to parse ERC20 ABI: %w", err)
	}

	// Create a bound contract instance
	contract := bind.NewBoundContract(tokenAddress, erc20ABI, m.rpcClient, m.rpcClient, m.rpcClient)

	// Try to fetch name
	var results []interface{}
	results = make([]interface{}, 1)
	results[0] = new(string)
	if err := contract.Call(nil, &results, "name"); err != nil {
		m.logger.Debug().Err(err).Str("token", tokenAddress.Hex()).Msg("Failed to fetch token name")
	} else if name, ok := results[0].(*string); ok && name != nil && *name != "" {
		metadata.Name = *name
	}

	// Try to fetch symbol
	results = make([]interface{}, 1)
	results[0] = new(string)
	if err := contract.Call(nil, &results, "symbol"); err != nil {
		m.logger.Debug().Err(err).Str("token", tokenAddress.Hex()).Msg("Failed to fetch token symbol")
	} else if symbol, ok := results[0].(*string); ok && symbol != nil && *symbol != "" {
		metadata.Symbol = *symbol
	}

	// Try to fetch decimals
	results = make([]interface{}, 1)
	results[0] = new(uint8)
	if err := contract.Call(nil, &results, "decimals"); err != nil {
		m.logger.Debug().Err(err).Str("token", tokenAddress.Hex()).Msg("Failed to fetch token decimals")
	} else if decimals, ok := results[0].(*uint8); ok && decimals != nil {
		metadata.Decimals = int(*decimals)
	}

	// Try to fetch total supply
	results = make([]interface{}, 1)
	results[0] = new(*big.Int)
	if err := contract.Call(nil, &results, "totalSupply"); err != nil {
		m.logger.Debug().Err(err).Str("token", tokenAddress.Hex()).Msg("Failed to fetch token totalSupply")
	} else if totalSupply, ok := results[0].(**big.Int); ok && totalSupply != nil && *totalSupply != nil {
		metadata.TotalSupply = *totalSupply
	}

	m.logger.Debug().
		Str("token", tokenAddress.Hex()).
		Str("name", metadata.Name).
		Str("symbol", metadata.Symbol).
		Int("decimals", metadata.Decimals).
		Msg("Fetched token metadata via RPC")

	return metadata, nil
}

// updateFactoryPairCount updates the factory's pair count
func (m *UniswapV2Module) updateFactoryPairCount(ctx context.Context, pairIndex uint64) error {
	// Upsert factory record
	query := `
		INSERT INTO uniswap_v2_factory (address, pair_count)
		VALUES ($1, $2)
		ON CONFLICT (address) DO UPDATE SET
			pair_count = EXCLUDED.pair_count,
			updated_at = CURRENT_TIMESTAMP`

	_, err := m.db.Pool().Exec(ctx, query,
		strings.ToLower(m.factoryAddress.Hex()),
		pairIndex,
	)

	return err
}

// recomputeTokenLiquidityUSD recalculates tokens.total_liquidity_usd as the sum of the token's
// reserve USD across all Uniswap V2 pairs and Uniswap V3 pools.
func recomputeTokenLiquidityUSD(ctx context.Context, module *UniswapV2Module, addr string) {
	// V2 side: sum over token side reserves valued at token.price_usd
	// V3 side: same using uniswap_v3_pools reserves
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
func recomputeTokenMarketCapUSD(ctx context.Context, module *UniswapV2Module, addr string) {
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
