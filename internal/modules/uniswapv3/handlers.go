package uniswapv3

import (
	"context"
	"fmt"
	"math/big"
	"strings"

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
	if err := module.ensureToken(ctx, token0); err != nil { return fmt.Errorf("failed to ensure token0: %w", err) }
	if err := module.ensureToken(ctx, token1); err != nil { return fmt.Errorf("failed to ensure token1: %w", err) }

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
	return err
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
	return err
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
	return err
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
	return err
}

// ensureToken ensures a token exists in the universal tokens table
func (m *UniswapV3Module) ensureToken(ctx context.Context, tokenAddress common.Address) error {
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
		0, 0,
	)
	return err
}
