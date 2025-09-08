package uniswapv2

import (
	"context"
	"fmt"
	"math/big"
	"strings"

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
	if err := module.ensureToken(ctx, token0); err != nil {
		return fmt.Errorf("failed to ensure token0: %w", err)
	}

	if err := module.ensureToken(ctx, token1); err != nil {
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
		strings.ToLower(event.Address.Hex()), // pair address
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

	// TODO: Update pair volume and USD amounts
	// This would require price calculation logic

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("sender", sender.Hex()).
		Str("swap_id", swapID).
		Msg("Swap processed")

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
		strings.ToLower(event.Address.Hex()), // pair address
		reserve0.String(),
		reserve1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert sync: %w", err)
	}

	// Update pair reserves
	updatePairQuery := `
		UPDATE uniswap_v2_pairs 
		SET reserve0 = $2, reserve1 = $3, updated_at = CURRENT_TIMESTAMP
		WHERE address = $1`

	_, err = module.db.Pool().Exec(ctx, updatePairQuery,
		strings.ToLower(event.Address.Hex()),
		reserve0.String(),
		reserve1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to update pair reserves: %w", err)
	}

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("reserve0", reserve0.String()).
		Str("reserve1", reserve1.String()).
		Msg("Sync processed")

	return nil
}

// handleMint processes Mint events (liquidity additions)
func handleMint(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error {
	// For Mint events:
	// topics[0] = event signature
	// topics[1] = indexed sender address
	// data contains: amount0, amount1
	
	// Extract indexed sender from topics
	var sender common.Address
	if len(event.Log.Topics) >= 2 {
		sender = common.BytesToAddress(event.Log.Topics[1].Bytes())
	} else {
		return fmt.Errorf("insufficient topics for Mint event")
	}

	// Parse amounts from data field
	if len(event.Log.Data) < 64 { // Need at least 2 * 32 bytes
		return fmt.Errorf("insufficient data for Mint event")
	}

	amount0 := new(big.Int).SetBytes(event.Log.Data[0:32])
	amount1 := new(big.Int).SetBytes(event.Log.Data[32:64])

	// Create mint ID
	mintID := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)

	// Check for duplicate mint event with same transaction hash
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v2_mints
		WHERE transaction_hash = $1
		  AND pair = $2
		  AND sender = $3
		  AND amount0 = $4
		  AND amount1 = $5
		  AND block_number = $6
		  AND id != $7`
	
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()),
		amount0.String(),
		amount1.String(),
		event.BlockNumber,
		mintID,
	).Scan(&duplicateCount)
	
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().
			Str("mint_id", mintID).
			Int("duplicates", duplicateCount).
			Msg("Skipping duplicate mint event")
		return nil
	}

	// Insert mint record
	query := `
		INSERT INTO uniswap_v2_mints (
			id, transaction_hash, log_index, block_number, timestamp,
			pair, to_address, sender, amount0, amount1
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING`

	// For mints, the 'to' address is usually found from a Transfer event
	// For now, we'll use the sender as the to_address
	_, err = module.db.Pool().Exec(ctx, query,
		mintID,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()), // to_address
		strings.ToLower(sender.Hex()),
		amount0.String(),
		amount1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert mint: %w", err)
	}

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("sender", sender.Hex()).
		Str("mint_id", mintID).
		Msg("Mint processed")

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

	// Create burn ID
	burnID := fmt.Sprintf("%s-%d", event.TransactionHash.Hex(), event.LogIndex)

	// Check for duplicate burn event with same transaction hash
	dupCheckQuery := `
		SELECT COUNT(*) FROM uniswap_v2_burns
		WHERE transaction_hash = $1
		  AND pair = $2
		  AND sender = $3
		  AND to_address = $4
		  AND amount0 = $5
		  AND amount1 = $6
		  AND block_number = $7
		  AND id != $8`
	
	var duplicateCount int
	err := module.db.Pool().QueryRow(ctx, dupCheckQuery,
		event.TransactionHash.Hex(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(sender.Hex()),
		strings.ToLower(to.Hex()),
		amount0.String(),
		amount1.String(),
		event.BlockNumber,
		burnID,
	).Scan(&duplicateCount)
	
	if err == nil && duplicateCount > 0 {
		module.logger.Debug().
			Str("burn_id", burnID).
			Int("duplicates", duplicateCount).
			Msg("Skipping duplicate burn event")
		return nil
	}

	// Insert burn record
	query := `
		INSERT INTO uniswap_v2_burns (
			id, transaction_hash, log_index, block_number, timestamp,
			pair, to_address, sender, amount0, amount1
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING`

	_, err = module.db.Pool().Exec(ctx, query,
		burnID,
		event.TransactionHash.Hex(),
		event.LogIndex,
		event.BlockNumber,
		event.Timestamp.Int64(),
		strings.ToLower(event.Address.Hex()),
		strings.ToLower(to.Hex()),
		strings.ToLower(sender.Hex()),
		amount0.String(),
		amount1.String(),
	)

	if err != nil {
		return fmt.Errorf("failed to insert burn: %w", err)
	}

	module.logger.Debug().
		Str("pair", event.Address.Hex()).
		Str("to", to.Hex()).
		Str("burn_id", burnID).
		Msg("Burn processed")

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

	// Check if this is a mint (from zero address) or burn (to zero address)
	zero := common.Address{}
	if from == zero || to == zero {
		// This is LP token mint/burn, we handle this differently
		// Could update total supply here
		module.logger.Debug().
			Str("pair", event.Address.Hex()).
			Str("from", from.Hex()).
			Str("to", to.Hex()).
			Str("value", value.String()).
			Msg("LP token transfer processed")
	}

	return nil
}

// Helper functions

// ensureToken makes sure a token exists in the tokens table
func (m *UniswapV2Module) ensureToken(ctx context.Context, tokenAddress common.Address) error {
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

	// Insert token with metadata
	insertQuery := `
		INSERT INTO tokens (address, name, symbol, decimals, total_supply, first_seen_block, first_seen_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (address) DO UPDATE SET
			name = COALESCE(tokens.name, EXCLUDED.name),
			symbol = COALESCE(tokens.symbol, EXCLUDED.symbol),
			decimals = COALESCE(tokens.decimals, EXCLUDED.decimals),
			total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply)`

	_, err = m.db.Pool().Exec(ctx, insertQuery,
		strings.ToLower(tokenAddress.Hex()),
		tokenInfo.Name,
		tokenInfo.Symbol,
		tokenInfo.Decimals,
		tokenInfo.TotalSupply.String(),
		0, // Will be updated with real block number
		0, // Will be updated with real timestamp
	)

	if err != nil {
		return fmt.Errorf("failed to insert token: %w", err)
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